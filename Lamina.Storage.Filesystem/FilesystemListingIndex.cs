using System.Diagnostics;
using System.Diagnostics.Metrics;
using Lamina.Storage.Core.Listing;
using Lamina.Storage.Filesystem.Configuration;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

/// <summary>
/// Bounded, disposable acceleration of name selection, never a source of object metadata.
/// All retained state (including in-flight scans and mutation journals) shares one budget.
/// </summary>
public sealed class FilesystemListingIndex : IAsyncDisposable, IDisposable
{
    private static readonly Meter Meter = new("Lamina.Storage.Listing");
    private static readonly Counter<long> Hits = Meter.CreateCounter<long>("listing.index.hits");
    private static readonly Counter<long> Builds = Meter.CreateCounter<long>("listing.index.builds");
    private static readonly Counter<long> Fallbacks = Meter.CreateCounter<long>("listing.index.fallbacks");
    private static readonly UpDownCounter<long> RetainedBytes = Meter.CreateUpDownCounter<long>("listing.index.estimated_bytes", "By");
    private static readonly Histogram<double> ScanDuration = Meter.CreateHistogram<double>("listing.scan.duration", "ms");
    private static readonly Counter<long> ScannedEntries = Meter.CreateCounter<long>("listing.scan.entries");

    internal static void RecordScan(ListingStatistics statistics, long started)
    {
        ScannedEntries.Add(statistics.ScannedEntries);
        ScanDuration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
    }

    private readonly FilesystemListingIndexSettings _settings;
    private readonly TimeProvider _clock;
    private readonly SemaphoreSlim _builders;
    private readonly object _sync = new();
    private readonly Dictionary<QueryKey, State> _states = new();
    private long _bytes;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly HashSet<Task<BuildResult>> _pending = new();
    private readonly record struct QueryKey(string Bucket, string Prefix, string? Delimiter, ListingOrder Order);

    private sealed class State
    {
        public required QueryKey Key;
        public required Func<ListingEntry, bool> Exists;
        public required long Started;
        public long Used;
        public long Bytes;
        public bool Abandoned;
        public long Revision;
        public Dictionary<string, ListingEntry>? Building = new(StringComparer.Ordinal);
        public HashSet<string> Changes = new(StringComparer.Ordinal);
        public List<ListingEntry>? Entries;
        public Task<BuildResult>? Task;
    }

    private sealed record BuildResult(ListingCandidates Fallback);

    public FilesystemListingIndex(IOptions<FilesystemListingIndexSettings> settings, TimeProvider? clock = null)
    {
        _settings = settings.Value;
        if (_settings.AbsoluteExpirationSeconds <= 0 || _settings.SlidingExpirationSeconds <= 0
            || _settings.SizeLimit <= 0 || _settings.MaxConcurrentBuilds <= 0)
            throw new ArgumentException("Listing index expiration, size and concurrency limits must be positive.", nameof(settings));
        _clock = clock ?? TimeProvider.System;
        _builders = new SemaphoreSlim(_settings.MaxConcurrentBuilds);
    }

    public bool Enabled => _settings.Enabled;

    public async Task<ListingCandidates> GetAsync(string bucket, ListingQuery query,
        Func<ListingEntryObserver?, CancellationToken, (ListingCandidates Page, bool HasSymlinks)> scan,
        Func<ListingEntry, bool> exists, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _shutdown.Token.ThrowIfCancellationRequested();
        if (!Enabled || query.CandidateLimit == 0)
            return scan(null, cancellationToken).Page;
        var key = new QueryKey(bucket, query.Prefix, query.Delimiter, query.Order);
        State state;
        bool owner = false;
        lock (_sync)
        {
            if (_states.TryGetValue(key, out var cached))
            {
                if (Expired(cached)) Remove(cached);
                else if (cached.Entries != null)
                {
                    Hits.Add(1);
                    cached.Used = _clock.GetTimestamp();
                    return Page(cached.Entries, query, new ListingStatistics());
                }
            }
            if (!_states.TryGetValue(key, out state!))
            {
                if (_pending.Count >= _settings.MaxConcurrentBuilds)
                {
                    state = null!; // Saturated: use bounded scanner, do not queue another cache build.
                }
                else
                {
                    state = new State { Key = key, Exists = exists, Started = _clock.GetTimestamp(), Used = _clock.GetTimestamp() };
                    _states.Add(key, state);
                    if (!Reserve(state, 512 + bucket.Length * 2L + query.Prefix.Length * 2L + (query.Delimiter?.Length ?? 0) * 2L))
                        Remove(state);
                    owner = true;
                    state.Task = Task.Run(() => BuildAsync(state, scan));
                    _pending.Add(state.Task);
                    _ = state.Task.ContinueWith(task =>
                    {
                        lock (_sync) _pending.Remove(task);
                        _ = task.Exception; // Observe faults even if every waiter disconnected.
                    }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                }
            }
        }
        if (state == null)
        {
            Fallbacks.Add(1);
            return scan(null, cancellationToken).Page;
        }
        var result = await state.Task!.WaitAsync(cancellationToken);
        lock (_sync)
        {
            if (state.Entries != null && !state.Abandoned && !Expired(state))
            {
                state.Used = _clock.GetTimestamp();
                return Page(state.Entries, query, owner ? result.Fallback.Statistics : new ListingStatistics());
            }
        }
        // The owner already selected its bounded fallback during the same scan.
        Fallbacks.Add(1);
        return owner ? result.Fallback : scan(null, cancellationToken).Page;
    }

    private async Task<BuildResult> BuildAsync(State state,
        Func<ListingEntryObserver?, CancellationToken, (ListingCandidates Page, bool HasSymlinks)> scan)
    {
        await _builders.WaitAsync(_shutdown.Token);
        try
        {
            Builds.Add(1);
            lock (_sync) state.Started = _clock.GetTimestamp();
            var result = scan((name, prefix) =>
            {
                lock (_sync)
                {
                    if (state.Abandoned || state.Building!.GetAlternateLookup<ReadOnlySpan<char>>().ContainsKey(name)) return;
                    // Includes dictionary/node overhead, string, final array and sorting workspace.
                    if (!Reserve(state, Estimate(name.Length))) { Remove(state); return; }
                    var value = name.ToString();
                    state.Building.Add(value, new ListingEntry(value, prefix));
                }
            }, _shutdown.Token);
            // Probe changed names outside the global cache lock. If writers continue
            // racing, discard this acceleration rather than publish a stale generation.
            for (var attempt = 0; attempt < 3; attempt++)
            {
                string[] changes;
                long revision;
                lock (_sync)
                {
                    if (result.HasSymlinks || state.Abandoned || Expired(state))
                    {
                        Remove(state);
                        return new BuildResult(result.Page);
                    }
                    changes = state.Changes.ToArray();
                    revision = state.Revision;
                }
                var observed = changes.Select(key => (Key: key, Exists: state.Exists(Project(state, key)))).ToArray();
                lock (_sync)
                {
                    if (state.Abandoned) return new BuildResult(result.Page);
                    if (state.Revision != revision) continue;
                    foreach (var change in observed)
                    {
                        Reconcile(state, change.Key, change.Exists);
                        if (state.Abandoned) return new BuildResult(result.Page);
                    }
                    state.Entries = state.Building!.Values.ToList();
                    state.Entries.Sort(Comparer(state.Key.Order));
                    state.Building = null;
                    state.Changes.Clear();
                    state.Used = _clock.GetTimestamp();
                    return new BuildResult(result.Page);
                }
            }
            lock (_sync) Remove(state);
            return new BuildResult(result.Page);
        }
        catch
        {
            lock (_sync) Remove(state);
            throw;
        }
        finally { _builders.Release(); }
    }

    /// <summary>Called after publication/removal, including directory cleanup.</summary>
    public void Changed(string bucket, string key)
    {
        var probes = new List<(State State, long Revision)>();
        lock (_sync)
        {
            foreach (var state in _states.Values.Where(x => x.Key.Bucket == bucket).ToArray())
            {
                if (state.Abandoned) continue;
                if (state.Key.Delimiter is not null and not "/") { Remove(state); continue; }
                if (!key.StartsWith(state.Key.Prefix, StringComparison.Ordinal)) continue;
                state.Revision++;
                if (state.Building != null)
                {
                    if (!state.Changes.Contains(key))
                    {
                        if (!Reserve(state, Estimate(key.Length))) { Remove(state); continue; }
                        state.Changes.Add(key);
                    }
                }
                else probes.Add((state, state.Revision));
            }
        }
        foreach (var (state, revision) in probes)
        {
            var exists = state.Exists(Project(state, key));
            lock (_sync)
            {
                if (state.Abandoned) continue;
                if (state.Revision != revision) Remove(state);
                else Reconcile(state, key, exists);
            }
        }
    }

    public void InvalidateAll()
    {
        lock (_sync)
            foreach (var state in _states.Values.ToArray()) Remove(state);
    }

    public void InvalidateBucket(string bucket)
    {
        lock (_sync)
            foreach (var state in _states.Values.Where(x => x.Key.Bucket == bucket).ToArray()) Remove(state);
    }

    private static ListingEntry Project(State state, string key)
    {
        var offset = state.Key.Delimiter == "/" ? key.AsSpan(state.Key.Prefix.Length).IndexOf('/') : -1;
        return offset < 0 ? new ListingEntry(key, false)
            : new ListingEntry(key[..(state.Key.Prefix.Length + offset + 1)], true);
    }

    private void Reconcile(State state, string key, bool exists)
    {
        var projected = Project(state, key);
        if (state.Building != null)
        {
            if (!exists) state.Building.Remove(projected.Name);
            else if (!state.Building.ContainsKey(projected.Name))
            {
                if (!Reserve(state, Estimate(projected.Name.Length))) { Remove(state); return; }
                state.Building.Add(projected.Name, projected);
            }
            return;
        }
        var entries = state.Entries!;
        var position = entries.BinarySearch(projected, Comparer(state.Key.Order));
        if (!exists && position >= 0) entries.RemoveAt(position);
        else if (exists && position < 0)
        {
            if (!Reserve(state, Estimate(projected.Name.Length))) { Remove(state); return; }
            entries.Insert(~position, projected);
        }
    }

    public void Dispose()
    {
        _shutdown.Cancel();
        lock (_sync)
            foreach (var state in _states.Values.ToArray()) Remove(state);
    }

    public async ValueTask DisposeAsync()
    {
        Dispose();
        Task[] pending;
        lock (_sync) pending = _pending.ToArray();
        try { await Task.WhenAll(pending); }
        catch (OperationCanceledException) { }
        // Faulted scans are already observed and delivered to their waiting callers.
        catch (Exception) { }
    }

    private bool Reserve(State state, long bytes)
    {
        while (_bytes > _settings.SizeLimit - bytes)
        {
            var victim = _states.Values.Where(x => x != state && x.Entries != null).MinBy(x => x.Used);
            if (victim == null) return false;
            Remove(victim);
        }
        _bytes += bytes;
        RetainedBytes.Add(bytes);
        state.Bytes += bytes;
        return true;
    }

    private void Remove(State state)
    {
        if (state.Abandoned) return;
        if (_states.TryGetValue(state.Key, out var current) && current == state) _states.Remove(state.Key);
        _bytes -= state.Bytes;
        RetainedBytes.Add(-state.Bytes);
        state.Bytes = 0;
        state.Abandoned = true;
        state.Building = null;
        state.Entries = null;
        state.Changes.Clear();
    }

    private bool Expired(State state) =>
        _clock.GetElapsedTime(state.Started).TotalSeconds >= _settings.AbsoluteExpirationSeconds
        || (state.Entries != null && _clock.GetElapsedTime(state.Used).TotalSeconds >= _settings.SlidingExpirationSeconds);

    private static long Estimate(int length) => 256 + length * 4L;

    private static IComparer<ListingEntry> Comparer(ListingOrder order) =>
        System.Collections.Generic.Comparer<ListingEntry>.Create((a, b) =>
        {
            var compared = order == ListingOrder.DirectoryHashV1
                ? ListingNameComparer.DirectoryHash(a.Name).CompareTo(ListingNameComparer.DirectoryHash(b.Name)) : 0;
            return compared != 0 ? compared : ListingNameComparer.Compare(a.Name, b.Name);
        });

    private static ListingCandidates Page(List<ListingEntry> entries, ListingQuery query, ListingStatistics statistics)
    {
        var start = 0;
        if (query.After != null)
        {
            var found = entries.BinarySearch(new ListingEntry(query.After.Name, false), Comparer(query.Order));
            start = found < 0 ? ~found : found + 1;
        }
        var count = Math.Min(query.CandidateLimit, entries.Count - start);
        statistics.PeakCandidates = count;
        return new ListingCandidates(entries.GetRange(start, count), statistics);
    }
}
