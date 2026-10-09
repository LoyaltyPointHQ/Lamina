using Lamina.Core.Models;
using Lamina.Storage.Core.Listing;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem.Tests;

public sealed class FilesystemListingIndexTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "lamina-index-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public void DefaultSettings_EnableIndex()
    {
        using var index = new FilesystemListingIndex(Options.Create(new FilesystemListingIndexSettings()));
        Assert.True(index.Enabled);
    }

    [Fact]
    public async Task ConsecutivePages_ScanOnlyOnce()
    {
        Directory.CreateDirectory(Path.Combine(_root, "bucket"));
        foreach (var key in new[] { "a", "b", "c" })
            File.WriteAllText(Path.Combine(_root, "bucket", key), key);
        var storage = CreateStorage();
        var first = await storage.ListDataCandidatesAsync("bucket", new ListingQuery(BucketType.GeneralPurpose, maxKeys: 1));
        var second = await storage.ListDataCandidatesAsync("bucket", new ListingQuery(BucketType.GeneralPurpose,
            after: new ListingPosition(ListingOrder.Lexicographical, "a"), maxKeys: 1));
        Assert.Equal(new[] { "b", "c" }, second.Entries.Select(x => x.Name));
        Assert.True(first.Statistics.ScannedEntries > 0);
        Assert.Equal(0, second.Statistics.ScannedEntries);
    }

    [Theory]
    [InlineData(BucketType.GeneralPurpose)]
    [InlineData(BucketType.Directory)]
    public async Task CachedPages_MatchScannerWithUnicodePrefixesAndEmptyDirectories(BucketType type)
    {
        Directory.CreateDirectory(Path.Combine(_root, "bucket", "empty"));
        foreach (var key in new[] { "a", "z", "é", "😀", "" })
            File.WriteAllText(Path.Combine(_root, "bucket", key), key);
        var indexed = CreateStorage();
        var scanner = CreateStorage(enabled: false);
        foreach (var delimiter in new string?[] { null, "/", "é" })
        {
            ListingPosition? cursor = null;
            do
            {
                var query = new ListingQuery(type, delimiter: delimiter, after: cursor, maxKeys: 2);
                var expected = await scanner.ListDataCandidatesAsync("bucket", query);
                var actual = await indexed.ListDataCandidatesAsync("bucket", query);
                Assert.Equal(expected.Entries, actual.Entries);
                cursor = actual.Entries.Count > 2 ? new ListingPosition(query.Order, actual.Entries[1].Name) : null;
            } while (cursor != null);
        }
    }

    [Fact]
    public async Task LocalPublicationAndDeletion_UpdateCachedNamesAndPrefixes()
    {
        Directory.CreateDirectory(Path.Combine(_root, "bucket"));
        var storage = CreateStorage();
        var query = new ListingQuery(BucketType.GeneralPurpose);
        var prefixes = new ListingQuery(BucketType.GeneralPurpose, delimiter: "/");
        await storage.ListDataCandidatesAsync("bucket", query);
        await storage.ListDataCandidatesAsync("bucket", prefixes);
        await using (var write = await storage.BeginWriteAsync("bucket", "nested/key"))
        {
            await write.Stream.WriteAsync(new byte[] { 1 });
            using var prepared = await write.SealAsync();
            await storage.CommitPreparedDataAsync(prepared);
        }
        var objects = await storage.ListDataCandidatesAsync("bucket", query);
        Assert.Equal("nested/key", Assert.Single(objects.Entries).Name);
        Assert.Equal(0, objects.Statistics.ScannedEntries);
        var dirs = await storage.ListDataCandidatesAsync("bucket", prefixes);
        Assert.Equal(new ListingEntry("nested/", true), Assert.Single(dirs.Entries));
        Assert.Equal(0, dirs.Statistics.ScannedEntries);
        await storage.DeleteDataAsync("bucket", "nested/key");
        Assert.Empty((await storage.ListDataCandidatesAsync("bucket", query)).Entries);
        Assert.Empty((await storage.ListDataCandidatesAsync("bucket", prefixes)).Entries);
    }

    [Fact]
    public async Task SlidingExpiration_IsExtendedByReadsButAbsoluteExpirationIsNot()
    {
        var time = new ManualClock();
        var index = NewIndex(time: time);
        var scans = 0;
        Task<ListingCandidates> Read() => index.GetAsync("bucket", new ListingQuery(BucketType.GeneralPurpose),
            (observer, _) => { scans++; observer?.Invoke("a", false); return (new ListingCandidates([], new()), false); },
            _ => true, CancellationToken.None);
        await Read();
        for (var i = 0; i < 59; i++) { time.Advance(5); await Read(); }
        Assert.Equal(1, scans);
        time.Advance(5);
        await Read();
        Assert.Equal(2, scans);
        time.Advance(10);
        await Read();
        Assert.Equal(3, scans);
    }

    [Fact]
    public async Task OverBudget_FallsBackToBoundedPageFromSameScan()
    {
        var index = NewIndex(sizeLimit: 600);
        var scans = 0;
        var query = new ListingQuery(BucketType.GeneralPurpose, maxKeys: 1);
        for (var i = 0; i < 2; i++)
        {
            var result = await index.GetAsync("bucket", query, (observer, _) =>
            {
                scans++;
                var selector = new ListingPageSelector(query, observer: observer);
                foreach (var key in new[] { "c", "a", "b" }) selector.ConsiderKey(key);
                return (selector.Finish(), false);
            }, _ => true, CancellationToken.None);
            Assert.Equal(new[] { "a", "b" }, result.Entries.Select(x => x.Name));
        }
        Assert.Equal(2, scans);
    }

    [Fact]
    public async Task MutationDuringBuild_IsReconciledBeforePublishing()
    {
        var index = NewIndex();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var read = index.GetAsync("bucket", new ListingQuery(BucketType.GeneralPurpose), (observer, _) =>
        {
            observer?.Invoke("a", false);
            entered.SetResult();
            release.Task.GetAwaiter().GetResult();
            return (new ListingCandidates([], new()), false);
        }, entry => entry.Name == "b", CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        index.Changed("bucket", "a");
        index.Changed("bucket", "b");
        release.SetResult();
        Assert.Equal("b", Assert.Single((await read).Entries).Name);
    }

    [Fact]
    public async Task ConcurrentQueries_ShareBuildAndCancelledWaiterDoesNotCancelOthers()
    {
        var index = NewIndex();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var scans = 0;
        (ListingCandidates, bool) Scan(ListingEntryObserver? observer, CancellationToken token)
        {
            Interlocked.Increment(ref scans);
            observer?.Invoke("a", false);
            observer?.Invoke("b", false);
            entered.SetResult();
            release.Task.GetAwaiter().GetResult();
            return (new ListingCandidates([], new()), false);
        }
        using var cancellation = new CancellationTokenSource();
        var first = index.GetAsync("bucket", new ListingQuery(BucketType.GeneralPurpose), Scan, _ => true, cancellation.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var second = index.GetAsync("bucket", new ListingQuery(BucketType.GeneralPurpose,
            after: new ListingPosition(ListingOrder.Lexicographical, "a")), Scan, _ => true, CancellationToken.None);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        release.SetResult();
        Assert.Equal("b", Assert.Single((await second).Entries).Name);
        Assert.Equal(1, scans);
    }

    [Fact]
    public async Task BatchDataInfo_ReturnsFreshStatsAndMissingKeys()
    {
        Directory.CreateDirectory(Path.Combine(_root, "bucket"));
        File.WriteAllText(Path.Combine(_root, "bucket", "a"), "abc");
        var batch = Assert.IsAssignableFrom<Lamina.Storage.Core.Abstract.IBatchObjectDataInfoStorage>(CreateStorage());
        var result = await batch.GetDataInfoBatchAsync("bucket", ["a", "missing"]);
        Assert.Equal(3, result["a"]!.Value.size);
        Assert.Null(result["missing"]);
        File.WriteAllText(Path.Combine(_root, "bucket", "a"), "longer");
        Assert.Equal(6, (await batch.GetDataInfoBatchAsync("bucket", ["a"]))["a"]!.Value.size);
    }

    [Fact]
    public async Task PrefixBelowDirectorySymlink_DoesNotCacheAliasedNamespace()
    {
        if (OperatingSystem.IsWindows()) return;
        Directory.CreateDirectory(Path.Combine(_root, "bucket", "real", "nested"));
        File.WriteAllText(Path.Combine(_root, "bucket", "real", "nested", "a"), "a");
        Directory.CreateSymbolicLink(Path.Combine(_root, "bucket", "alias"), Path.Combine(_root, "bucket", "real"));
        var storage = CreateStorage();
        var query = new ListingQuery(BucketType.GeneralPurpose, prefix: "alias/nested/");
        await storage.ListDataCandidatesAsync("bucket", query);
        File.WriteAllText(Path.Combine(_root, "bucket", "real", "nested", "b"), "b");
        var page = await storage.ListDataCandidatesAsync("bucket", query);
        Assert.Equal(2, page.Entries.Count);
        Assert.True(page.Statistics.ScannedEntries > 0);
    }

    [Fact]
    public async Task Shutdown_CancelsManagedBuildAndAwaitsExit()
    {
        var index = NewIndex();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var read = index.GetAsync("bucket", new ListingQuery(BucketType.GeneralPurpose), (_, token) =>
        {
            entered.SetResult();
            token.WaitHandle.WaitOne();
            token.ThrowIfCancellationRequested();
            return (new ListingCandidates([], new()), false);
        }, _ => true, CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await index.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read);
    }

    [Fact]
    public async Task SaturatedBuilders_UseScannerWithoutQueuingAnotherIndexBuild()
    {
        await using var index = NewIndex();
        var entered = new SemaphoreSlim(0);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        (ListingCandidates, bool) Block(ListingEntryObserver? observer, CancellationToken token)
        {
            Assert.NotNull(observer);
            entered.Release();
            release.Task.GetAwaiter().GetResult();
            return (new ListingCandidates([], new()), false);
        }
        var first = index.GetAsync("a", new ListingQuery(BucketType.GeneralPurpose), Block, _ => true, CancellationToken.None);
        var second = index.GetAsync("b", new ListingQuery(BucketType.GeneralPurpose), Block, _ => true, CancellationToken.None);
        await entered.WaitAsync(TimeSpan.FromSeconds(5));
        await entered.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            await index.GetAsync("c", new ListingQuery(BucketType.GeneralPurpose), (observer, _) =>
            {
                Assert.Null(observer);
                return (new ListingCandidates([], new()), false);
            }, _ => true, CancellationToken.None);
        }
        finally { release.SetResult(); }
        await Task.WhenAll(first, second);
    }

    [Fact]
    public async Task SlowMutationProbe_DoesNotBlockUnrelatedCachedPage()
    {
        await using var index = NewIndex();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        (ListingCandidates, bool) Scan(ListingEntryObserver? observer, CancellationToken token)
        {
            observer?.Invoke("key", false);
            return (new ListingCandidates([], new()), false);
        }
        await index.GetAsync("slow", new ListingQuery(BucketType.GeneralPurpose), Scan, _ =>
        {
            entered.SetResult();
            release.Task.GetAwaiter().GetResult();
            return true;
        }, CancellationToken.None);
        await index.GetAsync("fast", new ListingQuery(BucketType.GeneralPurpose), Scan, _ => true, CancellationToken.None);
        var change = Task.Run(() => index.Changed("slow", "key"));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            var read = Task.Run(() => index.GetAsync("fast", new ListingQuery(BucketType.GeneralPurpose), Scan, _ => true, CancellationToken.None));
            Assert.Single((await read.WaitAsync(TimeSpan.FromSeconds(2))).Entries);
        }
        finally { release.SetResult(); }
        await change;
    }

    [Fact]
    public async Task BucketInvalidationAndUnusualDelimiterMutation_ForceRebuild()
    {
        await using var index = NewIndex();
        var scans = 0;
        Task<ListingCandidates> Read() => index.GetAsync("bucket", new ListingQuery(BucketType.GeneralPurpose, delimiter: "-"),
            (observer, _) => { scans++; observer?.Invoke("a-", true); return (new ListingCandidates([], new()), false); },
            _ => true, CancellationToken.None);
        await Read();
        await Read();
        Assert.Equal(1, scans);
        index.Changed("bucket", "a-key");
        await Read();
        Assert.Equal(2, scans);
        index.InvalidateBucket("bucket");
        await Read();
        Assert.Equal(3, scans);
    }

    [Fact]
    public async Task MemoryPressure_EvictsLeastRecentlyUsedIndex()
    {
        var time = new ManualClock();
        await using var index = NewIndex(sizeLimit: 1700, time: time);
        var scans = new Dictionary<string, int>();
        Task<ListingCandidates> Read(string bucket) => index.GetAsync(bucket, new ListingQuery(BucketType.GeneralPurpose),
            (observer, _) =>
            {
                scans[bucket] = scans.GetValueOrDefault(bucket) + 1;
                observer?.Invoke("key", false);
                return (new ListingCandidates([], new()), false);
            }, _ => true, CancellationToken.None);
        await Read("a");
        time.Advance(1);
        await Read("b");
        time.Advance(1);
        await Read("a");
        await Read("c");
        await Read("a");
        Assert.Equal(1, scans["a"]);
        await Read("b");
        Assert.Equal(2, scans["b"]);
    }

    [Theory]
    [InlineData(BucketType.GeneralPurpose)]
    [InlineData(BucketType.Directory)]
    public async Task HundredThousandNames_HundredPages_EnumerateOnce(BucketType type)
    {
        await using var index = NewIndex();
        var scans = 0;
        var names = new HashSet<string>(StringComparer.Ordinal);
        ListingPosition? cursor = null;
        var pages = 0;
        do
        {
            var query = new ListingQuery(type, prefix: "wal_005/", after: cursor, maxKeys: 1000);
            var result = await index.GetAsync("bucket", query, (observer, token) =>
            {
                scans++;
                var selector = new ListingPageSelector(query, observer: observer);
                for (var n = 0; n < 100_000; n++) selector.ConsiderKey($"wal_005/{n:D8}");
                return (selector.Finish(), false);
            }, _ => true, CancellationToken.None);
            foreach (var entry in result.Entries.Take(1000)) Assert.True(names.Add(entry.Name));
            cursor = result.Entries.Count > 1000 ? new ListingPosition(query.Order, result.Entries[999].Name) : null;
            pages++;
        } while (cursor != null);
        Assert.Equal(100, pages);
        Assert.Equal(100_000, names.Count);
        Assert.Equal(1, scans);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MutationThroughDirectoryAlias_InvalidatesCachedTargetPrefix(bool anotherBucket)
    {
        if (OperatingSystem.IsWindows()) return;
        var targetBucket = anotherBucket ? "target" : "bucket";
        Directory.CreateDirectory(Path.Combine(_root, "bucket"));
        Directory.CreateDirectory(Path.Combine(_root, targetBucket, "real"));
        File.WriteAllText(Path.Combine(_root, targetBucket, "real", "existing"), "a");
        Directory.CreateSymbolicLink(Path.Combine(_root, "bucket", "alias"), Path.Combine(_root, targetBucket, "real"));
        var storage = CreateStorage();
        var query = new ListingQuery(BucketType.GeneralPurpose, prefix: "real/");
        await storage.ListDataCandidatesAsync(targetBucket, query);
        await using (var write = await storage.BeginWriteAsync("bucket", "alias/new"))
        {
            await write.Stream.WriteAsync(new byte[] { 1 });
            using var prepared = await write.SealAsync();
            await storage.CommitPreparedDataAsync(prepared);
        }
        var afterWrite = await storage.ListDataCandidatesAsync(targetBucket, query);
        Assert.Equal(new[] { "real/existing", "real/new" }, afterWrite.Entries.Select(x => x.Name));
        Assert.True(afterWrite.Statistics.ScannedEntries > 0);
        await storage.DeleteDataAsync("bucket", "alias/new");
        var afterDelete = await storage.ListDataCandidatesAsync(targetBucket, query);
        Assert.Equal("real/existing", Assert.Single(afterDelete.Entries).Name);
        Assert.True(afterDelete.Statistics.ScannedEntries > 0);
    }

    [Theory]
    [InlineData(".lamina-meta/file", null)]
    [InlineData(".lamina-meta/file", "/")]
    [InlineData("visible/.lamina-meta/file", null)]
    [InlineData("visible/.lamina-meta/file", "/")]
    public async Task HiddenNameMutation_MatchesColdScannerIncludingVisibleParent(string key, string? delimiter)
    {
        Directory.CreateDirectory(Path.Combine(_root, "bucket"));
        var indexed = CreateStorage(metadataMode: MetadataStorageMode.SeparateDirectory);
        var scanner = CreateStorage(enabled: false, metadataMode: MetadataStorageMode.SeparateDirectory);
        var query = new ListingQuery(BucketType.GeneralPurpose, delimiter: delimiter);
        await indexed.ListDataCandidatesAsync("bucket", query);
        await using (var write = await indexed.BeginWriteAsync("bucket", key))
        {
            await write.Stream.WriteAsync(new byte[] { 1 });
            using var prepared = await write.SealAsync();
            await indexed.CommitPreparedDataAsync(prepared);
        }
        var actual = await indexed.ListDataCandidatesAsync("bucket", query);
        var expected = await scanner.ListDataCandidatesAsync("bucket", query);
        Assert.Equal(expected.Entries, actual.Entries);
        Assert.True(actual.Statistics.ScannedEntries > 0);
        await indexed.DeleteDataAsync("bucket", key);
        Assert.Equal((await scanner.ListDataCandidatesAsync("bucket", query)).Entries,
            (await indexed.ListDataCandidatesAsync("bucket", query)).Entries);
    }

    [Fact]
    public async Task OversizedDelimiter_IsChargedEvenForEmptyIndex()
    {
        await using var index = NewIndex(sizeLimit: 1024);
        var query = new ListingQuery(BucketType.GeneralPurpose, delimiter: new string('x', 5000));
        var scans = 0;
        for (var n = 0; n < 2; n++)
            await index.GetAsync("bucket", query, (_, _) =>
            {
                scans++;
                return (new ListingCandidates([], new()), false);
            }, _ => true, CancellationToken.None);
        Assert.Equal(2, scans);
    }

    private sealed class ManualClock : TimeProvider
    {
        private long _ticks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => _ticks;
        public void Advance(int seconds) => _ticks += TimeSpan.FromSeconds(seconds).Ticks;
    }

    private static FilesystemListingIndex NewIndex(long sizeLimit = 128 * 1024 * 1024, TimeProvider? time = null) =>
        new(Options.Create(new FilesystemListingIndexSettings { Enabled = true, SizeLimit = sizeLimit }), time);

    private FilesystemObjectDataStorage CreateStorage(bool enabled = true, MetadataStorageMode metadataMode = MetadataStorageMode.Inline)
    {
        var settings = Options.Create(new FilesystemStorageSettings { DataDirectory = _root, MetadataMode = metadataMode });
        return new FilesystemObjectDataStorage(settings,
            new NetworkFileSystemHelper(settings, NullLogger<NetworkFileSystemHelper>.Instance),
            new LinuxZeroCopyHelper(NullLogger<LinuxZeroCopyHelper>.Instance),
            NullLogger<FilesystemObjectDataStorage>.Instance,
            new FilesystemListingIndex(Options.Create(new FilesystemListingIndexSettings { Enabled = enabled })));
    }

    public void Dispose() { if (Directory.Exists(_root)) Directory.Delete(_root, true); }
}
