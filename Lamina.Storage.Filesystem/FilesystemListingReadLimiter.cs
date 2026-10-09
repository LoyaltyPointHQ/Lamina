using Lamina.Storage.Filesystem.Configuration;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

/// <summary>Shared across scoped metadata stores and requests; synchronous filesystem work runs on bounded workers.</summary>
public sealed class FilesystemListingReadLimiter : IDisposable
{
    internal static FilesystemListingReadLimiter Shared { get; } = new(Options.Create(new FilesystemListingReadSettings()));
    private readonly SemaphoreSlim _slots;
    private readonly int _concurrency;

    public FilesystemListingReadLimiter(IOptions<FilesystemListingReadSettings> options)
    {
        options.Value.Validate();
        _concurrency = options.Value.MaxConcurrency;
        _slots = new SemaphoreSlim(_concurrency, _concurrency);
    }

    public async Task<Dictionary<string, T>> ReadBatchAsync<T>(IEnumerable<string> keys,
        Func<string, CancellationToken, Task<T>> read, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var names = keys.Distinct(StringComparer.Ordinal).ToArray();
        var values = new T[names.Length];
        await Parallel.ForEachAsync(Enumerable.Range(0, names.Length),
            new ParallelOptions { MaxDegreeOfParallelism = _concurrency, CancellationToken = cancellationToken }, async (i, ct) =>
            {
                await _slots.WaitAsync(ct);
                try { values[i] = await read(names[i], ct); }
                finally { _slots.Release(); }
            });
        var result = new Dictionary<string, T>(names.Length, StringComparer.Ordinal);
        for (var i = 0; i < names.Length; i++) result.Add(names[i], values[i]);
        return result;
    }

    public void Dispose() => _slots.Dispose();
}
