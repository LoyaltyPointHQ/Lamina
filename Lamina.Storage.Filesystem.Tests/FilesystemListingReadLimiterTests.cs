using Lamina.Storage.Filesystem.Configuration;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem.Tests;

public class FilesystemListingReadLimiterTests
{
    [Fact]
    public async Task ConcurrentBatches_ShareLimit_AndPreserveKeys()
    {
        using var limiter = new FilesystemListingReadLimiter(Options.Create(new FilesystemListingReadSettings { MaxConcurrency = 2 }));
        var active = 0;
        var peak = 0;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task<string?> Read(string key, CancellationToken ct)
        {
            var current = Interlocked.Increment(ref active);
            lock (entered) peak = Math.Max(peak, current);
            if (current == 2) entered.TrySetResult();
            try { await release.Task.WaitAsync(ct); return key == "missing" ? null : key; }
            finally { Interlocked.Decrement(ref active); }
        }
        var first = limiter.ReadBatchAsync(new[] { "a", "b", "missing" }, Read);
        var second = limiter.ReadBatchAsync(new[] { "c", "d" }, Read);
        try { await entered.Task.WaitAsync(TimeSpan.FromSeconds(5)); Assert.Equal(2, Volatile.Read(ref active)); }
        finally { release.TrySetResult(); }
        var results = await Task.WhenAll(first, second);
        Assert.Equal(2, peak);
        Assert.Equal("a", results[0]["a"]);
        Assert.Null(results[0]["missing"]);
        Assert.Equal("d", results[1]["d"]);
    }

    [Fact]
    public async Task ReadFailure_IsNotConvertedToPartialResults()
    {
        using var limiter = new FilesystemListingReadLimiter(Options.Create(new FilesystemListingReadSettings()));
        await Assert.ThrowsAsync<IOException>(() => limiter.ReadBatchAsync<string>(new[] { "key" },
            (_, _) => throw new IOException("unavailable")));
    }
}
