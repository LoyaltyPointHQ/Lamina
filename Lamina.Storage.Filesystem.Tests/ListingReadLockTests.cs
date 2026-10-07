using Lamina.Storage.Filesystem.Locking;

namespace Lamina.Storage.Filesystem.Tests;

public sealed class ListingReadLockTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "lamina-read-lock-" + Guid.NewGuid().ToString("N"));

    public ListingReadLockTests() => Directory.CreateDirectory(_root);

    [Fact]
    public async Task ReadFileAsync_PropagatesAccessErrorInsteadOfTreatingItAsMissing()
    {
        var locks = new InMemoryLockManager();
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => locks.ReadFileAsync(_root, Task.FromResult));
    }

    [Fact]
    public async Task ReadFileAsync_CanCancelWhileWaitingForWriter()
    {
        var locks = new InMemoryLockManager();
        var path = Path.Combine(_root, "metadata.json");
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writer = locks.UpdateFileAsync(path, async _ =>
        {
            entered.SetResult();
            await release.Task;
            return "metadata";
        });
        await entered.Task;
        using var cancellation = new CancellationTokenSource();
        var reader = locks.ReadFileAsync(path, Task.FromResult, cancellation.Token);
        try
        {
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reader.WaitAsync(TimeSpan.FromSeconds(2)));
        }
        finally
        {
            release.TrySetResult();
            await writer;
            try { await reader; } catch (OperationCanceledException) { }
        }
    }

    [Fact]
    public async Task ReadFileAsync_MissingFileRemainsNull()
    {
        var locks = new InMemoryLockManager();
        Assert.Null(await locks.ReadFileAsync(Path.Combine(_root, "missing"), Task.FromResult));
    }

    public void Dispose() => Directory.Delete(_root, true);
}
