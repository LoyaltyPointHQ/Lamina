namespace Lamina.Storage.Core.Integrity;

/// <summary>Reference-counted logical object locks; idle entries are removed.</summary>
public sealed class InMemoryObjectPublicationLock : IObjectPublicationLock
{
    public static InMemoryObjectPublicationLock Shared { get; } = new();
    private readonly Dictionary<(string Bucket, string Key), Entry> _entries = new();
    private sealed class Entry
    {
        public readonly SemaphoreSlim Semaphore = new(1, 1);
        public int References;
    }

    public async ValueTask<IAsyncDisposable> AcquireAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var id = (bucketName, key);
        Entry entry;
        lock (_entries)
        {
            if (!_entries.TryGetValue(id, out entry!)) _entries.Add(id, entry = new());
            entry.References++;
        }
        try { await entry.Semaphore.WaitAsync(cancellationToken); }
        catch { ReleaseReference(id, entry); throw; }
        return new Lease(this, id, entry);
    }

    private void ReleaseReference((string, string) id, Entry entry)
    {
        lock (_entries)
            if (--entry.References == 0)
            {
                _entries.Remove(id);
                entry.Semaphore.Dispose();
            }
    }

    private sealed class Lease(InMemoryObjectPublicationLock owner, (string, string) id, Entry entry) : IAsyncDisposable
    {
        private int _disposed;
        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                entry.Semaphore.Release();
                owner.ReleaseReference(id, entry);
            }
            return ValueTask.CompletedTask;
        }
    }
}
