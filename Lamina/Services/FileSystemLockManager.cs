using System.Collections.Concurrent;
using Nito.AsyncEx;

namespace Lamina.Services;

public interface IFileSystemLockManager
{
    Task<T?> ReadFileAsync<T>(string filePath, Func<string, Task<T>> readOperation, CancellationToken cancellationToken = default);
    Task WriteFileAsync(string filePath, string content, CancellationToken cancellationToken = default);
    Task<bool> DeleteFile(string filePath);
}

public class FileSystemLockManager : IFileSystemLockManager
{
    private class LockInfo : IDisposable
    {
        private readonly string _filePath;
        private readonly FileSystemLockManager _manager;
        public AsyncReaderWriterLock Lock { get; }
        private int _referenceCount;

        public LockInfo(string filePath, FileSystemLockManager manager)
        {
            _filePath = filePath;
            _manager = manager;
            Lock = new AsyncReaderWriterLock();
        }

        public void Acquire() => Interlocked.Increment(ref _referenceCount);

        public void Dispose()
        {
            lock (_manager._acquireLock)
            {
                if (Interlocked.Decrement(ref _referenceCount) > 0)
                    return;
                _manager._locks.TryRemove(_filePath, out _);
            }
        }
    }

    private readonly ConcurrentDictionary<string, LockInfo> _locks = new();
    private readonly object _acquireLock = new();
    public async Task<T?> ReadFileAsync<T>(string filePath, Func<string, Task<T>> readOperation, CancellationToken cancellationToken = default)
    {
        var lockKey = GetNormalizedPath(filePath);
        using var lockInfo = AcquireLockInfo(lockKey);

        using var readLock = await lockInfo.Lock.ReaderLockAsync();

        if (!File.Exists(filePath))
        {
            return default;
        }

        var content = await File.ReadAllTextAsync(filePath, cancellationToken);
        return await readOperation(content);
    }

    public async Task WriteFileAsync(string filePath, string content, CancellationToken cancellationToken = default)
    {
        var lockKey = GetNormalizedPath(filePath);
        using var lockInfo = AcquireLockInfo(lockKey);

        using var writeLock = await lockInfo.Lock.WriterLockAsync();

        // Ensure directory exists
        var directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await File.WriteAllTextAsync(filePath, content, cancellationToken);
    }

    public async Task<bool> DeleteFile(string filePath)
    {
        var lockKey = GetNormalizedPath(filePath);
        using var lockInfo = AcquireLockInfo(lockKey);

        using var writeLock = await lockInfo.Lock.WriterLockAsync();

        if (File.Exists(filePath))
        {
            File.Delete(filePath);
            return true;
        }

        return false;
    }

    private LockInfo AcquireLockInfo(string lockKey)
    {
        lock (_acquireLock)
        {
            var lockInfo = _locks.GetOrAdd(lockKey, k => new LockInfo(k, this));
            lockInfo.Acquire();

            return lockInfo;
        }
    }

    private string GetNormalizedPath(string path)
    {
        try
        {
            // Normalize path to handle different representations of the same path
            return Path.GetFullPath(path).ToLowerInvariant();
        }
        catch
        {
            // If path normalization fails, use as-is
            return path.ToLowerInvariant();
        }
    }
}