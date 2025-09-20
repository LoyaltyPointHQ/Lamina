using System.Collections.Concurrent;

namespace S3Test.Services;

public interface IFileSystemLockManager
{
    Task<T> ReadFileAsync<T>(string filePath, Func<string, Task<T>> readOperation, CancellationToken cancellationToken = default);
    Task WriteFileAsync(string filePath, Func<Task<string>> writeOperation, CancellationToken cancellationToken = default);
    Task<bool> DeleteFileAsync(string filePath, CancellationToken cancellationToken = default);
}

public class FileSystemLockManager : IFileSystemLockManager, IDisposable
{
    private class LockInfo : IDisposable
    {
        private readonly string _filePath;
        private readonly FileSystemLockManager _manager;
        public ReaderWriterLockSlim Lock { get; }
        private int _referenceCount;

        public LockInfo(string filePath, FileSystemLockManager manager)
        {
            _filePath = filePath;
            _manager = manager;
            Lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        }

        public void Acquire() => _referenceCount++;

        public void Dispose()
        {
            lock (_manager._acquireLock)
            {
                if (--_referenceCount > 0)
                    return;
                Lock.Dispose();
                _manager._locks.TryRemove(_filePath, out _);
            }
        }
    }

    private readonly ConcurrentDictionary<string, LockInfo> _locks = new();
    private readonly object _acquireLock = new object();
    private readonly ILogger<FileSystemLockManager> _logger;
    private readonly TimeSpan _lockTimeout = TimeSpan.FromSeconds(5);
    private bool _disposed;

    public FileSystemLockManager(ILogger<FileSystemLockManager> logger)
    {
        _logger = logger;
    }

    public async Task<T> ReadFileAsync<T>(string filePath, Func<string, Task<T>> readOperation, CancellationToken cancellationToken = default)
    {
        var lockKey = GetNormalizedPath(filePath);
        using var lockInfo = AcquireLockInfo(lockKey);

        bool lockAcquired = false;
        try
        {
            if (!lockInfo.Lock.TryEnterReadLock(_lockTimeout))
            {
                throw new TimeoutException($"Could not acquire read lock for {filePath} within {_lockTimeout.TotalSeconds} seconds");
            }

            lockAcquired = true;

            if (!File.Exists(filePath))
            {
                return default(T)!;
            }

            var content = await File.ReadAllTextAsync(filePath, cancellationToken);
            return await readOperation(content);
        }
        finally
        {
            if (lockAcquired)
            {
                try
                {
                    lockInfo.Lock.ExitReadLock();
                }
                catch (SynchronizationLockException)
                {
                    // Lock was already released or disposed
                    _logger.LogWarning("Read lock was already released for {FilePath}", filePath);
                }
                catch (ObjectDisposedException)
                {
                    // Lock was disposed
                    _logger.LogWarning("Read lock was disposed for {FilePath}", filePath);
                }
            }
        }
    }

    public async Task WriteFileAsync(string filePath, Func<Task<string>> writeOperation, CancellationToken cancellationToken = default)
    {
        var lockKey = GetNormalizedPath(filePath);
        using var lockInfo = AcquireLockInfo(lockKey);

        bool lockAcquired = false;
        try
        {
            if (!lockInfo.Lock.TryEnterWriteLock(_lockTimeout))
            {
                throw new TimeoutException($"Could not acquire write lock for {filePath} within {_lockTimeout.TotalSeconds} seconds");
            }

            lockAcquired = true;

            // Generate new content
            var newContent = await writeOperation();

            // Ensure directory exists
            var directory = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            await File.WriteAllTextAsync(filePath, newContent, cancellationToken);
        }
        finally
        {
            if (lockAcquired)
            {
                try
                {
                    lockInfo.Lock.ExitWriteLock();
                }
                catch (SynchronizationLockException)
                {
                    // Lock was already released or disposed
                    _logger.LogWarning("Write lock was already released for {FilePath}", filePath);
                }
                catch (ObjectDisposedException)
                {
                    // Lock was disposed
                    _logger.LogWarning("Write lock was disposed for {FilePath}", filePath);
                }
            }
        }
    }

    public async Task<bool> DeleteFileAsync(string filePath, CancellationToken cancellationToken = default)
    {
        var lockKey = GetNormalizedPath(filePath);
        using var lockInfo = AcquireLockInfo(lockKey);

        bool lockAcquired = false;
        try
        {
            if (!lockInfo.Lock.TryEnterWriteLock(_lockTimeout))
            {
                throw new TimeoutException($"Could not acquire write lock for {filePath} within {_lockTimeout.TotalSeconds} seconds");
            }

            lockAcquired = true;

            if (File.Exists(filePath))
            {
                File.Delete(filePath);
                return true;
            }

            return false;
        }
        finally
        {
            if (lockAcquired)
            {
                try
                {
                    lockInfo.Lock.ExitWriteLock();
                }
                catch (SynchronizationLockException)
                {
                    // Lock was already released or disposed
                    _logger.LogWarning("Write lock was already released for delete {FilePath}", filePath);
                }
                catch (ObjectDisposedException)
                {
                    // Lock was disposed
                    _logger.LogWarning("Write lock was disposed for delete {FilePath}", filePath);
                }
            }
        }
    }

    private LockInfo AcquireLockInfo(string lockKey)
    {
        lock (_acquireLock)
        {
            var lockInfo = _locks.GetOrAdd(lockKey, _ => new LockInfo(lockKey, this));
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

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        // Dispose all locks
        foreach (var kvp in _locks)
        {
            try
            {
                kvp.Value.Lock.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error disposing lock for {LockKey}", kvp.Key);
            }
        }

        _locks.Clear();
    }
}