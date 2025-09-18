using System.Collections.Concurrent;

namespace S3Test.Services;

public interface IFileSystemLockManager
{
    Task<T> ReadFileAsync<T>(string filePath, Func<string, Task<T>> readOperation, CancellationToken cancellationToken = default);
    Task WriteFileAsync(string filePath, Func<string?, Task<string>> writeOperation, CancellationToken cancellationToken = default);
    Task<bool> DeleteFileAsync(string filePath, CancellationToken cancellationToken = default);
    Task<T> ExecuteWithLockAsync<T>(string lockKey, Func<Task<T>> operation, bool isWrite = false, CancellationToken cancellationToken = default, TimeSpan? timeout = null);
}

public class FileSystemLockManager : IFileSystemLockManager, IDisposable
{
    private class LockInfo
    {
        public ReaderWriterLockSlim Lock { get; }
        private int _referenceCount;
        public DateTime LastAccessed { get; set; }

        public LockInfo()
        {
            Lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
            _referenceCount = 0;
            LastAccessed = DateTime.UtcNow;
        }

        public int IncrementRefCount() => Interlocked.Increment(ref _referenceCount);
        public int DecrementRefCount() => Interlocked.Decrement(ref _referenceCount);
        public int GetRefCount() => Volatile.Read(ref _referenceCount);
    }

    private readonly ConcurrentDictionary<string, LockInfo> _locks = new();
    private readonly ILogger<FileSystemLockManager> _logger;
    private readonly TimeSpan _lockTimeout = TimeSpan.FromSeconds(30);
    private readonly Timer _cleanupTimer;
    private readonly TimeSpan _cleanupInterval = TimeSpan.FromMinutes(5);
    private readonly TimeSpan _lockMaxIdleTime = TimeSpan.FromMinutes(10);
    private bool _disposed;

    public FileSystemLockManager(ILogger<FileSystemLockManager> logger)
    {
        _logger = logger;

        // Start a timer to periodically clean up unused locks
        _cleanupTimer = new Timer(CleanupUnusedLocks, null, _cleanupInterval, _cleanupInterval);
    }

    public async Task<T> ReadFileAsync<T>(string filePath, Func<string, Task<T>> readOperation, CancellationToken cancellationToken = default)
    {
        var lockKey = GetNormalizedPath(filePath);
        var lockInfo = AcquireLockInfo(lockKey);

        bool lockAcquired = false;
        try
        {
            if (!lockInfo.Lock.TryEnterReadLock(_lockTimeout))
            {
                throw new TimeoutException($"Could not acquire read lock for {filePath} within {_lockTimeout.TotalSeconds} seconds");
            }
            lockAcquired = true;
            lockInfo.LastAccessed = DateTime.UtcNow;

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

            ReleaseLockInfo(lockKey);
        }
    }

    public async Task WriteFileAsync(string filePath, Func<string?, Task<string>> writeOperation, CancellationToken cancellationToken = default)
    {
        var lockKey = GetNormalizedPath(filePath);
        var lockInfo = AcquireLockInfo(lockKey);

        bool lockAcquired = false;
        try
        {
            if (!lockInfo.Lock.TryEnterWriteLock(_lockTimeout))
            {
                throw new TimeoutException($"Could not acquire write lock for {filePath} within {_lockTimeout.TotalSeconds} seconds");
            }
            lockAcquired = true;
            lockInfo.LastAccessed = DateTime.UtcNow;

            // Read existing content if file exists
            string? existingContent = null;
            if (File.Exists(filePath))
            {
                existingContent = await File.ReadAllTextAsync(filePath, cancellationToken);
            }

            // Generate new content
            var newContent = await writeOperation(existingContent);

            // Ensure directory exists
            var directory = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            // Write atomically using a temporary file
            var tempFile = $"{filePath}.tmp.{Guid.NewGuid():N}";
            try
            {
                await File.WriteAllTextAsync(tempFile, newContent, cancellationToken);

                // Atomic move (on same filesystem)
                File.Move(tempFile, filePath, overwrite: true);
            }
            catch
            {
                // Cleanup temp file on error
                if (File.Exists(tempFile))
                {
                    try { File.Delete(tempFile); } catch { }
                }
                throw;
            }
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

            ReleaseLockInfo(lockKey);
        }
    }

    public async Task<bool> DeleteFileAsync(string filePath, CancellationToken cancellationToken = default)
    {
        var lockKey = GetNormalizedPath(filePath);
        var lockInfo = AcquireLockInfo(lockKey);

        bool lockAcquired = false;
        try
        {
            if (!lockInfo.Lock.TryEnterWriteLock(_lockTimeout))
            {
                throw new TimeoutException($"Could not acquire write lock for {filePath} within {_lockTimeout.TotalSeconds} seconds");
            }
            lockAcquired = true;
            lockInfo.LastAccessed = DateTime.UtcNow;

            if (File.Exists(filePath))
            {
                await Task.Run(() => File.Delete(filePath), cancellationToken);
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

            ReleaseLockInfo(lockKey);
        }
    }

    public async Task<T> ExecuteWithLockAsync<T>(string lockKey, Func<Task<T>> operation, bool isWrite = false, CancellationToken cancellationToken = default, TimeSpan? timeout = null)
    {
        lockKey = GetNormalizedPath(lockKey);
        var lockInfo = AcquireLockInfo(lockKey);
        var effectiveTimeout = timeout ?? _lockTimeout;

        bool lockAcquired = false;
        try
        {
            if (isWrite)
            {
                if (!lockInfo.Lock.TryEnterWriteLock(effectiveTimeout))
                {
                    throw new TimeoutException($"Could not acquire write lock for {lockKey} within {effectiveTimeout.TotalSeconds} seconds");
                }
            }
            else
            {
                if (!lockInfo.Lock.TryEnterReadLock(effectiveTimeout))
                {
                    throw new TimeoutException($"Could not acquire read lock for {lockKey} within {effectiveTimeout.TotalSeconds} seconds");
                }
            }

            lockAcquired = true;
            lockInfo.LastAccessed = DateTime.UtcNow;
            return await operation();
        }
        finally
        {
            if (lockAcquired)
            {
                try
                {
                    if (isWrite)
                    {
                        lockInfo.Lock.ExitWriteLock();
                    }
                    else
                    {
                        lockInfo.Lock.ExitReadLock();
                    }
                }
                catch (SynchronizationLockException)
                {
                    // Lock was already released or disposed
                    _logger.LogWarning("{LockType} lock was already released for {LockKey}", isWrite ? "Write" : "Read", lockKey);
                }
                catch (ObjectDisposedException)
                {
                    // Lock was disposed
                    _logger.LogWarning("{LockType} lock was disposed for {LockKey}", isWrite ? "Write" : "Read", lockKey);
                }
            }

            ReleaseLockInfo(lockKey);
        }
    }

    private LockInfo AcquireLockInfo(string lockKey)
    {
        while (true)
        {
            var lockInfo = _locks.GetOrAdd(lockKey, _ => new LockInfo());

            // Atomically increment reference count
            var oldCount = lockInfo.IncrementRefCount();

            // If this was the first reference (oldCount is now 1), we're good
            if (oldCount > 0)
            {
                return lockInfo;
            }

            // If reference count was 0 or negative, the lock might be getting cleaned up
            // Decrement back and try again
            lockInfo.DecrementRefCount();

            // Small delay to avoid tight spinning
            Thread.Yield();
        }
    }

    private void ReleaseLockInfo(string lockKey)
    {
        if (_locks.TryGetValue(lockKey, out var lockInfo))
        {
            lockInfo.DecrementRefCount();
        }
    }

    private void CleanupUnusedLocks(object? state)
    {
        if (_disposed)
            return;

        try
        {
            var now = DateTime.UtcNow;
            var keysToRemove = new List<string>();

            foreach (var kvp in _locks)
            {
                var lockInfo = kvp.Value;

                // Only consider locks that:
                // 1. Have zero references
                // 2. Haven't been accessed recently
                // 3. Are not currently held
                if (lockInfo.GetRefCount() == 0 &&
                    (now - lockInfo.LastAccessed) > _lockMaxIdleTime &&
                    lockInfo.Lock.CurrentReadCount == 0 &&
                    !lockInfo.Lock.IsWriteLockHeld &&
                    lockInfo.Lock.WaitingReadCount == 0 &&
                    lockInfo.Lock.WaitingWriteCount == 0 &&
                    lockInfo.Lock.WaitingUpgradeCount == 0)
                {
                    keysToRemove.Add(kvp.Key);
                }
            }

            foreach (var key in keysToRemove)
            {
                if (_locks.TryRemove(key, out var removedLockInfo))
                {
                    // Double-check that it's still safe to remove
                    if (removedLockInfo.GetRefCount() == 0)
                    {
                        try
                        {
                            removedLockInfo.Lock.Dispose();
                            _logger.LogDebug("Cleaned up unused lock for {LockKey}", key);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Error disposing lock for {LockKey}", key);
                        }
                    }
                    else
                    {
                        // Someone acquired it while we were removing, put it back
                        _locks.TryAdd(key, removedLockInfo);
                    }
                }
            }

            if (keysToRemove.Count > 0)
            {
                _logger.LogInformation("Cleaned up {Count} unused locks. Total locks: {Total}",
                    keysToRemove.Count, _locks.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during lock cleanup");
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

        // Stop the cleanup timer
        _cleanupTimer.Dispose();

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