using Lamina.Storage.Core.Configuration;
using Medallion.Threading.Redis;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Lamina.Storage.Filesystem.Locking;

public class RedisLockManager : IFileSystemLockManager, IDisposable
{
    private readonly IDatabase _database;
    private readonly RedisSettings _settings;
    private readonly ILogger<RedisLockManager> _logger;

    public RedisLockManager(
        ConnectionMultiplexer redis,
        IOptions<RedisSettings> settingsOptions,
        ILogger<RedisLockManager> logger)
    {
        _database = redis.GetDatabase(settingsOptions.Value.Database);
        _settings = settingsOptions.Value;
        _logger = logger;
    }

    public async Task<T?> ReadFileAsync<T>(string filePath, Func<string, Task<T>> readOperation, CancellationToken cancellationToken = default)
    {
        var lockName = GetLockName(filePath);
        var rwLock = new RedisDistributedReaderWriterLock(lockName, _database);

        var timeout = TimeSpan.FromMilliseconds(_settings.RetryDelayMs * _settings.RetryCount);

        await using var handle = await rwLock.TryAcquireReadLockAsync(timeout, cancellationToken);

        if (handle == null)
        {
            _logger.LogWarning("Failed to acquire read lock for path: {FilePath}", filePath);
            throw new InvalidOperationException($"Failed to acquire read lock for path: {filePath}");
        }

        _logger.LogDebug("Acquired read lock for path: {FilePath}", filePath);

        if (!File.Exists(filePath))
            return default;

        var content = await File.ReadAllTextAsync(filePath, cancellationToken);
        return await readOperation(content);
    }

    public async Task WriteFileAsync(string filePath, string content, CancellationToken cancellationToken = default)
    {
        var lockName = GetLockName(filePath);
        var rwLock = new RedisDistributedReaderWriterLock(lockName, _database);

        var timeout = TimeSpan.FromMilliseconds(_settings.RetryDelayMs * _settings.RetryCount);

        await using var handle = await rwLock.TryAcquireWriteLockAsync(timeout, cancellationToken);

        if (handle == null)
        {
            _logger.LogWarning("Failed to acquire write lock for path: {FilePath}", filePath);
            throw new InvalidOperationException($"Failed to acquire write lock for path: {filePath}");
        }

        _logger.LogDebug("Acquired write lock for path: {FilePath}", filePath);

        var directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        await File.WriteAllTextAsync(filePath, content, cancellationToken);
    }

    public async Task<bool> DeleteFile(string filePath)
    {
        var lockName = GetLockName(filePath);
        var rwLock = new RedisDistributedReaderWriterLock(lockName, _database);

        var timeout = TimeSpan.FromMilliseconds(_settings.RetryDelayMs * _settings.RetryCount);

        await using var handle = await rwLock.TryAcquireWriteLockAsync(timeout);

        if (handle == null)
        {
            _logger.LogWarning("Failed to acquire delete lock for path: {FilePath}", filePath);
            throw new InvalidOperationException($"Failed to acquire delete lock for path: {filePath}");
        }

        _logger.LogDebug("Acquired delete lock for path: {FilePath}", filePath);

        if (!File.Exists(filePath))
            return false;

        File.Delete(filePath);
        return true;
    }

    private string GetLockName(string filePath)
    {
        try
        {
            var normalizedPath = Path.GetFullPath(filePath).ToLowerInvariant();
            return $"{_settings.LockKeyPrefix}:{normalizedPath}";
        }
        catch
        {
            return $"{_settings.LockKeyPrefix}:{filePath.ToLowerInvariant()}";
        }
    }

    public void Dispose()
    {
        // No resources to dispose - RedisDistributedReaderWriterLock doesn't need disposal
        // The ConnectionMultiplexer is managed externally via DI
    }
}
