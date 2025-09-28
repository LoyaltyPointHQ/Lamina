using System.Text.Json;
using Lamina.Storage.Core.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;

namespace Lamina.Storage.Filesystem.Locking;

public class RedisLockManager : IFileSystemLockManager, IDisposable
{
    private readonly RedLockFactory _redLockFactory;
    private readonly ConnectionMultiplexer _redis;
    private readonly RedisSettings _settings;
    private readonly ILogger<RedisLockManager> _logger;
    private bool _disposed;

    public RedisLockManager(
        ConnectionMultiplexer redis,
        RedLockFactory redLockFactory,
        IOptions<RedisSettings> settingsOptions,
        ILogger<RedisLockManager> logger)
    {
        _redis = redis ?? throw new ArgumentNullException(nameof(redis));
        _redLockFactory = redLockFactory ?? throw new ArgumentNullException(nameof(redLockFactory));
        _settings = settingsOptions.Value ?? throw new ArgumentNullException(nameof(settingsOptions));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<T?> ReadFileAsync<T>(string filePath, Func<string, Task<T>> readOperation, CancellationToken cancellationToken = default)
    {
        var lockKey = GetLockKey(filePath, "read");
        var lockExpiry = TimeSpan.FromSeconds(_settings.LockExpirySeconds);

        var waitTime = TimeSpan.FromMilliseconds(_settings.RetryDelayMs * _settings.RetryCount);

        var retryTime = TimeSpan.FromMilliseconds(_settings.RetryDelayMs);

        using var redLock = await _redLockFactory.CreateLockAsync(
            lockKey,
            lockExpiry,
            waitTime,
            retryTime,
            cancellationToken: cancellationToken);

        if (!redLock.IsAcquired)
        {
            _logger.LogWarning("Failed to acquire read lock for path: {FilePath}", filePath);
            throw new InvalidOperationException($"Failed to acquire read lock for path: {filePath}");
        }

        _logger.LogDebug("Acquired read lock for path: {FilePath}", filePath);

        try
        {
            if (!File.Exists(filePath))
            {
                return default;
            }

            var content = await File.ReadAllTextAsync(filePath, cancellationToken);
            return await readOperation(content);
        }
        finally
        {
            _logger.LogDebug("Released read lock for path: {FilePath}", filePath);
        }
    }

    public async Task WriteFileAsync(string filePath, string content, CancellationToken cancellationToken = default)
    {
        var lockKey = GetLockKey(filePath, "write");
        var lockExpiry = TimeSpan.FromSeconds(_settings.LockExpirySeconds);

        var waitTime = TimeSpan.FromMilliseconds(_settings.RetryDelayMs * _settings.RetryCount);

        var retryTime = TimeSpan.FromMilliseconds(_settings.RetryDelayMs);

        using var redLock = await _redLockFactory.CreateLockAsync(
            lockKey,
            lockExpiry,
            waitTime,
            retryTime,
            cancellationToken: cancellationToken);

        if (!redLock.IsAcquired)
        {
            _logger.LogWarning("Failed to acquire write lock for path: {FilePath}", filePath);
            throw new InvalidOperationException($"Failed to acquire write lock for path: {filePath}");
        }

        _logger.LogDebug("Acquired write lock for path: {FilePath}", filePath);

        try
        {
            // Ensure directory exists
            var directory = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            await File.WriteAllTextAsync(filePath, content, cancellationToken);
        }
        finally
        {
            _logger.LogDebug("Released write lock for path: {FilePath}", filePath);
        }
    }

    public async Task<bool> DeleteFile(string filePath)
    {
        var lockKey = GetLockKey(filePath, "delete");
        var lockExpiry = TimeSpan.FromSeconds(_settings.LockExpirySeconds);
        var waitTime = TimeSpan.FromMilliseconds(_settings.RetryDelayMs * _settings.RetryCount);

        var retryTime = TimeSpan.FromMilliseconds(_settings.RetryDelayMs);

        using var redLock = await _redLockFactory.CreateLockAsync(lockKey, lockExpiry, waitTime, retryTime);

        if (!redLock.IsAcquired)
        {
            _logger.LogWarning("Failed to acquire delete lock for path: {FilePath}", filePath);
            throw new InvalidOperationException($"Failed to acquire delete lock for path: {filePath}");
        }

        _logger.LogDebug("Acquired delete lock for path: {FilePath}", filePath);

        try
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
                return true;
            }

            return false;
        }
        finally
        {
            _logger.LogDebug("Released delete lock for path: {FilePath}", filePath);
        }
    }

    private string GetLockKey(string filePath, string operation)
    {
        try
        {
            // Normalize path to handle different representations of the same path
            var normalizedPath = Path.GetFullPath(filePath).ToLowerInvariant();
            return $"{_settings.LockKeyPrefix}:{operation}:{normalizedPath}";
        }
        catch
        {
            // If path normalization fails, use as-is
            return $"{_settings.LockKeyPrefix}:{operation}:{filePath.ToLowerInvariant()}";
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        try
        {
            _redLockFactory?.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing RedLockFactory");
        }

        try
        {
            _redis?.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing Redis connection");
        }

        _disposed = true;
    }
}