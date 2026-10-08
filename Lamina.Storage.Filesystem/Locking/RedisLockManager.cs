using System.Diagnostics;
using Lamina.Storage.Core;
using Lamina.Storage.Core.Configuration;
using Medallion.Threading.Redis;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Lamina.Storage.Filesystem.Locking;

public class RedisLockManager : IFileSystemLockManager
{
    private readonly IDatabase _database;
    private readonly RedisSettings _settings;
    private readonly ILogger<RedisLockManager> _logger;

    public RedisLockManager(ConnectionMultiplexer redis, IOptions<RedisSettings> settingsOptions,
        ILogger<RedisLockManager> logger)
    {
        _settings = settingsOptions.Value;
        _settings.Validate();
        _database = redis.GetDatabase(_settings.Database);
        _logger = logger;
    }

    public Task<T?> ReadFileAsync<T>(string filePath, Func<string, Task<T>> readOperation,
        CancellationToken cancellationToken = default) => WithLockAsync<T?>(filePath, "read", async () =>
    {
        string content;
        try
        {
            content = await File.ReadAllTextAsync(filePath, cancellationToken);
        }
        catch (FileNotFoundException) { return default; }
        catch (DirectoryNotFoundException) { return default; }
        // Callers may also read the file timestamp: keep content and version under the same lease.
        return string.IsNullOrWhiteSpace(content) ? default : await readOperation(content);
    }, cancellationToken);

    public async Task WriteFileAsync(string filePath, string content, CancellationToken cancellationToken = default)
    {
        await WithLockAsync(filePath, "write", async () =>
        {
            await WriteAtomicAsync(filePath, content, cancellationToken);
            return true;
        }, cancellationToken);
    }

    public Task<bool> UpdateFileAsync(string filePath, Func<string?, Task<string?>> transform,
        CancellationToken cancellationToken = default) => WithLockAsync(filePath, "update", async () =>
    {
        var current = File.Exists(filePath) ? await File.ReadAllTextAsync(filePath, cancellationToken) : null;
        var updated = await transform(current);
        if (updated == null) return false;
        await WriteAtomicAsync(filePath, updated, cancellationToken);
        return true;
    }, cancellationToken);

    public Task<bool> DeleteFile(string filePath) => WithLockAsync(filePath, "delete", () =>
    {
        if (!File.Exists(filePath)) return Task.FromResult(false);
        File.Delete(filePath);
        return Task.FromResult(true);
    }, CancellationToken.None);

    private async Task<T> WithLockAsync<T>(string filePath, string operation, Func<Task<T>> action,
        CancellationToken cancellationToken)
    {
        var lockKey = GetLockName(filePath);
        var rwLock = new RedisDistributedReaderWriterLock(lockKey, _database, options =>
        {
            options.Expiry(TimeSpan.FromSeconds(_settings.LockExpirySeconds));
            if (_settings.MinBusyWaitSleepTimeMs.HasValue || _settings.MaxBusyWaitSleepTimeMs.HasValue)
                options.BusyWaitSleepTime(TimeSpan.FromMilliseconds(_settings.MinBusyWaitSleepTimeMs ?? 10),
                    TimeSpan.FromMilliseconds(_settings.MaxBusyWaitSleepTimeMs ?? 800));
        });
        var started = Stopwatch.GetTimestamp();
        RedisDistributedReaderWriterLockHandle? handle;
        try
        {
            handle = operation == "read"
                ? await rwLock.TryAcquireReadLockAsync(_settings.GetAcquisitionTimeout(), cancellationToken)
                : await rwLock.TryAcquireWriteLockAsync(_settings.GetAcquisitionTimeout(), cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Metadata lock acquisition failed: {Operation}, {FilePath}, {LockKey}, wait {WaitMilliseconds} ms",
                operation, filePath, lockKey, Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            throw;
        }
        var wait = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        if (handle == null)
        {
            _logger.LogWarning("Metadata lock unavailable: {Operation}, {FilePath}, {LockKey}, wait {WaitMilliseconds} ms, configured timeout {TimeoutMilliseconds} ms",
                operation, filePath, lockKey, wait, _settings.GetAcquisitionTimeout().TotalMilliseconds);
            throw new LockAcquisitionException(operation);
        }
        var acquired = Stopwatch.GetTimestamp();
        try
        {
            await using (handle)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return await action();
            }
        }
        finally
        {
            _logger.LogDebug("Metadata lock scope finished: {Operation}, {FilePath}, {LockKey}, wait {WaitMilliseconds} ms, hold/release {HoldMilliseconds} ms",
                operation, filePath, lockKey, wait, Stopwatch.GetElapsedTime(acquired).TotalMilliseconds);
        }
    }

    private static async Task WriteAtomicAsync(string filePath, string content, CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
        var tempPath = Path.Combine(directory ?? ".", $".lamina-tmp-{Guid.NewGuid():N}");
        try
        {
            await File.WriteAllTextAsync(tempPath, content, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            File.Move(tempPath, filePath, overwrite: true);
        }
        catch
        {
            try { File.Delete(tempPath); } catch { /* Preserve the original failure. */ }
            throw;
        }
    }

    private string GetLockName(string filePath)
    {
        try { return $"{_settings.LockKeyPrefix}:{Path.GetFullPath(filePath).ToLowerInvariant()}"; }
        catch { return $"{_settings.LockKeyPrefix}:{filePath.ToLowerInvariant()}"; }
    }
}
