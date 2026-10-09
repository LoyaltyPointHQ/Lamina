using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Lamina.Storage.Core;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Core.Integrity;
using Medallion.Threading.Redis;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Lamina.Storage.Filesystem.Locking;

/// <summary>Cross-replica publication lease identified by logical object, independent of mount paths.</summary>
public sealed class RedisObjectPublicationLock : IObjectPublicationLock
{
    private readonly IDatabase _database;
    private readonly RedisSettings _settings;
    private readonly ILogger<RedisObjectPublicationLock> _logger;

    public RedisObjectPublicationLock(ConnectionMultiplexer redis, IOptions<RedisSettings> settings,
        ILogger<RedisObjectPublicationLock> logger)
    {
        _settings = settings.Value;
        _settings.Validate();
        _database = redis.GetDatabase(_settings.Database);
        _logger = logger;
    }

    public async ValueTask<IAsyncDisposable> AcquireAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var name = GetLockName(_settings.LockKeyPrefix, bucketName, key);
        var distributed = new RedisDistributedReaderWriterLock(name, _database, options =>
        {
            options.Expiry(TimeSpan.FromSeconds(_settings.LockExpirySeconds));
            if (_settings.MinBusyWaitSleepTimeMs.HasValue || _settings.MaxBusyWaitSleepTimeMs.HasValue)
                options.BusyWaitSleepTime(TimeSpan.FromMilliseconds(_settings.MinBusyWaitSleepTimeMs ?? 10),
                    TimeSpan.FromMilliseconds(_settings.MaxBusyWaitSleepTimeMs ?? 800));
        });
        var started = Stopwatch.GetTimestamp();
        var handle = await distributed.TryAcquireWriteLockAsync(_settings.GetAcquisitionTimeout(), cancellationToken);
        if (handle == null)
        {
            _logger.LogWarning("Object publication lock unavailable: {LockKey}, wait {WaitMilliseconds} ms, configured timeout {TimeoutMilliseconds} ms",
                name, Stopwatch.GetElapsedTime(started).TotalMilliseconds, _settings.GetAcquisitionTimeout().TotalMilliseconds);
            throw new LockAcquisitionException("object publication");
        }
        if (cancellationToken.IsCancellationRequested)
        {
            await handle.DisposeAsync();
            cancellationToken.ThrowIfCancellationRequested();
        }
        return handle;
    }

    internal static string GetLockName(string prefix, string bucketName, string key)
    {
        // A length-delimited, case-sensitive identity avoids ambiguous concatenation and path normalization.
        var identity = bucketName.Length.ToString(CultureInfo.InvariantCulture) + ":" + bucketName + key;
        return prefix + ":object-publication:" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(identity)));
    }
}
