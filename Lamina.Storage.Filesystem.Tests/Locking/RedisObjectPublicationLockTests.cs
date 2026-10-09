using Lamina.Storage.Core;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Filesystem.Locking;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Lamina.Storage.Filesystem.Tests;

[Collection("Redis")]
public class RedisObjectPublicationLockTests
{
    [Fact]
    public void LogicalKeysPreserveCaseAndComponentBoundaries()
    {
        Assert.NotEqual(RedisObjectPublicationLock.GetLockName("ns", "ab", "c"), RedisObjectPublicationLock.GetLockName("ns", "a", "bc"));
        Assert.NotEqual(RedisObjectPublicationLock.GetLockName("ns", "b", "Key"), RedisObjectPublicationLock.GetLockName("ns", "b", "key"));
        Assert.NotEqual(RedisObjectPublicationLock.GetLockName("ns", "b", "key"), RedisObjectPublicationLock.GetLockName("other", "b", "key"));
    }

    [RedisFact]
    public async Task LeaseRenewsAndReleaseAllowsAnotherReplica()
    {
        using var redis = await ConnectionMultiplexer.ConnectAsync(Environment.GetEnvironmentVariable("LAMINA_TEST_REDIS_CONNECTION_STRING")!);
        var settings = Options.Create(new RedisSettings { LockExpirySeconds = 1, AcquisitionTimeoutMs = 50, LockKeyPrefix = $"lamina-test:{Guid.NewGuid():N}" });
        var first = new RedisObjectPublicationLock(redis, settings, NullLogger<RedisObjectPublicationLock>.Instance);
        var second = new RedisObjectPublicationLock(redis, settings, NullLogger<RedisObjectPublicationLock>.Instance);
        var held = await first.AcquireAsync("bucket", "key");
        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(1800));
            await Assert.ThrowsAsync<LockAcquisitionException>(() => second.AcquireAsync("bucket", "key").AsTask());
        }
        finally { await held.DisposeAsync(); }
        await using var acquired = await second.AcquireAsync("bucket", "key");
    }

    [RedisFact]
    public async Task ReplicasSerializeSameLogicalObjectAndRespectCancellation()
    {
        using var redis = await ConnectionMultiplexer.ConnectAsync(Environment.GetEnvironmentVariable("LAMINA_TEST_REDIS_CONNECTION_STRING")!);
        var settings = Options.Create(new RedisSettings { AcquisitionTimeoutMs = 100, LockKeyPrefix = $"lamina-test:{Guid.NewGuid():N}" });
        var first = new RedisObjectPublicationLock(redis, settings, NullLogger<RedisObjectPublicationLock>.Instance);
        var second = new RedisObjectPublicationLock(redis, settings, NullLogger<RedisObjectPublicationLock>.Instance);
        await using var held = await first.AcquireAsync("bucket", "key");
        await Assert.ThrowsAsync<LockAcquisitionException>(() => second.AcquireAsync("bucket", "key").AsTask());
        await using var other = await second.AcquireAsync("bucket", "Key");
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => second.AcquireAsync("bucket", "key", cancelled.Token).AsTask());
    }
}
