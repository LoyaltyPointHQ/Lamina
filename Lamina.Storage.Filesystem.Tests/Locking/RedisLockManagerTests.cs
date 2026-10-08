using Lamina.Storage.Core;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Filesystem.Locking;
using Medallion.Threading.Redis;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Net;
using System.Net.Sockets;

namespace Lamina.Storage.Filesystem.Tests;

public sealed class RedisFactAttribute : FactAttribute
{
    public RedisFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("LAMINA_TEST_REDIS_CONNECTION_STRING")))
            Skip = "Set LAMINA_TEST_REDIS_CONNECTION_STRING to an isolated test Redis instance.";
    }
}

[Collection("Redis")]
public sealed class RedisLockManagerTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"redis-lock-test-{Guid.NewGuid():N}");
    private readonly RedisSettings _settings = new()
    {
        AcquisitionTimeoutMs = 100,
        MinBusyWaitSleepTimeMs = 5,
        MaxBusyWaitSleepTimeMs = 10,
        LockKeyPrefix = $"lamina-test:{Guid.NewGuid():N}"
    };
    private ConnectionMultiplexer? _redis;
    private string FilePath => Path.Combine(_directory, "metadata.json");
    private ConnectionMultiplexer Redis => _redis ??= ConnectionMultiplexer.Connect(
        Environment.GetEnvironmentVariable("LAMINA_TEST_REDIS_CONNECTION_STRING")
        ?? throw new InvalidOperationException("An isolated test Redis must be explicitly configured."));
    private RedisLockManager Manager => new(Redis, Options.Create(_settings), NullLogger<RedisLockManager>.Instance);
    private RedisDistributedReaderWriterLock RawLock => new(
        $"{_settings.LockKeyPrefix}:{Path.GetFullPath(FilePath).ToLowerInvariant()}", Redis.GetDatabase());

    [RedisFact]
    public async Task WriteReadUpdateDelete_RoundTrips()
    {
        var manager = Manager;
        Assert.Null(await manager.ReadFileAsync(FilePath, Task.FromResult));
        await manager.WriteFileAsync(FilePath, "before");
        Assert.Equal("before", await manager.ReadFileAsync(FilePath, Task.FromResult));
        Assert.False(await manager.UpdateFileAsync(FilePath, _ => Task.FromResult<string?>(null)));
        Assert.True(await manager.UpdateFileAsync(FilePath, c => Task.FromResult<string?>(c + " after")));
        Assert.Equal("before after", await manager.ReadFileAsync(FilePath, Task.FromResult));
        Assert.True(await manager.DeleteFile(FilePath));
        Assert.False(await manager.DeleteFile(FilePath));
    }

    [Fact]
    public async Task UnavailableRedis_FailsAcquisitionWithoutWritingFile()
    {
        // Reserve a real loopback endpoint without speaking Redis, rather than assuming
        // an arbitrary port is unused or interfering with the shared integration server.
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var options = new ConfigurationOptions
        {
            AbortOnConnectFail = false,
            ConnectRetry = 0,
            ConnectTimeout = 100,
            AsyncTimeout = 100,
            SyncTimeout = 100,
            BacklogPolicy = BacklogPolicy.FailFast
        };
        options.EndPoints.Add((IPEndPoint)listener.LocalEndpoint);
        using var redis = await ConnectionMultiplexer.ConnectAsync(options);
        var manager = new RedisLockManager(redis, Options.Create(_settings), NullLogger<RedisLockManager>.Instance);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        var failure = await Record.ExceptionAsync(() =>
            manager.WriteFileAsync(FilePath, "must not be published", cancellation.Token));

        Assert.True(failure is LockAcquisitionException or RedisException,
            $"Expected a lock or Redis failure, got {failure?.GetType().Name ?? "success"}.");
        Assert.False(File.Exists(FilePath));
    }

    [RedisFact]
    public async Task ActiveWriter_BlocksAllOperations_WithDedicatedSafeException()
    {
        var manager = Manager;
        await using var held = await RawLock.AcquireWriteLockAsync();
        Func<Task>[] operations = [
            () => manager.WriteFileAsync(FilePath, "competing"),
            () => manager.ReadFileAsync(FilePath, Task.FromResult),
            () => manager.UpdateFileAsync(FilePath, _ => Task.FromResult<string?>("competing")),
            () => manager.DeleteFile(FilePath)
        ];
        foreach (var operation in operations)
        {
            var error = await Assert.ThrowsAsync<LockAcquisitionException>(operation);
            Assert.DoesNotContain(_directory, error.Message);
        }
        Assert.False(File.Exists(FilePath));
    }

    [RedisFact]
    public async Task ActiveReader_AllowsReaders_BlocksWriter()
    {
        await Manager.WriteFileAsync(FilePath, "snapshot");
        await using var held = await RawLock.AcquireReadLockAsync();
        Assert.Equal("snapshot", await Manager.ReadFileAsync(FilePath, Task.FromResult));
        await Assert.ThrowsAsync<LockAcquisitionException>(() => Manager.WriteFileAsync(FilePath, "blocked"));
    }

    [RedisFact]
    public async Task ReadTransform_ContentAndTimestampRemainProtectedUntilCallbackCompletes()
    {
        var manager = Manager;
        await manager.WriteFileAsync(FilePath, "snapshot");
        var timestamp = File.GetLastWriteTimeUtc(FilePath);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var read = manager.ReadFileAsync(FilePath, async snapshot =>
        {
            entered.SetResult();
            await release.Task;
            return (Content: snapshot, Timestamp: File.GetLastWriteTimeUtc(FilePath));
        });
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.ThrowsAsync<LockAcquisitionException>(() => manager.WriteFileAsync(FilePath, "blocked"));
        }
        finally
        {
            release.TrySetResult();
            await read;
        }
        var result = await read;
        Assert.Equal("snapshot", result.Content);
        Assert.Equal(timestamp, result.Timestamp);
        await manager.WriteFileAsync(FilePath, "new version");
        Assert.Equal("new version", await File.ReadAllTextAsync(FilePath));
    }

    [RedisFact]
    public async Task CancelledWait_DoesNotLeakLock()
    {
        var manager = Manager;
        using var cancellation = new CancellationTokenSource();
        await using (var held = await RawLock.AcquireWriteLockAsync())
        {
            var waiting = manager.WriteFileAsync(FilePath, "cancelled", cancellation.Token);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        }
        await manager.WriteFileAsync(FilePath, "success");
        Assert.Equal("success", await File.ReadAllTextAsync(FilePath));
    }

    [RedisFact]
    public async Task FailedTransform_DoesNotLeakLockOrModifyFile()
    {
        var manager = Manager;
        await manager.WriteFileAsync(FilePath, "original");
        await Assert.ThrowsAsync<FormatException>(() => manager.UpdateFileAsync(FilePath, _ => throw new FormatException()));
        await Assert.ThrowsAsync<FormatException>(() => manager.ReadFileAsync<string>(FilePath, _ => throw new FormatException()));
        Assert.Equal("original", await File.ReadAllTextAsync(FilePath));
        await manager.WriteFileAsync(FilePath, "success");
    }

    [RedisFact]
    public async Task EmptyAndWhitespaceSnapshots_DoNotInvokeTransform()
    {
        foreach (var content in new[] { "", " \n " })
        {
            await Manager.WriteFileAsync(FilePath, content);
            Assert.Null(await Manager.ReadFileAsync<string>(FilePath, _ => throw new InvalidOperationException()));
        }
    }

    [RedisFact]
    public async Task ConfiguredExpiry_IsAppliedToRedisLease()
    {
        _settings.LockExpirySeconds = 2;
        await Manager.UpdateFileAsync(FilePath, async _ =>
        {
            var server = Redis.GetServer(Redis.GetEndPoints().Single());
            var keys = server.Keys(pattern: $"{_settings.LockKeyPrefix}:*").ToArray();
            Assert.NotEmpty(keys);
            foreach (var key in keys)
            {
                var remaining = await Redis.GetDatabase().KeyTimeToLiveAsync(key);
                Assert.NotNull(remaining);
                Assert.InRange(remaining.Value, TimeSpan.Zero, TimeSpan.FromSeconds(2));
            }
            return "updated";
        });
    }

    [RedisFact]
    public async Task CancelledUpdate_PreservesFileAndReleasesLock()
    {
        var manager = Manager;
        await manager.WriteFileAsync(FilePath, "original");
        using var cancellation = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => manager.UpdateFileAsync(FilePath, _ =>
        {
            cancellation.Cancel();
            return Task.FromResult<string?>("cancelled");
        }, cancellation.Token));
        Assert.Equal("original", await File.ReadAllTextAsync(FilePath));
        Assert.Single(Directory.GetFiles(_directory));
        await manager.WriteFileAsync(FilePath, "success");
    }

    [RedisFact]
    public async Task AtomicWrites_NeverExposePartialContent()
    {
        var manager = Manager;
        var versions = new[] { new string('a', 8192), new string('b', 8192) };
        await manager.WriteFileAsync(FilePath, versions[0]);
        using var stop = new CancellationTokenSource();
        var reader = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
                Assert.Contains(await File.ReadAllTextAsync(FilePath), versions);
        });
        try
        {
            for (var i = 0; i < 30; i++) await manager.WriteFileAsync(FilePath, versions[i % 2]);
        }
        finally
        {
            stop.Cancel();
            await reader;
        }
    }

    [RedisFact]
    public async Task Update_HoldsLockUntilTransformCompletes()
    {
        var manager = Manager;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var update = manager.UpdateFileAsync(FilePath, async _ =>
        {
            entered.SetResult();
            await release.Task;
            return "updated";
        });
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.ThrowsAsync<LockAcquisitionException>(() => manager.WriteFileAsync(FilePath, "competing"));
        }
        finally
        {
            release.TrySetResult();
            await update;
        }
        Assert.Equal("updated", await File.ReadAllTextAsync(FilePath));
    }

    public void Dispose()
    {
        _redis?.Dispose();
        if (Directory.Exists(_directory)) Directory.Delete(_directory, recursive: true);
    }
}
