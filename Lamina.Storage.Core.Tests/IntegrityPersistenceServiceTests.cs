using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using StackExchange.Redis;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Integrity;
using Lamina.WebApi.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Lamina.Storage.Core.Tests;

public class IntegrityPersistenceServiceTests
{
    private sealed class TransientDatabaseException : DbException
    {
        public override bool IsTransient => true;
    }

    [Theory]
    [InlineData("contention", true)]
    [InlineData("connection", true)]
    [InlineData("timeout", true)]
    [InlineData("cancellation", false)]
    public async Task AcquisitionRetriesTransientOutagesButNotCancellation(string failure, bool retry)
    {
        var settings = new IntegrityPersistenceSettings { Enabled = true, Workers = 1 };
        var queue = new IntegrityPersistenceQueue(settings);
        var data = new Mock<IObjectDataStorage>();
        data.Setup(x => x.GetDataInfoAsync("b", "k", It.IsAny<CancellationToken>())).ReturnsAsync((3L, DateTime.UnixEpoch));
        var metadata = new Mock<IObjectMetadataStorage>();
        var conditional = metadata.As<IConditionalObjectIntegrityStorage>();
        conditional.Setup(x => x.TryWriteIntegrityAsync(It.IsAny<ObjectIntegrityWrite>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(IntegrityWriteResult.Created);
        Exception error = failure switch
        {
            "contention" => new LockAcquisitionException("test"),
            "connection" => new RedisConnectionException(ConnectionFailureType.UnableToConnect, "unavailable"),
            "timeout" => new RedisTimeoutException("timeout", CommandStatus.Unknown),
            _ => new OperationCanceledException()
        };
        var locks = new Mock<IObjectPublicationLock>();
        locks.SetupSequence(x => x.AcquireAsync("b", "k", It.IsAny<CancellationToken>()))
            .ThrowsAsync(error).ReturnsAsync(Mock.Of<IAsyncDisposable>());
        await using var services = new ServiceCollection()
            .AddScoped(_ => data.Object).AddScoped(_ => metadata.Object).BuildServiceProvider();
        using var service = new IntegrityPersistenceService(queue, settings, services.GetRequiredService<IServiceScopeFactory>(),
            locks.Object, NullLogger<IntegrityPersistenceService>.Instance);
        await queue.EnqueueAsync(new("b", "k", 3, DateTime.UnixEpoch, null, "etag", new Dictionary<string, string>(), "text/plain"), default);
        await service.StartAsync(default);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await service.StopAsync(deadline.Token);
        locks.Verify(x => x.AcquireAsync("b", "k", It.IsAny<CancellationToken>()), Times.Exactly(retry ? 2 : 1));
        conditional.Verify(x => x.TryWriteIntegrityAsync(It.IsAny<ObjectIntegrityWrite>(), It.IsAny<CancellationToken>()), retry ? Times.Once() : Times.Never());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TransientFailureRetriesWithFreshScopesAndStopsAfterThreeAttempts(bool wrappedDatabaseFailure)
    {
        var settings = new IntegrityPersistenceSettings { Enabled = true, Workers = 1 };
        var queue = new IntegrityPersistenceQueue(settings);
        var data = new Mock<IObjectDataStorage>();
        data.Setup(x => x.GetDataInfoAsync("b", "k", It.IsAny<CancellationToken>())).ReturnsAsync((3L, DateTime.UnixEpoch));
        var metadata = new Mock<IObjectMetadataStorage>();
        var conditional = metadata.As<IConditionalObjectIntegrityStorage>();
        conditional.Setup(x => x.TryWriteIntegrityAsync(It.IsAny<ObjectIntegrityWrite>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(wrappedDatabaseFailure
                ? new DbUpdateException("save failed", new TransientDatabaseException())
                : new IOException("temporary failure"));
        var scopesCreated = 0;
        await using var services = new ServiceCollection()
            .AddScoped(_ => { Interlocked.Increment(ref scopesCreated); return data.Object; })
            .AddScoped(_ => metadata.Object).BuildServiceProvider();
        using var service = new IntegrityPersistenceService(queue, settings, services.GetRequiredService<IServiceScopeFactory>(),
            new InMemoryObjectPublicationLock(), NullLogger<IntegrityPersistenceService>.Instance);
        await queue.EnqueueAsync(new("b", "k", 3, DateTime.UnixEpoch, null, "etag", new Dictionary<string, string>(), "text/plain"), default);
        await service.StartAsync(default);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await service.StopAsync(deadline.Token);
        Assert.Equal(3, scopesCreated);
        conditional.Verify(x => x.TryWriteIntegrityAsync(It.IsAny<ObjectIntegrityWrite>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ShutdownDrainsAcceptedJobsAndRejectsChangedData(bool changed)
    {
        var settings = new IntegrityPersistenceSettings { Enabled = true, Workers = 2 };
        var queue = new IntegrityPersistenceQueue(settings);
        var data = new Mock<IObjectDataStorage>();
        data.Setup(x => x.GetDataInfoAsync("b", "k", It.IsAny<CancellationToken>()))
            .ReturnsAsync((changed ? 4L : 3L, DateTime.UnixEpoch));
        var metadata = new Mock<IObjectMetadataStorage>();
        var conditional = metadata.As<IConditionalObjectIntegrityStorage>();
        conditional.Setup(x => x.TryWriteIntegrityAsync(It.IsAny<ObjectIntegrityWrite>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(IntegrityWriteResult.Created);
        await using var services = new ServiceCollection()
            .AddScoped(_ => data.Object).AddScoped(_ => metadata.Object).BuildServiceProvider();
        using var service = new IntegrityPersistenceService(queue, settings, services.GetRequiredService<IServiceScopeFactory>(),
            new InMemoryObjectPublicationLock(), NullLogger<IntegrityPersistenceService>.Instance);
        for (var i = 0; i < 5; i++)
            await queue.EnqueueAsync(new("b", "k", 3, DateTime.UnixEpoch, null, "etag", new Dictionary<string, string>(), "text/plain"), default);
        await service.StartAsync(default);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await service.StopAsync(deadline.Token);
        conditional.Verify(x => x.TryWriteIntegrityAsync(It.IsAny<ObjectIntegrityWrite>(), It.IsAny<CancellationToken>()),
            changed ? Times.Never() : Times.Exactly(5));
    }
}
