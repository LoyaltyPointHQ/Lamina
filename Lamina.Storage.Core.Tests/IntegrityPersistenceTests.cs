using Lamina.Storage.Core.Integrity;
using Lamina.Storage.Core.Abstract;
using Lamina.WebApi.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Lamina.Storage.Core.Tests;

public class IntegrityPersistenceTests
{
    private static ObjectIntegrityWrite Job() => new("bucket", "key", 3, DateTime.UnixEpoch, null, "etag", new Dictionary<string, string>(), "text/plain");

    [Fact]
    public async Task SizeMismatchRefreshesEvenWhenModificationTimeIsUnchanged()
    {
        var data = new Mock<IObjectDataStorage>();
        data.Setup(x => x.GetDataInfoAsync("bucket", "key", default)).ReturnsAsync((3L, DateTime.UnixEpoch));
        data.Setup(x => x.OpenReadAsync("bucket", "key", default)).ReturnsAsync(new MemoryStream([1, 2, 3]));
        var metadata = new Mock<IObjectMetadataStorage>();
        metadata.As<IConditionalObjectIntegrityStorage>();
        metadata.Setup(x => x.GetMetadataAsync("bucket", "key", default)).ReturnsAsync(new ObjectMetadataSnapshot(
            new() { Key = "key", ETag = "old", Size = 1 }, DateTime.UnixEpoch));
        var queue = new IntegrityPersistenceQueue(new() { Enabled = true });
        var facade = new ObjectStorageFacade(data.Object, metadata.Object, Mock.Of<IBucketStorageFacade>(),
            Mock.Of<IMultipartUploadStorageFacade>(), NullLogger<ObjectStorageFacade>.Instance,
            new FileExtensionContentTypeDetector(), integrityQueue: queue);
        var result = await facade.GetObjectInfoAsync("bucket", "key");
        Assert.NotEqual("old", result!.ETag);
        data.Verify(x => x.OpenReadAsync("bucket", "key", default), Times.Once);
    }

    [Fact]
    public async Task ConcurrentMetadataWarmupDoesNotFailReadingUnchangedData()
    {
        var data = new Mock<IObjectDataStorage>();
        data.Setup(x => x.GetDataInfoAsync("bucket", "key", default)).ReturnsAsync((3L, DateTime.UnixEpoch));
        data.Setup(x => x.OpenReadAsync("bucket", "key", default)).ReturnsAsync(new MemoryStream([1, 2, 3]));
        var metadata = new Mock<IObjectMetadataStorage>();
        metadata.As<IConditionalObjectIntegrityStorage>();
        metadata.SetupSequence(x => x.GetMetadataAsync("bucket", "key", default))
            .ReturnsAsync((ObjectMetadataSnapshot?)null)
            .ReturnsAsync(new ObjectMetadataSnapshot(new() { Key = "key", ETag = "already-warmed" }, DateTime.UnixEpoch));
        var queue = new IntegrityPersistenceQueue(new() { Enabled = true });
        var facade = new ObjectStorageFacade(data.Object, metadata.Object, Mock.Of<IBucketStorageFacade>(),
            Mock.Of<IMultipartUploadStorageFacade>(), NullLogger<ObjectStorageFacade>.Instance,
            new FileExtensionContentTypeDetector(), integrityQueue: queue);
        Assert.NotNull(await facade.GetObjectInfoAsync("bucket", "key"));
    }

    [Fact]
    public async Task StaleMetadataQueuesExpectedIntegrityWithoutSynchronousUpdate()
    {
        var data = new Mock<IObjectDataStorage>();
        data.Setup(x => x.GetDataInfoAsync("bucket", "key", default)).ReturnsAsync((3L, DateTime.UnixEpoch));
        data.Setup(x => x.OpenReadAsync("bucket", "key", default)).ReturnsAsync(new MemoryStream([1, 2, 3]));
        var metadata = new Mock<IObjectMetadataStorage>();
        metadata.As<IConditionalObjectIntegrityStorage>();
        var snapshot = new ObjectMetadataSnapshot(new() { Key = "key", ETag = "old", Size = 1, ContentType = "text/plain" }, DateTime.UnixEpoch.AddSeconds(-1));
        metadata.Setup(x => x.GetMetadataAsync("bucket", "key", default)).ReturnsAsync(snapshot);
        var queue = new IntegrityPersistenceQueue(new() { Enabled = true });
        var facade = new ObjectStorageFacade(data.Object, metadata.Object, Mock.Of<IBucketStorageFacade>(),
            Mock.Of<IMultipartUploadStorageFacade>(), NullLogger<ObjectStorageFacade>.Instance,
            new FileExtensionContentTypeDetector(), integrityQueue: queue);
        var result = await facade.GetObjectInfoAsync("bucket", "key");
        Assert.NotNull(result);
        Assert.NotEqual("old", result.ETag);
        Assert.Equal("old", snapshot.Metadata.ETag);
        queue.Complete();
        await foreach (var job in queue.ReadAllAsync(default))
        {
            Assert.Equal(ObjectIntegrityState.FromSnapshot(snapshot), job.Expected);
            Assert.Equal(result.ETag, job.ETag);
        }
        metadata.Verify(x => x.UpdateIntegrityAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(),
            It.IsAny<long>(), It.IsAny<DateTime>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task HashStreamIsAcquiredUnderPublicationLockButQueueAdmissionIsNot()
    {
        var locks = new TrackingLock();
        var data = new Mock<IObjectDataStorage>();
        data.Setup(x => x.GetDataInfoAsync("bucket", "key", default)).ReturnsAsync((3L, DateTime.UnixEpoch));
        data.Setup(x => x.OpenReadAsync("bucket", "key", default)).ReturnsAsync(() =>
        {
            Assert.True(locks.Held);
            return new MemoryStream([1, 2, 3]);
        });
        var metadata = new Mock<IObjectMetadataStorage>();
        metadata.As<IConditionalObjectIntegrityStorage>();
        var queue = new IntegrityPersistenceQueue(new() { Enabled = true, Capacity = 1 });
        await queue.EnqueueAsync(Job(), default);
        var facade = new ObjectStorageFacade(data.Object, metadata.Object, Mock.Of<IBucketStorageFacade>(),
            Mock.Of<IMultipartUploadStorageFacade>(), NullLogger<ObjectStorageFacade>.Instance,
            new FileExtensionContentTypeDetector(), publicationLock: locks, integrityQueue: queue);
        var reading = facade.GetObjectInfoAsync("bucket", "key");
        Assert.False(reading.IsCompleted);
        Assert.False(locks.Held);
        await using var enumerator = queue.ReadAllAsync(default).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        Assert.NotNull(await reading.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    private sealed class TrackingLock : IObjectPublicationLock
    {
        public bool Held;
        public ValueTask<IAsyncDisposable> AcquireAsync(string bucketName, string key, CancellationToken cancellationToken = default)
        {
            Assert.False(Held);
            Held = true;
            return ValueTask.FromResult<IAsyncDisposable>(new Lease(this));
        }
        private sealed class Lease(TrackingLock owner) : IAsyncDisposable
        {
            public ValueTask DisposeAsync() { owner.Held = false; return ValueTask.CompletedTask; }
        }
    }

    [Fact]
    public async Task MissingMetadataIsQueuedWithoutWaitingForPersistence()
    {
        var data = new Mock<IObjectDataStorage>();
        data.Setup(x => x.GetDataInfoAsync("bucket", "key", default)).ReturnsAsync((3L, DateTime.UnixEpoch));
        data.Setup(x => x.OpenReadAsync("bucket", "key", default)).ReturnsAsync(new MemoryStream([1, 2, 3]));
        var metadata = new Mock<IObjectMetadataStorage>();
        metadata.As<IConditionalObjectIntegrityStorage>();
        var queue = new IntegrityPersistenceQueue(new() { Enabled = true });
        var facade = new ObjectStorageFacade(data.Object, metadata.Object, Mock.Of<IBucketStorageFacade>(),
            Mock.Of<IMultipartUploadStorageFacade>(), NullLogger<ObjectStorageFacade>.Instance,
            new FileExtensionContentTypeDetector(), integrityQueue: queue);
        var result = await facade.GetObjectInfoAsync("bucket", "key");
        Assert.NotNull(result);
        queue.Complete();
        var writes = new List<ObjectIntegrityWrite>();
        await foreach (var job in queue.ReadAllAsync(default)) writes.Add(job);
        var write = Assert.Single(writes);
        Assert.Null(write.Expected);
        Assert.Equal(result.ETag, write.ETag);
        Assert.Equal(3, write.Size);
        metadata.As<IConditionalObjectIntegrityStorage>().Verify(x => x.TryWriteIntegrityAsync(It.IsAny<ObjectIntegrityWrite>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task FullQueueWaitsAndHonorsRequestCancellation()
    {
        var queue = new IntegrityPersistenceQueue(new() { Enabled = true, Capacity = 1 });
        await queue.EnqueueAsync(Job(), default);
        using var cancellation = new CancellationTokenSource();
        var blocked = queue.EnqueueAsync(Job(), cancellation.Token).AsTask();
        Assert.False(blocked.IsCompleted);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blocked);
        queue.Complete();
        var count = 0;
        await foreach (var job in queue.ReadAllAsync(default)) count++;
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task QueueDetachesChecksumDictionary()
    {
        var queue = new IntegrityPersistenceQueue(new() { Enabled = true });
        var sums = new Dictionary<string, string> { ["SHA1"] = "old" };
        await queue.EnqueueAsync(Job() with { Checksums = sums }, default);
        sums["SHA1"] = "new";
        queue.Complete();
        await foreach (var job in queue.ReadAllAsync(default)) Assert.Equal("old", job.Checksums["SHA1"]);
    }

    [Fact]
    public async Task PublicationLockSerializesSameObjectButNotOtherObjects()
    {
        var locks = new InMemoryObjectPublicationLock();
        var first = await locks.AcquireAsync("b", "k", default);
        var waiting = locks.AcquireAsync("b", "k", default).AsTask();
        Assert.False(waiting.IsCompleted);
        await using var other = await locks.AcquireAsync("b", "other", default);
        await first.DisposeAsync();
        await using var second = await waiting.WaitAsync(TimeSpan.FromSeconds(5));
    }
}
