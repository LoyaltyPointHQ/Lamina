using System.Security.Cryptography;
using System.IO.Pipelines;
using Lamina.Storage.Core.Helpers;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Lamina.Storage.Core.Tests;

public class ObjectIntegrityRefreshTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StaleMetadata_IsRefreshedOnceWithoutLosingFields(bool multipart)
    {
        byte[] body = [1, 2, 3, 4];
        var timestamp = DateTime.UtcNow;
        var data = new Mock<IObjectDataStorage>();
        var metadata = new Mock<IObjectMetadataStorage>();
        var oldEtag = multipart ? "01234567890123456789012345678901-2" : "old";
        ObjectMetadataSnapshot snapshot = new(new S3ObjectInfo
        {
            ETag = oldEtag,
            ChecksumSHA256 = "old",
            ContentType = "application/custom",
            OwnerId = "owner",
            Tags = new() { ["tag"] = "value" },
            Metadata = new() { ["meta"] = "value" }
        }, timestamp.AddDays(-1));
        data.Setup(x => x.GetDataInfoAsync("b", "k", default)).ReturnsAsync((4L, timestamp));
        data.Setup(x => x.OpenReadAsync("b", "k", default)).ReturnsAsync(() => new MemoryStream(body));
        metadata.Setup(x => x.GetMetadataAsync("b", "k", default)).ReturnsAsync(() => snapshot.DetachedCopy());
        metadata.Setup(x => x.UpdateIntegrityAsync("b", "k", It.IsAny<string>(), 4, timestamp, It.IsAny<Dictionary<string, string>>(), default))
            .ReturnsAsync((string b, string k, string etag, long size, DateTime modified, Dictionary<string, string> sums, CancellationToken ct) =>
            {
                Assert.Single(sums);
                Assert.Equal(Convert.ToBase64String(SHA256.HashData(body)), sums["SHA256"]);
                var updated = ObjectMetadataSnapshot.CloneMetadata(snapshot.Metadata);
                updated.ETag = etag;
                updated.Size = size;
                updated.ChecksumSHA256 = sums["SHA256"];
                snapshot = new(updated, modified);
                return true;
            });
        var facade = CreateFacade(data.Object, metadata.Object);
        var first = await facade.GetObjectInfoAsync("b", "k");
        var second = await facade.GetObjectInfoAsync("b", "k");
        Assert.NotNull(first);
        Assert.Equal(multipart ? oldEtag : Convert.ToHexString(MD5.HashData(body)).ToLowerInvariant(), first.ETag);
        Assert.Equal(first.ChecksumSHA256, second!.ChecksumSHA256);
        Assert.Equal("application/custom", first.ContentType);
        Assert.Equal("owner", first.OwnerId);
        Assert.Equal("value", first.Tags["tag"]);
        Assert.Equal("value", first.Metadata["meta"]);
        Assert.Null(first.ChecksumCRC32);
        Assert.Equal(timestamp, first.LastModified);
        data.Verify(x => x.OpenReadAsync("b", "k", default), Times.Once);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedRefresh_DoesNotAdvertiseSuccess(bool changedDuringRead)
    {
        var timestamp = DateTime.UtcNow;
        var data = new Mock<IObjectDataStorage>();
        var metadata = new Mock<IObjectMetadataStorage>();
        data.SetupSequence(x => x.GetDataInfoAsync("b", "k", default))
            .ReturnsAsync((1L, timestamp)).ReturnsAsync((1L, changedDuringRead ? timestamp.AddSeconds(1) : timestamp));
        data.Setup(x => x.OpenReadAsync("b", "k", default)).ReturnsAsync(new MemoryStream([1]));
        metadata.Setup(x => x.GetMetadataAsync("b", "k", default))
            .ReturnsAsync(new ObjectMetadataSnapshot(new S3ObjectInfo { ETag = "old" }, null));
        await Assert.ThrowsAsync<IOException>(() => CreateFacade(data.Object, metadata.Object).GetObjectInfoAsync("b", "k"));
        metadata.Verify(x => x.UpdateIntegrityAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<long>(), It.IsAny<DateTime>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<CancellationToken>()),
            changedDuringRead ? Times.Never() : Times.Once());
    }

    [Fact]
    public async Task Copy_SourceChangedDuringPreparation_DoesNotPublishMismatchedChecksums()
    {
        var timestamp = DateTime.UtcNow;
        var data = new Mock<IObjectDataStorage>();
        var metadata = new Mock<IObjectMetadataStorage>();
        var cleaned = false;
        var prepared = new PreparedData { BucketName = "b", Key = "copy", Size = 4 };
        prepared.SetDisposeAction(() => cleaned = true);
        metadata.Setup(x => x.IsValidObjectKey(It.IsAny<string>())).Returns(true);
        metadata.Setup(x => x.GetMetadataAsync("b", "source", default)).ReturnsAsync(new ObjectMetadataSnapshot(
            new S3ObjectInfo { ETag = "etag", Size = 4, ContentType = "application/custom", ChecksumSHA256 = "source-checksum" }, timestamp));
        metadata.Setup(x => x.StoreMetadataAsync("b", "copy", It.IsAny<string>(), 4, It.IsAny<PutObjectRequest>(),
            It.IsAny<Dictionary<string, string>?>(), It.IsAny<DateTime?>(), default)).ReturnsAsync(new S3Object());
        data.SetupSequence(x => x.GetDataInfoAsync("b", "source", default))
            .ReturnsAsync((4L, timestamp)).ReturnsAsync((4L, timestamp.AddSeconds(1)));
        data.Setup(x => x.PrepareCopyDataAsync("b", "source", "b", "copy", default)).ReturnsAsync(prepared);
        data.Setup(x => x.OpenPreparedReadAsync(prepared, default)).ReturnsAsync(new MemoryStream([1, 2, 3, 4]));

        Assert.Null(await CreateFacade(data.Object, metadata.Object).CopyObjectAsync("b", "source", "b", "copy"));
        Assert.True(cleaned);
        data.Verify(x => x.CommitPreparedDataAsync(It.IsAny<PreparedData>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CopyPart_RejectedUpload_CancelsAndJoinsProducer()
    {
        var timestamp = DateTime.UtcNow;
        var data = new Mock<IObjectDataStorage>();
        var metadata = new Mock<IObjectMetadataStorage>();
        var multipart = new Mock<IMultipartUploadStorageFacade>();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stopped = false;
        using var cancellation = new CancellationTokenSource();
        metadata.Setup(x => x.IsValidObjectKey(It.IsAny<string>())).Returns(true);
        metadata.Setup(x => x.GetMetadataAsync("b", "source", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ObjectMetadataSnapshot(new S3ObjectInfo { ETag = "etag", Size = 4 }, timestamp));
        data.Setup(x => x.GetDataInfoAsync("b", "source", It.IsAny<CancellationToken>())).ReturnsAsync((4L, timestamp));
        data.Setup(x => x.WriteDataToPipeAsync("b", "source", It.IsAny<PipeWriter>(), null, null, It.IsAny<CancellationToken>()))
            .Returns(async (string b, string k, PipeWriter writer, long? start, long? end, CancellationToken ct) =>
            {
                started.SetResult();
                try { await Task.Delay(Timeout.Infinite, ct); }
                finally { stopped = true; }
                return true;
            });
        multipart.Setup(x => x.UploadPartAsync("b", "dest", "upload", 1, It.IsAny<PipeReader>(),
                It.IsAny<ChecksumRequest?>(), null, It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                await started.Task;
                return StorageResult<UploadPart>.Error("NoSuchUpload", "Upload is gone");
            });
        var facade = new ObjectStorageFacade(data.Object, metadata.Object, Mock.Of<IBucketStorageFacade>(),
            multipart.Object, NullLogger<ObjectStorageFacade>.Instance, Mock.Of<IContentTypeDetector>());
        var copy = facade.CopyObjectPartAsync("b", "source", "b", "dest", "upload", 1, cancellationToken: cancellation.Token);
        try
        {
            Assert.Null(await copy.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.True(stopped);
        }
        finally
        {
            cancellation.Cancel();
            try { await copy; } catch (OperationCanceledException) { }
        }
    }

    private static ObjectStorageFacade CreateFacade(IObjectDataStorage data, IObjectMetadataStorage metadata) => new(
        data, metadata, Mock.Of<IBucketStorageFacade>(), Mock.Of<IMultipartUploadStorageFacade>(),
        NullLogger<ObjectStorageFacade>.Instance, Mock.Of<IContentTypeDetector>());
}
