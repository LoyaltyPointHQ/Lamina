using Lamina.Core.Models;
using Lamina.Storage.Core;
using Lamina.Storage.Core.Abstract;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Lamina.Storage.Core.Tests;

public class ObjectStorageFacadeBatchMetadataTests
{
    private readonly Mock<IObjectDataStorage> _mockDataStorage;
    private readonly Mock<IObjectMetadataStorage> _mockMetadataStorage;
    private readonly Mock<IBucketStorageFacade> _mockBucketStorage;
    private readonly Mock<IMultipartUploadStorageFacade> _mockMultipartStorage;
    private readonly Mock<IContentTypeDetector> _mockContentTypeDetector;

    public ObjectStorageFacadeBatchMetadataTests()
    {
        _mockDataStorage = new Mock<IObjectDataStorage>();
        _mockMetadataStorage = new Mock<IObjectMetadataStorage>();
        _mockBucketStorage = new Mock<IBucketStorageFacade>();
        _mockMultipartStorage = new Mock<IMultipartUploadStorageFacade>();
        _mockContentTypeDetector = new Mock<IContentTypeDetector>();

        _mockBucketStorage
            .Setup(x => x.BucketExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        _mockBucketStorage
            .Setup(x => x.GetBucketAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new Bucket { Name = "test", Type = BucketType.GeneralPurpose });

        _mockDataStorage
            .Setup(x => x.ListDataKeysAsync(It.IsAny<string>(), It.IsAny<BucketType>(),
                It.IsAny<string?>(), It.IsAny<string?>(), It.IsAny<string?>(), It.IsAny<int>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListDataResult { Keys = new List<string> { "a.txt", "b.txt", "c.txt" } });
    }

    private ObjectStorageFacade BuildFacade() => new(
        _mockDataStorage.Object,
        _mockMetadataStorage.Object,
        _mockBucketStorage.Object,
        _mockMultipartStorage.Object,
        NullLogger<ObjectStorageFacade>.Instance,
        _mockContentTypeDetector.Object);

    [Fact]
    public async Task ListObjects_WhenMetadataStorageImplementsBatch_UsesBatchPath()
    {
        var keys = new[] { "a.txt", "b.txt", "c.txt" };
        var batchResult = keys.ToDictionary(
            k => k,
            k => (S3ObjectInfo?)new S3ObjectInfo { Key = k, ETag = "etag", Size = 10 });

        _mockMetadataStorage
            .As<IBatchObjectMetadataStorage>()
            .Setup(x => x.GetMetadataBatchAsync("test-bucket", It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(batchResult);

        var facade = BuildFacade();
        var result = await facade.ListObjectsAsync("test-bucket");

        Assert.True(result.IsSuccess);
        Assert.Equal(3, result.Value!.Contents.Count);

        _mockMetadataStorage
            .As<IBatchObjectMetadataStorage>()
            .Verify(x => x.GetMetadataBatchAsync("test-bucket", It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()),
                Times.Once);

        _mockMetadataStorage
            .Verify(x => x.GetMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
    }

    [Fact]
    public async Task ListObjects_WhenMetadataStorageDoesNotImplementBatch_UsesPerKeyPath()
    {
        _mockMetadataStorage
            .Setup(x => x.GetMetadataAsync("test-bucket", It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string _, string key, CancellationToken _) =>
                new S3ObjectInfo { Key = key, ETag = "etag", Size = 10 });

        var facade = BuildFacade();
        var result = await facade.ListObjectsAsync("test-bucket");

        Assert.True(result.IsSuccess);
        Assert.Equal(3, result.Value!.Contents.Count);

        _mockMetadataStorage
            .Verify(x => x.GetMetadataAsync("test-bucket", It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Exactly(3));
    }
}
