using System.IO.Pipelines;
using System.Text;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Microsoft.Extensions.Logging;
using Lamina.WebApi.Services;
using Moq;

namespace Lamina.Storage.Core.Tests;

/// <summary>
/// Tests verifying that ObjectStorageFacade uses data-first ordering when the metadata
/// backend requires the data file to exist before writing metadata (e.g. Xattr).
/// </summary>
public class XattrOrderingTests
{
    private readonly Mock<IObjectDataStorage> _dataStorageMock;
    private readonly Mock<IObjectMetadataStorage> _xattrMetadataMock;
    private readonly Mock<IBucketStorageFacade> _bucketStorageMock;
    private readonly Mock<IMultipartUploadStorageFacade> _multipartStorageMock;
    private readonly ObjectStorageFacade _facade;

    public XattrOrderingTests()
    {
        _dataStorageMock = new Mock<IObjectDataStorage>();
        _xattrMetadataMock = new Mock<IObjectMetadataStorage>();
        _xattrMetadataMock.As<IRequiresDataFileForMetadata>(); // Simulate Xattr backend
        _bucketStorageMock = new Mock<IBucketStorageFacade>();
        _multipartStorageMock = new Mock<IMultipartUploadStorageFacade>();

        _xattrMetadataMock.Setup(x => x.IsValidObjectKey(It.IsAny<string>())).Returns(true);

        _facade = new ObjectStorageFacade(
            _dataStorageMock.Object,
            _xattrMetadataMock.Object,
            _bucketStorageMock.Object,
            _multipartStorageMock.Object,
            Mock.Of<ILogger<ObjectStorageFacade>>(),
            new FileExtensionContentTypeDetector());
    }

    private PreparedData MakePreparedData(string bucket, string key) => new PreparedData
    {
        BucketName = bucket,
        Key = key,
        Size = 5,
        ETag = "abc123",
        Checksums = new Dictionary<string, string>()
    };

    [Fact]
    public async Task PutObjectAsync_WithXattrStorage_CommitsDataBeforeStoringMetadata()
    {
        // Arrange
        var bucket = "b";
        var key = "k.txt";
        var callOrder = new List<string>();

        var preparedData = MakePreparedData(bucket, key);
        _dataStorageMock
            .Setup(x => x.PrepareDataAsync(bucket, key, It.IsAny<PipeReader>(), null, It.IsAny<ChecksumRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(StorageResult<PreparedData>.Success(preparedData));

        _dataStorageMock
            .Setup(x => x.CommitPreparedDataAsync(preparedData, It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add("Commit"))
            .Returns(Task.CompletedTask);

        _xattrMetadataMock
            .Setup(x => x.StoreMetadataAsync(bucket, key, It.IsAny<string>(), It.IsAny<long>(), It.IsAny<PutObjectRequest>(), It.IsAny<Dictionary<string, string>?>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add("StoreMetadata"))
            .ReturnsAsync(new S3Object { Key = key, BucketName = bucket, ETag = "abc123", Size = 5, LastModified = DateTime.UtcNow, ContentType = "text/plain" });

        var request = new PutObjectRequest { Key = key, ContentType = "application/octet-stream", Metadata = new Dictionary<string, string> { ["x"] = "y" } };
        var pipe = PipeReader.Create(new MemoryStream(Encoding.UTF8.GetBytes("hello")));

        // Act
        var result = await _facade.PutObjectAsync(bucket, key, pipe, request);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(2, callOrder.Count);
        Assert.Equal("Commit", callOrder[0]);
        Assert.Equal("StoreMetadata", callOrder[1]);
    }

    [Fact]
    public async Task PutObjectAsync_WithXattrStorage_WhenMetadataFails_DeletesCommittedData()
    {
        // Arrange
        var bucket = "b";
        var key = "k.txt";

        var preparedData = MakePreparedData(bucket, key);
        _dataStorageMock
            .Setup(x => x.PrepareDataAsync(bucket, key, It.IsAny<PipeReader>(), null, It.IsAny<ChecksumRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(StorageResult<PreparedData>.Success(preparedData));

        _dataStorageMock
            .Setup(x => x.CommitPreparedDataAsync(preparedData, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        _dataStorageMock
            .Setup(x => x.DeleteDataAsync(bucket, key, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        _xattrMetadataMock
            .Setup(x => x.StoreMetadataAsync(bucket, key, It.IsAny<string>(), It.IsAny<long>(), It.IsAny<PutObjectRequest>(), It.IsAny<Dictionary<string, string>?>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((S3Object?)null);

        var request = new PutObjectRequest { Key = key, ContentType = "application/octet-stream", Metadata = new Dictionary<string, string> { ["x"] = "y" } };
        var pipe = PipeReader.Create(new MemoryStream(Encoding.UTF8.GetBytes("hello")));

        // Act
        var result = await _facade.PutObjectAsync(bucket, key, pipe, request);

        // Assert
        Assert.False(result.IsSuccess);
        _dataStorageMock.Verify(x => x.DeleteDataAsync(bucket, key, It.IsAny<CancellationToken>()), Times.Once);
        _xattrMetadataMock.Verify(x => x.DeleteMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CopyObjectAsync_WithXattrStorage_CommitsDataBeforeStoringMetadata()
    {
        // Arrange
        var srcBucket = "src";
        var srcKey = "src.txt";
        var dstBucket = "dst";
        var dstKey = "dst.txt";
        var callOrder = new List<string>();

        // Use custom content type so ShouldStoreMetadata returns true
        _xattrMetadataMock
            .Setup(x => x.GetMetadataAsync(srcBucket, srcKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3ObjectInfo { Key = srcKey, ETag = "e", Size = 5, LastModified = DateTime.UtcNow, ContentType = "application/custom" });

        var preparedData = MakePreparedData(dstBucket, dstKey);
        _dataStorageMock
            .Setup(x => x.GetDataInfoAsync(srcBucket, srcKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync((5L, DateTime.UtcNow));

        _dataStorageMock
            .Setup(x => x.PrepareCopyDataAsync(srcBucket, srcKey, dstBucket, dstKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(preparedData);

        _dataStorageMock
            .Setup(x => x.CommitPreparedDataAsync(preparedData, It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add("Commit"))
            .Returns(Task.CompletedTask);

        _xattrMetadataMock
            .Setup(x => x.StoreMetadataAsync(dstBucket, dstKey, It.IsAny<string>(), It.IsAny<long>(), It.IsAny<PutObjectRequest>(), It.IsAny<Dictionary<string, string>?>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add("StoreMetadata"))
            .ReturnsAsync(new S3Object { Key = dstKey, BucketName = dstBucket, ETag = "e", Size = 5, LastModified = DateTime.UtcNow, ContentType = "application/custom" });

        // Act
        var result = await _facade.CopyObjectAsync(srcBucket, srcKey, dstBucket, dstKey, "COPY");

        // Assert
        Assert.NotNull(result);
        Assert.Equal(2, callOrder.Count);
        Assert.Equal("Commit", callOrder[0]);
        Assert.Equal("StoreMetadata", callOrder[1]);
    }

    [Fact]
    public async Task CopyObjectAsync_WithXattrStorage_WhenMetadataFails_DeletesCommittedData()
    {
        // Arrange
        var srcBucket = "src";
        var srcKey = "src.txt";
        var dstBucket = "dst";
        var dstKey = "dst.txt";

        // Use custom content type so ShouldStoreMetadata returns true
        _xattrMetadataMock
            .Setup(x => x.GetMetadataAsync(srcBucket, srcKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3ObjectInfo { Key = srcKey, ETag = "e", Size = 5, LastModified = DateTime.UtcNow, ContentType = "application/custom" });

        _dataStorageMock
            .Setup(x => x.GetDataInfoAsync(srcBucket, srcKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync((5L, DateTime.UtcNow));

        var preparedData = MakePreparedData(dstBucket, dstKey);
        _dataStorageMock
            .Setup(x => x.PrepareCopyDataAsync(srcBucket, srcKey, dstBucket, dstKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(preparedData);

        _dataStorageMock
            .Setup(x => x.CommitPreparedDataAsync(preparedData, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        _dataStorageMock
            .Setup(x => x.DeleteDataAsync(dstBucket, dstKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        _xattrMetadataMock
            .Setup(x => x.StoreMetadataAsync(dstBucket, dstKey, It.IsAny<string>(), It.IsAny<long>(), It.IsAny<PutObjectRequest>(), It.IsAny<Dictionary<string, string>?>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((S3Object?)null);

        // Act
        var result = await _facade.CopyObjectAsync(srcBucket, srcKey, dstBucket, dstKey, "COPY");

        // Assert
        Assert.Null(result);
        _dataStorageMock.Verify(x => x.DeleteDataAsync(dstBucket, dstKey, It.IsAny<CancellationToken>()), Times.Once);
        _xattrMetadataMock.Verify(x => x.DeleteMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}
