using System.IO.Pipelines;
using System.Text;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core;
using Lamina.Storage.Core.Abstract;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Lamina.Tests.Storage;

public class MultipartUploadStorageFacadeTests
{
    private readonly Mock<IMultipartUploadDataStorage> _mockDataStorage;
    private readonly Mock<IMultipartUploadMetadataStorage> _mockMetadataStorage;
    private readonly Mock<IObjectDataStorage> _mockObjectDataStorage;
    private readonly Mock<IObjectMetadataStorage> _mockObjectMetadataStorage;
    private readonly Mock<IChunkedDataParser> _mockChunkedDataParser;
    private readonly MultipartUploadStorageFacade _facade;

    public MultipartUploadStorageFacadeTests()
    {
        _mockDataStorage = new Mock<IMultipartUploadDataStorage>();
        _mockMetadataStorage = new Mock<IMultipartUploadMetadataStorage>();
        _mockObjectDataStorage = new Mock<IObjectDataStorage>();
        _mockObjectMetadataStorage = new Mock<IObjectMetadataStorage>();
        _mockChunkedDataParser = new Mock<IChunkedDataParser>();

        _facade = new MultipartUploadStorageFacade(
            _mockDataStorage.Object,
            _mockMetadataStorage.Object,
            _mockObjectDataStorage.Object,
            _mockObjectMetadataStorage.Object,
            NullLogger<MultipartUploadStorageFacade>.Instance,
            _mockChunkedDataParser.Object
        );
    }

    [Fact]
    public async Task InitiateMultipartUploadAsync_CallsMetadataStorage()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var request = new InitiateMultipartUploadRequest { Key = key };
        var expectedUpload = new MultipartUpload { UploadId = "upload123", Key = key };

        _mockMetadataStorage
            .Setup(x => x.InitiateUploadAsync(bucketName, key, request, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedUpload);

        // Act
        var result = await _facade.InitiateMultipartUploadAsync(bucketName, key, request);

        // Assert
        Assert.Equal(expectedUpload, result);
        _mockMetadataStorage.Verify(x => x.InitiateUploadAsync(bucketName, key, request, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UploadPartAsync_ValidUpload_ReturnsUploadPart()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
        var partNumber = 1;
        var upload = new MultipartUpload { UploadId = uploadId, Key = key };
        var expectedPart = new UploadPart { PartNumber = partNumber, ETag = "d41d8cd98f00b204e9800998ecf8427e" };

        var pipe = new Pipe();
        var data = "test data"u8.ToArray();
        await pipe.Writer.WriteAsync(data);
        await pipe.Writer.CompleteAsync();

        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(upload);

        _mockDataStorage
            .Setup(x => x.StorePartDataAsync(bucketName, key, uploadId, partNumber, It.IsAny<PipeReader>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedPart);

        // Act
        var result = await _facade.UploadPartAsync(bucketName, key, uploadId, partNumber, pipe.Reader);

        // Assert
        Assert.Equal(expectedPart, result);
        _mockMetadataStorage.Verify(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()), Times.Once);
        _mockDataStorage.Verify(x => x.StorePartDataAsync(bucketName, key, uploadId, partNumber, It.IsAny<PipeReader>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UploadPartAsync_UploadNotFound_ThrowsInvalidOperationException()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "nonexistent-upload";
        var partNumber = 1;

        var pipe = new Pipe();
        await pipe.Writer.CompleteAsync();

        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((MultipartUpload?)null);

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _facade.UploadPartAsync(bucketName, key, uploadId, partNumber, pipe.Reader));

        Assert.Contains("not found", exception.Message);
        _mockDataStorage.Verify(x => x.StorePartDataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<PipeReader>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_UploadNotFound_ReturnsError()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var request = new CompleteMultipartUploadRequest
        {
            UploadId = "nonexistent-upload",
            Parts = new List<CompletedPart>()
        };

        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, request.UploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((MultipartUpload?)null);

        // Act
        var result = await _facade.CompleteMultipartUploadAsync(bucketName, key, request);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Equal("NoSuchUpload", result.ErrorCode);
        Assert.Contains("not found", result.ErrorMessage!);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_PartNotFound_ReturnsError()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
        var upload = new MultipartUpload { UploadId = uploadId, Key = key };
        var request = new CompleteMultipartUploadRequest
        {
            UploadId = uploadId,
            Parts = new List<CompletedPart>
            {
                new() { PartNumber = 1, ETag = "d41d8cd98f00b204e9800998ecf8427e" },
                new() { PartNumber = 2, ETag = "098f6bcd4621d373cade4e832627b4f6" }
            }
        };

        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(upload);

        // Only return part 1, not part 2
        _mockDataStorage
            .Setup(x => x.GetStoredPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<UploadPart>
            {
                new() { PartNumber = 1, ETag = "d41d8cd98f00b204e9800998ecf8427e" }
            });

        // Act
        var result = await _facade.CompleteMultipartUploadAsync(bucketName, key, request);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Equal("InvalidPart", result.ErrorCode);
        Assert.Contains("Part number 2 does not exist", result.ErrorMessage!);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_ETagMismatch_ReturnsError()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
        var upload = new MultipartUpload { UploadId = uploadId, Key = key };
        var request = new CompleteMultipartUploadRequest
        {
            UploadId = uploadId,
            Parts = new List<CompletedPart>
            {
                new() { PartNumber = 1, ETag = "098f6bcd4621d373cade4e832627b4f6" }
            }
        };

        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(upload);

        _mockDataStorage
            .Setup(x => x.GetStoredPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<UploadPart>
            {
                new() { PartNumber = 1, ETag = "d41d8cd98f00b204e9800998ecf8427e" }
            });

        // Act
        var result = await _facade.CompleteMultipartUploadAsync(bucketName, key, request);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Equal("InvalidPart", result.ErrorCode);
        Assert.Contains("ETag does not match", result.ErrorMessage!);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_ValidRequest_Success()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
        var upload = new MultipartUpload { UploadId = uploadId, Key = key, ContentType = "text/plain" };
        var request = new CompleteMultipartUploadRequest
        {
            UploadId = uploadId,
            Parts = new List<CompletedPart>
            {
                new() { PartNumber = 1, ETag = "d41d8cd98f00b204e9800998ecf8427e" },
                new() { PartNumber = 2, ETag = "098f6bcd4621d373cade4e832627b4f6" }
            }
        };

        var storedParts = new List<UploadPart>
        {
            new() { PartNumber = 1, ETag = "d41d8cd98f00b204e9800998ecf8427e" },
            new() { PartNumber = 2, ETag = "098f6bcd4621d373cade4e832627b4f6" }
        };

        var partReaders = new List<PipeReader>
        {
            CreatePipeReader("part1 data"),
            CreatePipeReader("part2 data")
        };

        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(upload);

        _mockDataStorage
            .Setup(x => x.GetStoredPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(storedParts);

        _mockDataStorage
            .Setup(x => x.GetPartReadersAsync(bucketName, key, uploadId, request.Parts, It.IsAny<CancellationToken>()))
            .ReturnsAsync(partReaders);

        _mockObjectDataStorage
            .Setup(x => x.StoreMultipartDataAsync(bucketName, key, It.IsAny<IEnumerable<PipeReader>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((100L, "final-etag"));

        _mockObjectMetadataStorage
            .Setup(x => x.StoreMetadataAsync(bucketName, key, It.IsAny<string>(), It.IsAny<long>(), It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((S3Object?)null);

        _mockDataStorage
            .Setup(x => x.DeleteAllPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        _mockMetadataStorage
            .Setup(x => x.DeleteUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        // Act
        var result = await _facade.CompleteMultipartUploadAsync(bucketName, key, request);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.NotNull(result.Value);
        Assert.Equal(bucketName, result.Value!.BucketName);
        Assert.Equal(key, result.Value.Key);

        // Verify cleanup was called
        _mockDataStorage.Verify(x => x.DeleteAllPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()), Times.Once);
        _mockMetadataStorage.Verify(x => x.DeleteUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task AbortMultipartUploadAsync_CallsCleanupMethods()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";

        _mockDataStorage
            .Setup(x => x.DeleteAllPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        _mockMetadataStorage
            .Setup(x => x.DeleteUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        // Act
        var result = await _facade.AbortMultipartUploadAsync(bucketName, key, uploadId);

        // Assert
        Assert.True(result);
        _mockDataStorage.Verify(x => x.DeleteAllPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()), Times.Once);
        _mockMetadataStorage.Verify(x => x.DeleteUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ListPartsAsync_CallsDataStorage()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
        var expectedParts = new List<UploadPart>
        {
            new() { PartNumber = 1, ETag = "d41d8cd98f00b204e9800998ecf8427e" },
            new() { PartNumber = 2, ETag = "098f6bcd4621d373cade4e832627b4f6" }
        };

        _mockDataStorage
            .Setup(x => x.GetStoredPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedParts);

        // Act
        var result = await _facade.ListPartsAsync(bucketName, key, uploadId);

        // Assert
        Assert.Equal(expectedParts, result);
        _mockDataStorage.Verify(x => x.GetStoredPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ListMultipartUploadsAsync_CallsMetadataStorage()
    {
        // Arrange
        var bucketName = "test-bucket";
        var expectedUploads = new List<MultipartUpload>
        {
            new() { UploadId = "upload1", Key = "key1" },
            new() { UploadId = "upload2", Key = "key2" }
        };

        _mockMetadataStorage
            .Setup(x => x.ListUploadsAsync(bucketName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedUploads);

        // Act
        var result = await _facade.ListMultipartUploadsAsync(bucketName);

        // Assert
        Assert.Equal(expectedUploads, result);
        _mockMetadataStorage.Verify(x => x.ListUploadsAsync(bucketName, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UploadPartAsync_WithChunkValidator_ValidSignature_Success()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
        var partNumber = 1;
        var upload = new MultipartUpload { UploadId = uploadId, Key = key };
        var expectedPart = new UploadPart { PartNumber = partNumber, ETag = "d41d8cd98f00b204e9800998ecf8427e" };
        var mockValidator = new Mock<IChunkSignatureValidator>();

        var pipe = new Pipe();
        var data = "test data"u8.ToArray();
        await pipe.Writer.WriteAsync(data);
        await pipe.Writer.CompleteAsync();

        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(upload);

        _mockChunkedDataParser
            .Setup(x => x.ParseChunkedDataToStreamAsync(It.IsAny<PipeReader>(), It.IsAny<Stream>(), mockValidator.Object, It.IsAny<CancellationToken>()))
            .ReturnsAsync(100L);

        _mockDataStorage
            .Setup(x => x.StorePartDataAsync(bucketName, key, uploadId, partNumber, It.IsAny<PipeReader>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedPart);

        // Act
        var result = await _facade.UploadPartAsync(bucketName, key, uploadId, partNumber, pipe.Reader, mockValidator.Object);

        // Assert
        Assert.Equal(expectedPart, result);
        _mockChunkedDataParser.Verify(x => x.ParseChunkedDataToStreamAsync(It.IsAny<PipeReader>(), It.IsAny<Stream>(), mockValidator.Object, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UploadPartAsync_WithChunkValidator_InvalidSignature_ReturnsNull()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
        var partNumber = 1;
        var upload = new MultipartUpload { UploadId = uploadId, Key = key };
        var mockValidator = new Mock<IChunkSignatureValidator>();

        var pipe = new Pipe();
        await pipe.Writer.CompleteAsync();

        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(upload);

        _mockChunkedDataParser
            .Setup(x => x.ParseChunkedDataToStreamAsync(It.IsAny<PipeReader>(), It.IsAny<Stream>(), mockValidator.Object, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Invalid chunk signature"));

        // Act
        var result = await _facade.UploadPartAsync(bucketName, key, uploadId, partNumber, pipe.Reader, mockValidator.Object);

        // Assert
        Assert.Null(result);
        _mockDataStorage.Verify(x => x.StorePartDataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<PipeReader>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private static PipeReader CreatePipeReader(string data)
    {
        var pipe = new Pipe();
        var bytes = Encoding.UTF8.GetBytes(data);
        pipe.Writer.WriteAsync(bytes).AsTask().Wait();
        pipe.Writer.Complete();
        return pipe.Reader;
    }
}