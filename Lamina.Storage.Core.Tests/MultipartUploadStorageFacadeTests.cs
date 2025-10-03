using System.IO.Pipelines;
using System.Text;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core;
using Lamina.Storage.Core.Abstract;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Lamina.Storage.Core.Tests;

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
    public async Task UploadPartAsync_WithoutMetadata_ReturnsUploadPart()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
        var partNumber = 1;
        var expectedPart = new UploadPart { PartNumber = partNumber, ETag = "d41d8cd98f00b204e9800998ecf8427e" };

        var pipe = new Pipe();
        var data = "test data"u8.ToArray();
        await pipe.Writer.WriteAsync(data);
        await pipe.Writer.CompleteAsync();

        // Data-first approach: No metadata check required
        _mockDataStorage
            .Setup(x => x.StorePartDataAsync(bucketName, key, uploadId, partNumber, It.IsAny<PipeReader>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedPart);

        // Act
        var result = await _facade.UploadPartAsync(bucketName, key, uploadId, partNumber, pipe.Reader);

        // Assert
        Assert.Equal(expectedPart, result);
        // Verify metadata was NOT checked (data-first approach)
        _mockMetadataStorage.Verify(x => x.GetUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        _mockDataStorage.Verify(x => x.StorePartDataAsync(bucketName, key, uploadId, partNumber, It.IsAny<PipeReader>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_NoPartsData_ReturnsError()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var request = new CompleteMultipartUploadRequest
        {
            UploadId = "nonexistent-upload",
            Parts = new List<CompletedPart>()
        };

        // Data-first approach: Check for data existence, not metadata
        _mockDataStorage
            .Setup(x => x.GetStoredPartsAsync(bucketName, key, request.UploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<UploadPart>());

        // Act
        var result = await _facade.CompleteMultipartUploadAsync(bucketName, key, request);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Equal("NoSuchUpload", result.ErrorCode);
        Assert.Contains("not found", result.ErrorMessage!);
        // Verify metadata was NOT checked (data-first approach)
        _mockMetadataStorage.Verify(x => x.GetUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_PartNotFound_ReturnsError()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
        var request = new CompleteMultipartUploadRequest
        {
            UploadId = uploadId,
            Parts = new List<CompletedPart>
            {
                new() { PartNumber = 1, ETag = "d41d8cd98f00b204e9800998ecf8427e" },
                new() { PartNumber = 2, ETag = "098f6bcd4621d373cade4e832627b4f6" }
            }
        };

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
        var request = new CompleteMultipartUploadRequest
        {
            UploadId = uploadId,
            Parts = new List<CompletedPart>
            {
                new() { PartNumber = 1, ETag = "098f6bcd4621d373cade4e832627b4f6" }
            }
        };

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
    public async Task CompleteMultipartUploadAsync_WithoutMetadata_Success()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
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

        // Metadata is missing (returns null) - fall back to defaults
        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((MultipartUpload?)null);

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

        // Verify metadata was checked, and defaults were used when missing
        _mockMetadataStorage.Verify(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()), Times.Once);
        _mockObjectMetadataStorage.Verify(
            x => x.StoreMetadataAsync(
                bucketName,
                key,
                It.IsAny<string>(),
                It.IsAny<long>(),
                It.Is<PutObjectRequest>(req =>
                    req.ContentType == "application/octet-stream" &&
                    req.Metadata != null &&
                    req.Metadata.Count == 0),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_WithMetadata_UsesStoredMetadata()
    {
        // Arrange - S3 compliance: metadata from InitiateUpload should be preserved
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
        var upload = new MultipartUpload
        {
            UploadId = uploadId,
            BucketName = bucketName,
            Key = key,
            ContentType = "video/mp4",
            Metadata = new Dictionary<string, string>
            {
                { "author", "John Doe" },
                { "project", "Test Project" }
            }
        };
        var request = new CompleteMultipartUploadRequest
        {
            UploadId = uploadId,
            Parts = new List<CompletedPart>
            {
                new() { PartNumber = 1, ETag = "d41d8cd98f00b204e9800998ecf8427e" }
            }
        };

        var storedParts = new List<UploadPart>
        {
            new() { PartNumber = 1, ETag = "d41d8cd98f00b204e9800998ecf8427e" }
        };

        var partReaders = new List<PipeReader> { CreatePipeReader("part1 data") };

        // Metadata exists - should be used
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

        // Verify stored metadata was retrieved and used (S3 compliance)
        _mockMetadataStorage.Verify(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()), Times.Once);
        _mockObjectMetadataStorage.Verify(
            x => x.StoreMetadataAsync(
                bucketName,
                key,
                It.IsAny<string>(),
                It.IsAny<long>(),
                It.Is<PutObjectRequest>(req =>
                    req.ContentType == "video/mp4" &&
                    req.Metadata != null &&
                    req.Metadata.Count == 2 &&
                    req.Metadata["author"] == "John Doe" &&
                    req.Metadata["project"] == "Test Project"),
                It.IsAny<CancellationToken>()),
            Times.Once);
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
        var expectedPart = new UploadPart { PartNumber = partNumber, ETag = "d41d8cd98f00b204e9800998ecf8427e" };
        var mockValidator = new Mock<IChunkSignatureValidator>();

        var pipe = new Pipe();
        var data = "test data"u8.ToArray();
        await pipe.Writer.WriteAsync(data);
        await pipe.Writer.CompleteAsync();

        // Data-first approach: No metadata check required
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
        // Verify metadata was NOT checked (data-first approach)
        _mockMetadataStorage.Verify(x => x.GetUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task UploadPartAsync_WithChunkValidator_InvalidSignature_ReturnsNull()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
        var partNumber = 1;
        var mockValidator = new Mock<IChunkSignatureValidator>();

        var pipe = new Pipe();
        await pipe.Writer.CompleteAsync();

        // Data-first approach: No metadata check required
        _mockChunkedDataParser
            .Setup(x => x.ParseChunkedDataToStreamAsync(It.IsAny<PipeReader>(), It.IsAny<Stream>(), mockValidator.Object, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Invalid chunk signature"));

        // Act
        var result = await _facade.UploadPartAsync(bucketName, key, uploadId, partNumber, pipe.Reader, mockValidator.Object);

        // Assert
        Assert.Null(result);
        _mockDataStorage.Verify(x => x.StorePartDataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<PipeReader>(), It.IsAny<CancellationToken>()), Times.Never);
        // Verify metadata was NOT checked (data-first approach)
        _mockMetadataStorage.Verify(x => x.GetUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task UploadPartAsync_MetadataDeletedAfterInitiate_StillWorks()
    {
        // Arrange - Scenario: Metadata was deleted/corrupted but we still want to upload parts
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
        var partNumber = 1;
        var expectedPart = new UploadPart { PartNumber = partNumber, ETag = "d41d8cd98f00b204e9800998ecf8427e" };

        var pipe = new Pipe();
        var data = "test data"u8.ToArray();
        await pipe.Writer.WriteAsync(data);
        await pipe.Writer.CompleteAsync();

        // Metadata is missing (returns null)
        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((MultipartUpload?)null);

        // But data storage still works (data-first approach)
        _mockDataStorage
            .Setup(x => x.StorePartDataAsync(bucketName, key, uploadId, partNumber, It.IsAny<PipeReader>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedPart);

        // Act
        var result = await _facade.UploadPartAsync(bucketName, key, uploadId, partNumber, pipe.Reader);

        // Assert
        Assert.Equal(expectedPart, result);
        // Verify metadata was NOT checked (data-first approach allows uploads even without metadata)
        _mockMetadataStorage.Verify(x => x.GetUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        _mockDataStorage.Verify(x => x.StorePartDataAsync(bucketName, key, uploadId, partNumber, It.IsAny<PipeReader>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_MetadataDeletedAfterParts_StillWorks()
    {
        // Arrange - Scenario: Parts were uploaded, then metadata was deleted, but we can still complete
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "upload123";
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

        // Metadata is missing (returns null)
        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((MultipartUpload?)null);

        // But parts data exists (data-first approach)
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
            .ReturnsAsync(false); // Metadata already deleted

        // Act
        var result = await _facade.CompleteMultipartUploadAsync(bucketName, key, request);

        // Assert - Should succeed using defaults for ContentType and Metadata
        Assert.True(result.IsSuccess);
        Assert.NotNull(result.Value);
        Assert.Equal(bucketName, result.Value!.BucketName);
        Assert.Equal(key, result.Value.Key);

        // Verify metadata was checked but returned null, so defaults were used
        _mockMetadataStorage.Verify(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()), Times.Once);

        // Verify object was stored with default metadata
        _mockObjectMetadataStorage.Verify(
            x => x.StoreMetadataAsync(
                bucketName,
                key,
                It.IsAny<string>(),
                It.IsAny<long>(),
                It.Is<PutObjectRequest>(req =>
                    req.ContentType == "application/octet-stream" &&
                    req.Metadata != null &&
                    req.Metadata.Count == 0),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_OnlyPartDataExists_UsesDefaults()
    {
        // Arrange - Scenario: Only part data exists, no metadata was ever created
        var bucketName = "test-bucket";
        var key = "test-key";
        var uploadId = "orphaned-upload";
        var request = new CompleteMultipartUploadRequest
        {
            UploadId = uploadId,
            Parts = new List<CompletedPart>
            {
                new() { PartNumber = 1, ETag = "5d41402abc4b2a76b9719d911017c592" }
            }
        };

        var storedParts = new List<UploadPart>
        {
            new() { PartNumber = 1, ETag = "5d41402abc4b2a76b9719d911017c592", Size = 1024 }
        };

        var partReaders = new List<PipeReader> { CreatePipeReader("data") };

        // No metadata exists
        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((MultipartUpload?)null);

        // Only part data exists
        _mockDataStorage
            .Setup(x => x.GetStoredPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(storedParts);

        _mockDataStorage
            .Setup(x => x.GetPartReadersAsync(bucketName, key, uploadId, request.Parts, It.IsAny<CancellationToken>()))
            .ReturnsAsync(partReaders);

        _mockObjectDataStorage
            .Setup(x => x.StoreMultipartDataAsync(bucketName, key, It.IsAny<IEnumerable<PipeReader>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((1024L, "etag"));

        _mockObjectMetadataStorage
            .Setup(x => x.StoreMetadataAsync(bucketName, key, It.IsAny<string>(), It.IsAny<long>(), It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((S3Object?)null);

        _mockDataStorage
            .Setup(x => x.DeleteAllPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        _mockMetadataStorage
            .Setup(x => x.DeleteUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        // Act
        var result = await _facade.CompleteMultipartUploadAsync(bucketName, key, request);

        // Assert
        Assert.True(result.IsSuccess);

        // Verify defaults were used: application/octet-stream and empty metadata
        _mockObjectMetadataStorage.Verify(
            x => x.StoreMetadataAsync(
                bucketName,
                key,
                It.IsAny<string>(),
                1024L,
                It.Is<PutObjectRequest>(req =>
                    req.Key == key &&
                    req.ContentType == "application/octet-stream" &&
                    req.Metadata != null &&
                    req.Metadata.Count == 0),
                It.IsAny<CancellationToken>()),
            Times.Once);
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