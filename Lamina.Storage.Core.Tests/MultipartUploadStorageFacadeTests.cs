using System.IO.Pipelines;
using System.Text;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
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
        _mockObjectMetadataStorage
            .Setup(x => x.StoreMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<long>(),
                It.IsAny<PutObjectRequest?>(), It.IsAny<Dictionary<string, string>?>(), It.IsAny<DateTime?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string bucket, string key, string etag, long size, PutObjectRequest? _, Dictionary<string, string>? _, DateTime? modified, CancellationToken _) =>
                new S3Object { BucketName = bucket, Key = key, ETag = etag, Size = size, LastModified = modified ?? DateTime.UtcNow });
        _mockChunkedDataParser = new Mock<IChunkedDataParser>();
        _mockDataStorage.Setup(x => x.BeginPartWriteAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string bucket, string key, string upload, int number, CancellationToken ct) =>
                new StagedDataWrite(new MemoryStream(), size => new PreparedData { BucketName = bucket, Key = key, Size = size }, () => { }));


        // Default: assume upload has parts. Individual tests exercising the "no upload" path
        // override this with .Setup(...).ReturnsAsync(false).
        _mockDataStorage
            .Setup(x => x.HasAnyPartsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

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

        var pipe = new Pipe();
        var data = "test data"u8.ToArray();
        await pipe.Writer.WriteAsync(data);
        await pipe.Writer.CompleteAsync();

        // Data-first approach: No metadata check required

        // Act
        var result = await _facade.UploadPartAsync(bucketName, key, uploadId, partNumber, pipe.Reader);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(partNumber, result.Value!.PartNumber);
        Assert.Equal(ETagHelper.ComputeETag("test data"u8.ToArray()), result.Value.ETag);
        // Data-first: upload succeeds regardless of metadata store state. The facade may read
        // metadata to persist the computed ETag (best-effort), but never writes if upload doesn't exist.
        _mockMetadataStorage.Verify(x => x.UpdateUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<MultipartUpload>(), It.IsAny<CancellationToken>()), Times.Never);
        _mockDataStorage.Verify(x => x.BeginPartWriteAsync(bucketName, key, uploadId, partNumber, It.IsAny<CancellationToken>()), Times.Once);
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

        // Data-first: the cheap HasAnyPartsAsync check short-circuits before any metadata load
        _mockDataStorage
            .Setup(x => x.HasAnyPartsAsync(bucketName, key, request.UploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        // Act
        var result = await _facade.CompleteMultipartUploadAsync(bucketName, key, request);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Equal("NoSuchUpload", result.ErrorCode);
        Assert.Contains("not found", result.ErrorMessage!);
        // Verify metadata was NOT checked (data-first approach)
        _mockMetadataStorage.Verify(x => x.GetUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        // And GetStoredPartsAsync was also not called - the existence check alone sufficed
        _mockDataStorage.Verify(x => x.GetStoredPartsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
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
            new() { PartNumber = 1, ETag = "d41d8cd98f00b204e9800998ecf8427e", Size = 5 * 1024 * 1024 },
            new() { PartNumber = 2, ETag = "098f6bcd4621d373cade4e832627b4f6", Size = 100 }
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
            .Setup(x => x.PrepareMultipartDataAsync(bucketName, key, It.IsAny<IEnumerable<PipeReader>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PreparedData { BucketName = bucketName, Key = key, Size = 100L });

        _mockObjectDataStorage
            .Setup(x => x.CommitPreparedDataAsync(It.IsAny<PreparedData>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        _mockObjectMetadataStorage
            .Setup(x => x.StoreMetadataAsync(bucketName, key, It.IsAny<string>(), It.IsAny<long>(), It.IsAny<PutObjectRequest>(), It.IsAny<Dictionary<string, string>?>(), It.IsAny<DateTime?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3Object { BucketName = bucketName, Key = key });

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
                It.IsAny<Dictionary<string, string>?>(),
                It.IsAny<DateTime?>(),
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
            .Setup(x => x.PrepareMultipartDataAsync(bucketName, key, It.IsAny<IEnumerable<PipeReader>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PreparedData { BucketName = bucketName, Key = key, Size = 100L });

        _mockObjectDataStorage
            .Setup(x => x.CommitPreparedDataAsync(It.IsAny<PreparedData>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        _mockObjectMetadataStorage
            .Setup(x => x.StoreMetadataAsync(bucketName, key, It.IsAny<string>(), It.IsAny<long>(), It.IsAny<PutObjectRequest>(), It.IsAny<Dictionary<string, string>?>(), It.IsAny<DateTime?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3Object { BucketName = bucketName, Key = key });

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
                It.IsAny<Dictionary<string, string>?>(),
                It.IsAny<DateTime?>(),
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
        Assert.True(result.IsSuccess);
        Assert.Equal(expectedParts, result.Value);
        _mockDataStorage.Verify(x => x.GetStoredPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ListPartsAsync_RequiresMetadataOrStoredParts(bool hasMetadata, bool hasParts)
    {
        const string bucket = "test-bucket";
        const string key = "test-key";
        var uploadId = Guid.NewGuid().ToString("N");
        var upload = hasMetadata ? new MultipartUpload { BucketName = bucket, Key = key, UploadId = uploadId } : null;
        var parts = new List<UploadPart>();
        if (hasParts)
            parts.Add(new UploadPart { PartNumber = 1, ETag = "stored-etag" });
        using var cts = new CancellationTokenSource();
        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucket, key, uploadId, cts.Token))
            .ReturnsAsync(upload);
        _mockDataStorage
            .Setup(x => x.GetStoredPartsAsync(bucket, key, uploadId, cts.Token))
            .ReturnsAsync(parts);

        var result = await _facade.ListPartsAsync(bucket, key, uploadId, cts.Token);

        Assert.Equal(hasMetadata || hasParts, result.IsSuccess);
        if (result.IsSuccess)
        {
            Assert.Same(parts, result.Value);
            Assert.Null(result.ErrorCode);
        }
        else
        {
            Assert.Equal("NoSuchUpload", result.ErrorCode);
            Assert.Null(result.Value);
        }
        _mockDataStorage.Verify(x => x.GetStoredPartsAsync(
            bucket, key, uploadId, cts.Token), Times.Once);
    }

    [Fact]
    public async Task ListPartsAsync_UsesStoredMetadataWithoutReadingBytesAndMergesChecksums()
    {
        const string bucket = "test-bucket";
        const string key = "test-key";
        var uploadId = Guid.NewGuid().ToString("N");
        var metadata = new PartMetadata
        {
            ETag = "stored-etag",
            ChecksumCRC32 = "crc32",
            ChecksumCRC32C = "crc32c",
            ChecksumCRC64NVME = "crc64",
            ChecksumSHA1 = "sha1",
            ChecksumSHA256 = "sha256"
        };
        var upload = new MultipartUpload { BucketName = bucket, Key = key, UploadId = uploadId };
        upload.Parts[1] = metadata;
        var part = new UploadPart { PartNumber = 1, ETag = string.Empty };
        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucket, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(upload);
        _mockDataStorage
            .Setup(x => x.GetStoredPartsAsync(bucket, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<UploadPart> { part });

        var result = await _facade.ListPartsAsync(bucket, key, uploadId);

        Assert.True(result.IsSuccess);
        var actual = Assert.Single(result.Value!);
        Assert.Equal(metadata.ETag, actual.ETag);
        Assert.Equal(metadata.ChecksumCRC32, actual.ChecksumCRC32);
        Assert.Equal(metadata.ChecksumCRC32C, actual.ChecksumCRC32C);
        Assert.Equal(metadata.ChecksumCRC64NVME, actual.ChecksumCRC64NVME);
        Assert.Equal(metadata.ChecksumSHA1, actual.ChecksumSHA1);
        Assert.Equal(metadata.ChecksumSHA256, actual.ChecksumSHA256);
        _mockDataStorage.Verify(x => x.GetPartReadersAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<List<CompletedPart>>(), It.IsAny<CancellationToken>()), Times.Never);
        _mockDataStorage.Verify(x => x.GetStoredPartsAsync(
            bucket, key, uploadId, It.IsAny<CancellationToken>()), Times.Once);
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
        var mockValidator = new Mock<IChunkSignatureValidator>();

        var pipe = new Pipe();
        var data = "test data"u8.ToArray();
        await pipe.Writer.WriteAsync(data);
        await pipe.Writer.CompleteAsync();

        // Data-first approach: No metadata check required
        _mockChunkedDataParser
            .Setup(x => x.ParseChunkedDataToStreamAsync(It.IsAny<PipeReader>(), It.IsAny<Stream>(), mockValidator.Object, It.IsAny<Action<ReadOnlySpan<byte>>>(), It.IsAny<CancellationToken>()))
            .Returns(async (PipeReader reader, Stream destination, IChunkSignatureValidator validator, Action<ReadOnlySpan<byte>> onData, CancellationToken ct) =>
            {
                await destination.WriteAsync(data, ct);
                onData(data);
                return new ChunkedDataResult { Success = true, TotalBytesWritten = data.Length };
            });


        // Act
        var result = await _facade.UploadPartAsync(bucketName, key, uploadId, partNumber, pipe.Reader, mockValidator.Object);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(partNumber, result.Value!.PartNumber);
        Assert.Equal(ETagHelper.ComputeETag("test data"u8.ToArray()), result.Value.ETag);
        // The facade invokes the parser; the storage receives only a raw staging request.
        _mockDataStorage.Verify(x => x.BeginPartWriteAsync(bucketName, key, uploadId, partNumber, It.IsAny<CancellationToken>()), Times.Once);
        // Data-first: no metadata write when upload doesn't exist
        _mockMetadataStorage.Verify(x => x.UpdateUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<MultipartUpload>(), It.IsAny<CancellationToken>()), Times.Never);
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

        // Validation belongs to the facade processor, before publication.

        _mockChunkedDataParser.Setup(x => x.ParseChunkedDataToStreamAsync(It.IsAny<PipeReader>(), It.IsAny<Stream>(), mockValidator.Object, It.IsAny<Action<ReadOnlySpan<byte>>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ChunkedDataResult { Success = false });
        // Act
        var result = await _facade.UploadPartAsync(bucketName, key, uploadId, partNumber, pipe.Reader, mockValidator.Object);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Equal("SignatureDoesNotMatch", result.ErrorCode);
        // A private staging session is allocated but never published.
        _mockDataStorage.Verify(x => x.BeginPartWriteAsync(bucketName, key, uploadId, partNumber, It.IsAny<CancellationToken>()), Times.Once);
        _mockDataStorage.Verify(x => x.CommitPreparedPartAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<PreparedData>(), It.IsAny<CancellationToken>()), Times.Never);
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

        var pipe = new Pipe();
        var data = "test data"u8.ToArray();
        await pipe.Writer.WriteAsync(data);
        await pipe.Writer.CompleteAsync();

        // Metadata is missing (returns null)
        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((MultipartUpload?)null);

        // But data storage still works (data-first approach)

        // Act
        var result = await _facade.UploadPartAsync(bucketName, key, uploadId, partNumber, pipe.Reader);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(partNumber, result.Value!.PartNumber);
        Assert.Equal(ETagHelper.ComputeETag("test data"u8.ToArray()), result.Value.ETag);
        // Data-first: even though metadata is gone (GetUploadMetadataAsync returned null), the part upload
        // still succeeds because data is the source of truth. No metadata write happens because there's
        // nothing to update.
        _mockMetadataStorage.Verify(x => x.UpdateUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<MultipartUpload>(), It.IsAny<CancellationToken>()), Times.Never);
        _mockDataStorage.Verify(x => x.BeginPartWriteAsync(bucketName, key, uploadId, partNumber, It.IsAny<CancellationToken>()), Times.Once);
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
            new() { PartNumber = 1, ETag = "d41d8cd98f00b204e9800998ecf8427e", Size = 5 * 1024 * 1024 },
            new() { PartNumber = 2, ETag = "098f6bcd4621d373cade4e832627b4f6", Size = 100 }
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
            .Setup(x => x.PrepareMultipartDataAsync(bucketName, key, It.IsAny<IEnumerable<PipeReader>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PreparedData { BucketName = bucketName, Key = key, Size = 100L });

        _mockObjectDataStorage
            .Setup(x => x.CommitPreparedDataAsync(It.IsAny<PreparedData>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        _mockObjectMetadataStorage
            .Setup(x => x.StoreMetadataAsync(bucketName, key, It.IsAny<string>(), It.IsAny<long>(), It.IsAny<PutObjectRequest>(), It.IsAny<Dictionary<string, string>?>(), It.IsAny<DateTime?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3Object { BucketName = bucketName, Key = key });

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
                It.IsAny<Dictionary<string, string>?>(),
                It.IsAny<DateTime?>(),
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
            .Setup(x => x.PrepareMultipartDataAsync(bucketName, key, It.IsAny<IEnumerable<PipeReader>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PreparedData { BucketName = bucketName, Key = key, Size = 1024L });

        _mockObjectDataStorage
            .Setup(x => x.CommitPreparedDataAsync(It.IsAny<PreparedData>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        _mockObjectMetadataStorage
            .Setup(x => x.StoreMetadataAsync(bucketName, key, It.IsAny<string>(), It.IsAny<long>(), It.IsAny<PutObjectRequest>(), It.IsAny<Dictionary<string, string>?>(), It.IsAny<DateTime?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3Object { BucketName = bucketName, Key = key });

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
                It.IsAny<Dictionary<string, string>?>(),
                It.IsAny<DateTime?>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_StoresMetadataBeforeCommittingData_WithExplicitLastModified()
    {
        // Metadata must land before data becomes visible so a GET during the tiny window
        // between the two calls returns 404 (data-first architecture: no data → not found)
        // instead of a 200 with a full-file MD5 ETag auto-generated from the already-committed
        // bytes. The metadata write carries an explicit non-MinValue LastModified so the
        // staleness detector doesn't immediately trigger a recompute and overwrite the
        // multipart ETag with MD5-of-full-file.
        const string bucketName = "test-bucket";
        const string key = "test-key";
        const string uploadId = "upload-order";
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
        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((MultipartUpload?)null);
        _mockDataStorage
            .Setup(x => x.GetStoredPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(storedParts);
        _mockDataStorage
            .Setup(x => x.GetPartReadersAsync(bucketName, key, uploadId, request.Parts, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<PipeReader> { CreatePipeReader("p1") });
        _mockObjectDataStorage
            .Setup(x => x.PrepareMultipartDataAsync(bucketName, key, It.IsAny<IEnumerable<PipeReader>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PreparedData { BucketName = bucketName, Key = key, Size = 1 });
        _mockDataStorage
            .Setup(x => x.DeleteAllPartsAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        _mockMetadataStorage
            .Setup(x => x.DeleteUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var callOrder = new List<string>();
        DateTime? capturedLastModified = null;
        _mockObjectDataStorage
            .Setup(x => x.CommitPreparedDataAsync(It.IsAny<PreparedData>(), It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add(nameof(IObjectDataStorage.CommitPreparedDataAsync)))
            .Returns(Task.CompletedTask);
        _mockObjectMetadataStorage
            .Setup(x => x.StoreMetadataAsync(bucketName, key, It.IsAny<string>(), It.IsAny<long>(), It.IsAny<PutObjectRequest>(), It.IsAny<Dictionary<string, string>?>(), It.IsAny<DateTime?>(), It.IsAny<CancellationToken>()))
            .Callback((string _, string _, string _, long _, PutObjectRequest? _, Dictionary<string, string>? _, DateTime? lm, CancellationToken _) =>
            {
                callOrder.Add(nameof(IObjectMetadataStorage.StoreMetadataAsync));
                capturedLastModified = lm;
            })
            .ReturnsAsync(new S3Object { BucketName = bucketName, Key = key });

        var before = DateTime.UtcNow.AddSeconds(-5);
        var result = await _facade.CompleteMultipartUploadAsync(bucketName, key, request);
        var after = DateTime.UtcNow.AddSeconds(5);

        Assert.True(result.IsSuccess);
        Assert.Equal(
            new[] { nameof(IObjectMetadataStorage.StoreMetadataAsync), nameof(IObjectDataStorage.CommitPreparedDataAsync) },
            callOrder);
        Assert.NotNull(capturedLastModified);
        Assert.NotEqual(DateTime.MinValue, capturedLastModified!.Value);
        Assert.InRange(capturedLastModified.Value, before, after);
    }

    [Fact]
    public async Task UploadPartAsync_ConcurrentCallsForSameUpload_PersistsAllPartMetadata()
    {
        // Simulates aws s3 cp firing parallel UploadPartAsync for the same upload. Without per-
        // upload serialisation, Facade.PersistPartMetadataAsync races concurrently on a shared
        // non-thread-safe Dictionary<int, PartMetadata>, causing NREs on resize or silently
        // dropping entries (last-writer-wins on transient bucket array).
        const string bucketName = "test-bucket";
        const string key = "test-key";
        const string uploadId = "upload-concurrent";
        const int partCount = 50;

        var sharedUpload = new MultipartUpload
        {
            UploadId = uploadId,
            BucketName = bucketName,
            Key = key
        };

        // Every concurrent call sees the same MultipartUpload instance (mimics the real filesystem
        // backend, which returns the cached reference from IMemoryCache).
        _mockMetadataStorage
            .Setup(x => x.GetUploadMetadataAsync(bucketName, key, uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(sharedUpload);


        await Parallel.ForEachAsync(
            Enumerable.Range(1, partCount),
            new ParallelOptions { MaxDegreeOfParallelism = 16 },
            async (partNumber, ct) =>
            {
                var pipe = new Pipe();
                await pipe.Writer.CompleteAsync();
                var result = await _facade.UploadPartAsync(bucketName, key, uploadId, partNumber, pipe.Reader, cancellationToken: ct);
                Assert.True(result.IsSuccess);
            });

        Assert.Equal(partCount, sharedUpload.Parts.Count);
        for (int i = 1; i <= partCount; i++)
        {
            Assert.True(sharedUpload.Parts.ContainsKey(i), $"Missing part {i} after concurrent uploads");
            Assert.Equal(ETagHelper.ComputeETag(Array.Empty<byte>()), sharedUpload.Parts[i].ETag);
        }
    }

    [Theory]
    [InlineData(0, true)]
    [InlineData(0, false)]
    [InlineData(1, true)]
    [InlineData(1, false)]
    [InlineData(2, true)]
    [InlineData(2, false)]
    [InlineData(3, true)]
    [InlineData(3, false)]
    public async Task UploadPartAsync_AllOverloadsValidateContentMd5BeforeCommit(int overload, bool valid)
    {
        var expected = valid ? System.Security.Cryptography.MD5.HashData("payload"u8.ToArray()) : new byte[16];
        var reader = CreatePipeReader("payload");
        var result = overload switch
        {
            0 => await _facade.UploadPartAsync("bucket", "key", "upload", 1, reader, expectedMd5: expected),
            1 => await _facade.UploadPartAsync("bucket", "key", "upload", 1, reader, (IChunkSignatureValidator?)null, expected),
            2 => await _facade.UploadPartAsync("bucket", "key", "upload", 1, reader, (ChecksumRequest?)null, expected),
            _ => await _facade.UploadPartAsync("bucket", "key", "upload", 1, reader, null, null, expected)
        };
        Assert.Equal(valid, result.IsSuccess);
        if (valid) Assert.Equal(ETagHelper.ComputeETag("payload"u8.ToArray()), result.Value!.ETag);
        else Assert.Equal("BadDigest", result.ErrorCode);
        _mockDataStorage.Verify(x => x.CommitPreparedPartAsync("bucket", "key", "upload", 1, It.IsAny<PreparedData>(), It.IsAny<CancellationToken>()), valid ? Times.Once() : Times.Never());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ListPartsAsync_MissingEtagComputesFromRawReaderAndPreservesChecksums(bool hasMetadata)
    {
        var upload = new MultipartUpload { BucketName = "bucket", Key = "key", UploadId = "upload" };
        upload.Parts[1] = new PartMetadata { ETag = string.Empty, ChecksumSHA256 = "stored-checksum" };
        _mockMetadataStorage.Setup(x => x.GetUploadMetadataAsync("bucket", "key", "upload", It.IsAny<CancellationToken>()))
            .ReturnsAsync(hasMetadata ? upload : null);
        _mockDataStorage.Setup(x => x.GetStoredPartsAsync("bucket", "key", "upload", It.IsAny<CancellationToken>()))
            .ReturnsAsync([new UploadPart { PartNumber = 1, ETag = string.Empty, Size = 7 }]);
        _mockDataStorage.Setup(x => x.GetPartReadersAsync("bucket", "key", "upload", It.IsAny<List<CompletedPart>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new[] { CreatePipeReader("payload") });
        var result = await _facade.ListPartsAsync("bucket", "key", "upload");
        Assert.True(result.IsSuccess);
        var part = Assert.Single(result.Value!);
        Assert.Equal(ETagHelper.ComputeETag("payload"u8.ToArray()), part.ETag);
        Assert.Equal(hasMetadata ? "stored-checksum" : null, part.ChecksumSHA256);
        _mockMetadataStorage.Verify(x => x.UpdateUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<MultipartUpload>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_MissingEtagComputesFromRawReader()
    {
        var etag = ETagHelper.ComputeETag("payload"u8.ToArray());
        _mockDataStorage.Setup(x => x.GetStoredPartsAsync("bucket", "key", "upload", It.IsAny<CancellationToken>()))
            .ReturnsAsync([new UploadPart { PartNumber = 1, ETag = string.Empty, Size = 7 }]);
        _mockDataStorage.Setup(x => x.GetPartReadersAsync("bucket", "key", "upload", It.IsAny<List<CompletedPart>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new[] { CreatePipeReader("payload") });
        _mockObjectDataStorage.Setup(x => x.PrepareMultipartDataAsync("bucket", "key", It.IsAny<IEnumerable<PipeReader>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PreparedData { BucketName = "bucket", Key = "key", Size = 7 });
        var result = await _facade.CompleteMultipartUploadAsync("bucket", "key", new CompleteMultipartUploadRequest
        {
            UploadId = "upload",
            Parts = [new CompletedPart { PartNumber = 1, ETag = etag }]
        });
        Assert.True(result.IsSuccess);
        Assert.Equal(ETagHelper.ComputeMultipartETag([etag]), result.Value!.ETag);
        _mockObjectDataStorage.Verify(x => x.OpenPreparedReadAsync(It.IsAny<PreparedData>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Complete_WithQueuedListing_DoesNotDisposeBorrowedUploadLock()
    {
        var commitEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCommit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var etag = ETagHelper.ComputeETag("payload"u8.ToArray());
        var uploadId = Guid.NewGuid().ToString("N");
        _mockDataStorage.Setup(x => x.GetStoredPartsAsync("bucket", "key", uploadId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new List<UploadPart> { new() { PartNumber = 1, ETag = etag, Size = 7 } });
        _mockDataStorage.Setup(x => x.GetPartReadersAsync("bucket", "key", uploadId, It.IsAny<List<CompletedPart>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new[] { CreatePipeReader("payload") });
        _mockObjectDataStorage.Setup(x => x.PrepareMultipartDataAsync("bucket", "key", It.IsAny<IEnumerable<PipeReader>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PreparedData { BucketName = "bucket", Key = "key", Size = 7 });
        _mockObjectDataStorage.Setup(x => x.CommitPreparedDataAsync(It.IsAny<PreparedData>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                commitEntered.SetResult();
                await allowCommit.Task;
            });
        var completion = _facade.CompleteMultipartUploadAsync("bucket", "key", new CompleteMultipartUploadRequest
        {
            UploadId = uploadId,
            Parts = [new CompletedPart { PartNumber = 1, ETag = etag }]
        });
        await commitEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var listing = _facade.ListPartsAsync("bucket", "key", uploadId);
        Assert.False(listing.IsCompleted);
        allowCommit.SetResult();
        await Task.WhenAll(completion, listing).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True((await completion).IsSuccess);
        Assert.True((await listing).IsSuccess);
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
