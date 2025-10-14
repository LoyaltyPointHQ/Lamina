using System.IO.Pipelines;
using System.Text;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.InMemory;
using Lamina.WebApi.Services;
using Microsoft.Extensions.Logging;
using Moq;

namespace Lamina.Storage.Core.Tests;

public class ByteRangeReadingTests
{
    private readonly InMemoryObjectDataStorage _inMemoryStorage;
    private readonly Mock<IObjectMetadataStorage> _metadataStorageMock;
    private readonly Mock<IBucketStorageFacade> _bucketStorageMock;
    private readonly Mock<IMultipartUploadStorageFacade> _multipartUploadStorageMock;
    private readonly Mock<ILogger<ObjectStorageFacade>> _loggerMock;
    private readonly IContentTypeDetector _contentTypeDetector;
    private readonly ObjectStorageFacade _facade;

    public ByteRangeReadingTests()
    {
        var mockChunkedDataParser = new Mock<IChunkedDataParser>();
        _inMemoryStorage = new InMemoryObjectDataStorage(mockChunkedDataParser.Object);
        _metadataStorageMock = new Mock<IObjectMetadataStorage>();
        _bucketStorageMock = new Mock<IBucketStorageFacade>();
        _multipartUploadStorageMock = new Mock<IMultipartUploadStorageFacade>();
        _loggerMock = new Mock<ILogger<ObjectStorageFacade>>();
        _contentTypeDetector = new FileExtensionContentTypeDetector();

        _bucketStorageMock.Setup(x => x.GetBucketAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string bucketName, CancellationToken ct) => new Bucket
            {
                Name = bucketName,
                Type = BucketType.GeneralPurpose,
                CreationDate = DateTime.UtcNow
            });

        _facade = new ObjectStorageFacade(_inMemoryStorage, _metadataStorageMock.Object, _bucketStorageMock.Object, _multipartUploadStorageMock.Object, _loggerMock.Object, _contentTypeDetector);
    }

    private async Task<string> CreateTestObjectAsync(string bucketName, string key, string content)
    {
        var data = Encoding.UTF8.GetBytes(content);
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(data);
        await pipe.Writer.CompleteAsync();

        var storeResult = await _inMemoryStorage.StoreDataAsync(bucketName, key, pipe.Reader, null, null, default);
        Assert.True(storeResult.IsSuccess);
        return content;
    }

    private async Task<string> ReadFromPipeAsync(PipeReader reader)
    {
        var buffer = new List<byte>();
        while (true)
        {
            var result = await reader.ReadAsync();
            var readBuffer = result.Buffer;

            foreach (var segment in readBuffer)
            {
                buffer.AddRange(segment.ToArray());
            }

            reader.AdvanceTo(readBuffer.End);

            if (result.IsCompleted)
            {
                break;
            }
        }

        await reader.CompleteAsync();
        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    [Fact]
    public async Task WriteDataToPipeAsync_NoByteRange_ReadsEntireFile()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        const string content = "0123456789ABCDEFGHIJ"; // 20 bytes
        await CreateTestObjectAsync(bucketName, key, content);

        var pipe = new Pipe();

        // Act
        var writeTask = _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe.Writer, null, null, default);
        var readTask = ReadFromPipeAsync(pipe.Reader);

        await writeTask;
        var result = await readTask;

        // Assert
        Assert.Equal(content, result);
    }

    [Fact]
    public async Task WriteDataToPipeAsync_ReadFromStartWithEnd_ReturnsCorrectBytes()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        const string content = "0123456789ABCDEFGHIJ"; // 20 bytes
        await CreateTestObjectAsync(bucketName, key, content);

        var pipe = new Pipe();

        // Act - Read bytes 0-9 (first 10 bytes)
        var writeTask = _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe.Writer, 0, 9, default);
        var readTask = ReadFromPipeAsync(pipe.Reader);

        await writeTask;
        var result = await readTask;

        // Assert
        Assert.Equal("0123456789", result);
    }

    [Fact]
    public async Task WriteDataToPipeAsync_ReadMiddlePortion_ReturnsCorrectBytes()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        const string content = "0123456789ABCDEFGHIJ"; // 20 bytes
        await CreateTestObjectAsync(bucketName, key, content);

        var pipe = new Pipe();

        // Act - Read bytes 5-14 (middle 10 bytes: "56789ABCDE")
        var writeTask = _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe.Writer, 5, 14, default);
        var readTask = ReadFromPipeAsync(pipe.Reader);

        await writeTask;
        var result = await readTask;

        // Assert
        Assert.Equal("56789ABCDE", result);
    }

    [Fact]
    public async Task WriteDataToPipeAsync_ReadToEndWithStart_ReturnsCorrectBytes()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        const string content = "0123456789ABCDEFGHIJ"; // 20 bytes
        await CreateTestObjectAsync(bucketName, key, content);

        var pipe = new Pipe();

        // Act - Read from byte 15 to end (last 5 bytes: "FGHIJ")
        var writeTask = _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe.Writer, 15, 19, default);
        var readTask = ReadFromPipeAsync(pipe.Reader);

        await writeTask;
        var result = await readTask;

        // Assert
        Assert.Equal("FGHIJ", result);
    }

    [Fact]
    public async Task WriteDataToPipeAsync_ReadSingleByte_ReturnsCorrectByte()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        const string content = "0123456789ABCDEFGHIJ"; // 20 bytes
        await CreateTestObjectAsync(bucketName, key, content);

        var pipe = new Pipe();

        // Act - Read single byte at position 10 ('A')
        var writeTask = _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe.Writer, 10, 10, default);
        var readTask = ReadFromPipeAsync(pipe.Reader);

        await writeTask;
        var result = await readTask;

        // Assert
        Assert.Equal("A", result);
    }

    [Fact]
    public async Task WriteDataToPipeAsync_ReadEntireFileExplicitly_ReturnsAllBytes()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        const string content = "0123456789ABCDEFGHIJ"; // 20 bytes
        await CreateTestObjectAsync(bucketName, key, content);

        var pipe = new Pipe();

        // Act - Explicitly read bytes 0-19
        var writeTask = _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe.Writer, 0, 19, default);
        var readTask = ReadFromPipeAsync(pipe.Reader);

        await writeTask;
        var result = await readTask;

        // Assert
        Assert.Equal(content, result);
    }

    [Fact]
    public async Task WriteDataToPipeAsync_InvalidRangeStartGreaterThanEnd_ReturnsFalse()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        const string content = "0123456789ABCDEFGHIJ"; // 20 bytes
        await CreateTestObjectAsync(bucketName, key, content);

        var pipe = new Pipe();

        // Act - Invalid range: start=15, end=10
        var result = await _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe.Writer, 15, 10, default);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task WriteDataToPipeAsync_InvalidRangeEndBeyondFileSize_ReturnsFalse()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        const string content = "0123456789ABCDEFGHIJ"; // 20 bytes
        await CreateTestObjectAsync(bucketName, key, content);

        var pipe = new Pipe();

        // Act - Invalid range: end=100 (beyond file size of 20)
        var result = await _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe.Writer, 0, 100, default);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task WriteDataToPipeAsync_InvalidRangeNegativeStart_ReturnsFalse()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        const string content = "0123456789ABCDEFGHIJ"; // 20 bytes
        await CreateTestObjectAsync(bucketName, key, content);

        var pipe = new Pipe();

        // Act - Invalid range: start=-5
        var result = await _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe.Writer, -5, 10, default);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task WriteDataToPipeAsync_ConcurrentReadsOfDifferentRanges_BothSucceed()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        var content = new string('A', 10000) + new string('B', 10000) + new string('C', 10000); // 30KB
        await CreateTestObjectAsync(bucketName, key, content);

        var pipe1 = new Pipe();
        var pipe2 = new Pipe();

        // Act - Read two different ranges concurrently (simulating parallel UploadPartCopy)
        var write1Task = _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe1.Writer, 0, 9999, default); // First 10KB
        var write2Task = _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe2.Writer, 10000, 19999, default); // Second 10KB

        var read1Task = ReadFromPipeAsync(pipe1.Reader);
        var read2Task = ReadFromPipeAsync(pipe2.Reader);

        await Task.WhenAll(write1Task, write2Task);
        var result1 = await read1Task;
        var result2 = await read2Task;

        // Assert
        Assert.Equal(10000, result1.Length);
        Assert.Equal(10000, result2.Length);
        Assert.True(result1.All(c => c == 'A'));
        Assert.True(result2.All(c => c == 'B'));
    }

    [Fact]
    public async Task WriteDataToPipeAsync_ThreeConcurrentReadsFromSameFile_AllSucceed()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "large-object.bin";
        var content = new string('X', 5000) + new string('Y', 5000) + new string('Z', 5000); // 15KB
        await CreateTestObjectAsync(bucketName, key, content);

        var pipe1 = new Pipe();
        var pipe2 = new Pipe();
        var pipe3 = new Pipe();

        // Act - Read three different ranges concurrently
        var write1Task = _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe1.Writer, 0, 4999, default);
        var write2Task = _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe2.Writer, 5000, 9999, default);
        var write3Task = _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe3.Writer, 10000, 14999, default);

        var read1Task = ReadFromPipeAsync(pipe1.Reader);
        var read2Task = ReadFromPipeAsync(pipe2.Reader);
        var read3Task = ReadFromPipeAsync(pipe3.Reader);

        await Task.WhenAll(write1Task, write2Task, write3Task);
        var result1 = await read1Task;
        var result2 = await read2Task;
        var result3 = await read3Task;

        // Assert
        Assert.Equal(5000, result1.Length);
        Assert.Equal(5000, result2.Length);
        Assert.Equal(5000, result3.Length);
        Assert.True(result1.All(c => c == 'X'));
        Assert.True(result2.All(c => c == 'Y'));
        Assert.True(result3.All(c => c == 'Z'));
    }

    [Fact]
    public async Task WriteDataToPipeAsync_OverlappingConcurrentReads_BothSucceed()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        const string content = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 36 bytes
        await CreateTestObjectAsync(bucketName, key, content);

        var pipe1 = new Pipe();
        var pipe2 = new Pipe();

        // Act - Read overlapping ranges concurrently
        var write1Task = _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe1.Writer, 0, 19, default); // Bytes 0-19
        var write2Task = _inMemoryStorage.WriteDataToPipeAsync(bucketName, key, pipe2.Writer, 10, 29, default); // Bytes 10-29 (overlaps)

        var read1Task = ReadFromPipeAsync(pipe1.Reader);
        var read2Task = ReadFromPipeAsync(pipe2.Reader);

        await Task.WhenAll(write1Task, write2Task);
        var result1 = await read1Task;
        var result2 = await read2Task;

        // Assert
        Assert.Equal("0123456789ABCDEFGHIJ", result1);
        Assert.Equal("ABCDEFGHIJKLMNOPQRST", result2);
    }

    [Fact]
    public async Task CopyObjectPartAsync_WithByteRange_CopiesOnlySpecifiedBytes()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string sourceKey = "source-object.txt";
        const string destKey = "dest-object.txt";
        const string uploadId = "test-upload-id";
        const int partNumber = 1;
        const string content = "0123456789ABCDEFGHIJ"; // 20 bytes

        await CreateTestObjectAsync(bucketName, sourceKey, content);

        // Mock metadata storage
        _metadataStorageMock.Setup(x => x.GetMetadataAsync(bucketName, sourceKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync((S3ObjectInfo?)null);

        _metadataStorageMock.Setup(x => x.IsValidObjectKey(It.IsAny<string>()))
            .Returns(true);

        // Mock multipart upload storage to capture the uploaded data
        var capturedData = new List<byte>();
        _multipartUploadStorageMock.Setup(x => x.UploadPartAsync(
            bucketName, destKey, uploadId, partNumber,
            It.IsAny<PipeReader>(),
            It.IsAny<ChecksumRequest?>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync((string bn, string k, string uid, int pn, PipeReader reader, ChecksumRequest? cr, CancellationToken ct) =>
            {
                var readTask = Task.Run(async () =>
                {
                    while (true)
                    {
                        var result = await reader.ReadAsync(ct);
                        var buffer = result.Buffer;

                        foreach (var segment in buffer)
                        {
                            capturedData.AddRange(segment.ToArray());
                        }

                        reader.AdvanceTo(buffer.End);

                        if (result.IsCompleted)
                        {
                            break;
                        }
                    }

                    await reader.CompleteAsync();
                });

                readTask.Wait(ct);

                var etag = ETagHelper.ComputeETag(capturedData.ToArray());
                return StorageResult<UploadPart>.Success(new UploadPart
                {
                    PartNumber = pn,
                    ETag = etag,
                    Size = capturedData.Count,
                    LastModified = DateTime.UtcNow
                });
            });

        // Act - Copy bytes 5-14 (10 bytes: "56789ABCDE")
        var result = await _facade.CopyObjectPartAsync(
            bucketName, sourceKey,
            bucketName, destKey,
            uploadId, partNumber,
            5, 14,
            null,
            default);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(10, capturedData.Count);
        Assert.Equal("56789ABCDE", Encoding.UTF8.GetString(capturedData.ToArray()));
    }
}
