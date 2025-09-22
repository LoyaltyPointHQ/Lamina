using System.IO.Pipelines;
using System.Text;
using Microsoft.Extensions.Logging;
using Moq;
using Lamina.Models;
using Lamina.Storage.Abstract;
using Lamina.Storage.InMemory;
using Lamina.Helpers;

namespace Lamina.Tests.Services;

public class DataFirstObjectStorageTests
{
    private readonly Mock<IObjectDataStorage> _dataStorageMock;
    private readonly Mock<IObjectMetadataStorage> _metadataStorageMock;
    private readonly Mock<ILogger<ObjectStorageFacade>> _loggerMock;
    private readonly ObjectStorageFacade _facade;

    public DataFirstObjectStorageTests()
    {
        _dataStorageMock = new Mock<IObjectDataStorage>();
        _metadataStorageMock = new Mock<IObjectMetadataStorage>();
        _loggerMock = new Mock<ILogger<ObjectStorageFacade>>();
        _facade = new ObjectStorageFacade(_dataStorageMock.Object, _metadataStorageMock.Object, _loggerMock.Object);
    }

    [Fact]
    public async Task ObjectExistsAsync_ChecksDataExistenceNotMetadata()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";

        _dataStorageMock.Setup(x => x.DataExistsAsync(bucketName, key, default))
            .ReturnsAsync(true);

        // The metadata existence check should never be called
        _metadataStorageMock.Setup(x => x.MetadataExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Throws(new InvalidOperationException("Should not check metadata existence"));

        // Act
        var exists = await _facade.ObjectExistsAsync(bucketName, key);

        // Assert
        Assert.True(exists);
        _dataStorageMock.Verify(x => x.DataExistsAsync(bucketName, key, default), Times.Once);
        _metadataStorageMock.Verify(x => x.MetadataExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task GetObjectInfoAsync_GeneratesMetadataOnTheFlyWhenMissing()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var testData = Encoding.UTF8.GetBytes("Test content");
        var expectedEtag = ETagHelper.ComputeETag(testData);
        var expectedSize = testData.Length;
        var expectedLastModified = DateTime.UtcNow;

        // Data exists
        _dataStorageMock.Setup(x => x.GetDataInfoAsync(bucketName, key, default))
            .ReturnsAsync((expectedSize, expectedLastModified));

        // But metadata doesn't exist
        _metadataStorageMock.Setup(x => x.GetMetadataAsync(bucketName, key, default))
            .ReturnsAsync((S3ObjectInfo?)null);

        // Setup the ComputeETagAsync method
        _dataStorageMock.Setup(x => x.ComputeETagAsync(bucketName, key, default))
            .ReturnsAsync(expectedEtag);

        // Act
        var objectInfo = await _facade.GetObjectInfoAsync(bucketName, key);

        // Assert
        Assert.NotNull(objectInfo);
        Assert.Equal(key, objectInfo.Key);
        Assert.Equal(expectedSize, objectInfo.Size);
        Assert.Equal(expectedLastModified, objectInfo.LastModified);
        Assert.Equal(expectedEtag, objectInfo.ETag);
        Assert.Equal("application/octet-stream", objectInfo.ContentType);
        Assert.NotNull(objectInfo.Metadata);
    }

    [Fact]
    public async Task GetObjectInfoAsync_ReturnsNullWhenDataDoesNotExist()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";

        // Data doesn't exist
        _dataStorageMock.Setup(x => x.GetDataInfoAsync(bucketName, key, default))
            .ReturnsAsync(((long size, DateTime lastModified)?)null);

        // Act
        var objectInfo = await _facade.GetObjectInfoAsync(bucketName, key);

        // Assert
        Assert.Null(objectInfo);
        _dataStorageMock.Verify(x => x.GetDataInfoAsync(bucketName, key, default), Times.Once);
        _metadataStorageMock.Verify(x => x.GetMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ListObjectsAsync_IncludesObjectsWithoutMetadata()
    {
        // Arrange
        var bucketName = "test-bucket";
        var testData = Encoding.UTF8.GetBytes("Test content");
        var expectedEtag = ETagHelper.ComputeETag(testData);

        // Data storage returns additional keys (one with metadata, one without)
        _dataStorageMock.Setup(x => x.ListDataKeysAsync(bucketName, null, default))
            .ReturnsAsync(new[] { "file-with-metadata.txt", "file-without-metadata.txt" });

        // Setup metadata for first file (with metadata)
        _metadataStorageMock.Setup(x => x.GetMetadataAsync(bucketName, "file-with-metadata.txt", default))
            .ReturnsAsync(new S3ObjectInfo
            {
                Key = "file-with-metadata.txt",
                Size = 100,
                LastModified = DateTime.UtcNow,
                ETag = "etag1",
                ContentType = "text/plain"
            });

        // Setup metadata for second file (without metadata - returns null)
        _metadataStorageMock.Setup(x => x.GetMetadataAsync(bucketName, "file-without-metadata.txt", default))
            .ReturnsAsync((S3ObjectInfo?)null);

        // Setup info for file without metadata
        _dataStorageMock.Setup(x => x.GetDataInfoAsync(bucketName, "file-without-metadata.txt", default))
            .ReturnsAsync((200L, DateTime.UtcNow));

        // Setup the ComputeETagAsync method for file without metadata
        _dataStorageMock.Setup(x => x.ComputeETagAsync(bucketName, "file-without-metadata.txt", default))
            .ReturnsAsync(expectedEtag);

        // Act
        var response = await _facade.ListObjectsAsync(bucketName, null);

        // Assert
        Assert.NotNull(response);
        Assert.Equal(2, response.Contents.Count);
        Assert.Contains(response.Contents, o => o.Key == "file-with-metadata.txt");
        Assert.Contains(response.Contents, o => o.Key == "file-without-metadata.txt");

        var orphanedFile = response.Contents.First(o => o.Key == "file-without-metadata.txt");
        Assert.Equal(200L, orphanedFile.Size);
        Assert.Equal(expectedEtag, orphanedFile.ETag);
        Assert.Equal("text/plain", orphanedFile.ContentType);  // Now correctly detects .txt as text/plain
    }

    [Fact]
    public async Task DeleteObjectAsync_DeletesBothDataAndMetadata()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";

        _dataStorageMock.Setup(x => x.DeleteDataAsync(bucketName, key, default))
            .ReturnsAsync(true);
        _metadataStorageMock.Setup(x => x.DeleteMetadataAsync(bucketName, key, default))
            .ReturnsAsync(false);  // Metadata doesn't exist

        // Act
        var result = await _facade.DeleteObjectAsync(bucketName, key);

        // Assert
        Assert.True(result);  // Should return true if at least one was deleted
        _dataStorageMock.Verify(x => x.DeleteDataAsync(bucketName, key, default), Times.Once);
        _metadataStorageMock.Verify(x => x.DeleteMetadataAsync(bucketName, key, default), Times.Once);
    }

    [Fact]
    public async Task GetObjectInfoAsync_DetectsCorrectContentType()
    {
        // Arrange
        var bucketName = "test-bucket";

        var testCases = new[]
        {
            ("test.txt", "text/plain"),
            ("test.json", "application/json"),
            ("test.xml", "text/xml"),
            ("test.pdf", "application/pdf"),
            ("test.jpg", "image/jpeg"),
            ("test.png", "image/png"),
            ("test.html", "text/html"),
            ("test.css", "text/css"),
            ("test.js", "text/javascript"),  // .NET returns text/javascript
            ("test.yaml", "text/yaml"),
            ("test.yml", "text/yaml"),
            ("test.log", "text/plain"),
            ("test.zip", "application/x-zip-compressed"),  // .NET returns this for .zip
            ("test.mp4", "video/mp4"),
            ("test.mp3", "audio/mpeg"),
            ("test.dockerfile", "text/plain"),
            ("test.gitignore", "text/plain"),
            ("test", "application/octet-stream"),  // No extension
            ("test.unknown", "application/octet-stream")  // Unknown extension
        };

        foreach (var (key, expectedContentType) in testCases)
        {
            var testData = Encoding.UTF8.GetBytes("Test content");
            var expectedEtag = ETagHelper.ComputeETag(testData);
            var expectedSize = testData.Length;
            var expectedLastModified = DateTime.UtcNow;

            // Data exists
            _dataStorageMock.Setup(x => x.GetDataInfoAsync(bucketName, key, default))
                .ReturnsAsync((expectedSize, expectedLastModified));

            // But metadata doesn't exist
            _metadataStorageMock.Setup(x => x.GetMetadataAsync(bucketName, key, default))
                .ReturnsAsync((S3ObjectInfo?)null);

            // Setup the ComputeETagAsync method
            _dataStorageMock.Setup(x => x.ComputeETagAsync(bucketName, key, default))
                .ReturnsAsync(expectedEtag);

            // Act
            var objectInfo = await _facade.GetObjectInfoAsync(bucketName, key);

            // Assert
            Assert.NotNull(objectInfo);
            Assert.Equal(expectedContentType, objectInfo.ContentType);
        }
    }

    [Fact]
    public async Task InMemoryDataStorage_ImplementsNewMethods()
    {
        // Test that InMemoryObjectDataStorage properly implements the new interface methods
        var storage = new InMemoryObjectDataStorage();
        var bucketName = "test-bucket";
        var key = "test-key";
        var testData = Encoding.UTF8.GetBytes("Test content");

        // Store some data first
        var pipe = new Pipe();
        var writeTask = pipe.Writer.WriteAsync(testData).AsTask();
        await pipe.Writer.CompleteAsync();
        await writeTask;

        var (size, etag) = await storage.StoreDataAsync(bucketName, key, pipe.Reader);

        // Test DataExistsAsync
        var exists = await storage.DataExistsAsync(bucketName, key);
        Assert.True(exists);

        // Test GetDataInfoAsync
        var dataInfo = await storage.GetDataInfoAsync(bucketName, key);
        Assert.NotNull(dataInfo);
        Assert.Equal(testData.Length, dataInfo.Value.size);

        // Test ListDataKeysAsync
        var keys = await storage.ListDataKeysAsync(bucketName);
        Assert.Contains(key, keys);

        // Test with prefix
        var keysWithPrefix = await storage.ListDataKeysAsync(bucketName, "test");
        Assert.Contains(key, keysWithPrefix);

        var keysWithWrongPrefix = await storage.ListDataKeysAsync(bucketName, "wrong");
        Assert.Empty(keysWithWrongPrefix);
    }
}