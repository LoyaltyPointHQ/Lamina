using System.IO.Pipelines;
using System.Text;
using Lamina.Core.Models;
using Microsoft.Extensions.Logging;
using Moq;
using Lamina.Storage.Core;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.InMemory;
using Lamina.WebApi.Services;

namespace Lamina.Storage.Core.Tests;

public class DataFirstObjectStorageTests
{
    private readonly Mock<IObjectDataStorage> _dataStorageMock;
    private readonly Mock<IObjectMetadataStorage> _metadataStorageMock;
    private readonly Mock<IBucketStorageFacade> _bucketStorageMock;
    private readonly Mock<IMultipartUploadStorageFacade> _multipartUploadStorageMock;
    private readonly Mock<ILogger<ObjectStorageFacade>> _loggerMock;
    private readonly IContentTypeDetector _contentTypeDetector;
    private readonly ObjectStorageFacade _facade;

    public DataFirstObjectStorageTests()
    {
        _dataStorageMock = new Mock<IObjectDataStorage>();
        _metadataStorageMock = new Mock<IObjectMetadataStorage>();
        _bucketStorageMock = new Mock<IBucketStorageFacade>();
        _multipartUploadStorageMock = new Mock<IMultipartUploadStorageFacade>();
        _loggerMock = new Mock<ILogger<ObjectStorageFacade>>();
        _contentTypeDetector = new FileExtensionContentTypeDetector();

        // Setup default bucket to return GeneralPurpose type
        _bucketStorageMock.Setup(x => x.GetBucketAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string bucketName, CancellationToken ct) => new Bucket
            {
                Name = bucketName,
                Type = BucketType.GeneralPurpose,
                CreationDate = DateTime.UtcNow
            });

        _facade = new ObjectStorageFacade(_dataStorageMock.Object, _metadataStorageMock.Object, _bucketStorageMock.Object, _multipartUploadStorageMock.Object, _loggerMock.Object, _contentTypeDetector);
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

        // Setup the new delimiter-aware method
        _dataStorageMock.Setup(x => x.ListDataKeysAsync(bucketName, It.IsAny<BucketType>(), null, null, null, 1000, default))
            .ReturnsAsync(new ListDataResult
            {
                Keys = new List<string> { "file-with-metadata.txt", "file-without-metadata.txt" },
                CommonPrefixes = new List<string>()
            });

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
        Assert.True(response.IsSuccess);
        Assert.NotNull(response.Value);
        Assert.Equal(2, response.Value.Contents.Count);
        Assert.Contains(response.Value.Contents, o => o.Key == "file-with-metadata.txt");
        Assert.Contains(response.Value.Contents, o => o.Key == "file-without-metadata.txt");

        var orphanedFile = response.Value.Contents.First(o => o.Key == "file-without-metadata.txt");
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
    public async Task ListObjectsAsync_DirectoryBucket_IncludesMultipartUploadPrefixesWithDelimiter()
    {
        // Arrange
        var bucketName = "test-bucket";
        var delimiter = "/";
        var prefix = "uploads/";

        // Setup bucket as Directory bucket
        _bucketStorageMock.Setup(x => x.GetBucketAsync(bucketName, default))
            .ReturnsAsync(new Bucket
            {
                Name = bucketName,
                Type = BucketType.Directory,
                CreationDate = DateTime.UtcNow
            });

        // Setup data storage to return completed objects
        _dataStorageMock.Setup(x => x.ListDataKeysAsync(bucketName, BucketType.Directory, prefix, delimiter, null, 1000, default))
            .ReturnsAsync(new ListDataResult
            {
                Keys = new List<string> { "uploads/completed/file.txt" },
                CommonPrefixes = new List<string> { "uploads/completed/" }
            });

        // Setup multipart uploads with in-progress uploads
        _multipartUploadStorageMock.Setup(x => x.ListMultipartUploadsAsync(bucketName, default))
            .ReturnsAsync(new List<MultipartUpload>
            {
                new MultipartUpload { Key = "uploads/inprogress/file1.txt", UploadId = "upload1" },
                new MultipartUpload { Key = "uploads/inprogress/file2.txt", UploadId = "upload2" },
                new MultipartUpload { Key = "uploads/other/file3.txt", UploadId = "upload3" }
            });

        // Setup metadata for completed object
        _metadataStorageMock.Setup(x => x.GetMetadataAsync(bucketName, "uploads/completed/file.txt", default))
            .ReturnsAsync(new S3ObjectInfo
            {
                Key = "uploads/completed/file.txt",
                Size = 100,
                LastModified = DateTime.UtcNow,
                ETag = "etag1",
                ContentType = "text/plain"
            });

        // Act
        var request = new ListObjectsRequest { Prefix = prefix, Delimiter = delimiter };
        var response = await _facade.ListObjectsAsync(bucketName, request);

        // Assert
        Assert.NotNull(response);
        Assert.True(response.IsSuccess);
        Assert.NotNull(response.Value);

        // Should include prefixes from both completed objects and multipart uploads
        Assert.Contains("uploads/completed/", response.Value.CommonPrefixes);
        Assert.Contains("uploads/inprogress/", response.Value.CommonPrefixes);
        Assert.Contains("uploads/other/", response.Value.CommonPrefixes);
        Assert.Equal(3, response.Value.CommonPrefixes.Count);
    }

    [Fact]
    public async Task ListObjectsAsync_GeneralPurposeBucket_DoesNotIncludeMultipartUploadPrefixes()
    {
        // Arrange
        var bucketName = "test-bucket";
        var delimiter = "/";

        // Setup bucket as General Purpose bucket (default)
        _bucketStorageMock.Setup(x => x.GetBucketAsync(bucketName, default))
            .ReturnsAsync(new Bucket
            {
                Name = bucketName,
                Type = BucketType.GeneralPurpose,
                CreationDate = DateTime.UtcNow
            });

        // Setup data storage to return completed objects
        _dataStorageMock.Setup(x => x.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose, null, delimiter, null, 1000, default))
            .ReturnsAsync(new ListDataResult
            {
                Keys = new List<string> { "folder/file.txt" },
                CommonPrefixes = new List<string> { "folder/" }
            });

        // Setup multipart uploads (these should NOT be included)
        _multipartUploadStorageMock.Setup(x => x.ListMultipartUploadsAsync(bucketName, default))
            .ReturnsAsync(new List<MultipartUpload>
            {
                new MultipartUpload { Key = "inprogress/file1.txt", UploadId = "upload1" }
            });

        // Setup metadata for completed object
        _metadataStorageMock.Setup(x => x.GetMetadataAsync(bucketName, "folder/file.txt", default))
            .ReturnsAsync(new S3ObjectInfo
            {
                Key = "folder/file.txt",
                Size = 100,
                LastModified = DateTime.UtcNow,
                ETag = "etag1",
                ContentType = "text/plain"
            });

        // Act
        var request = new ListObjectsRequest { Delimiter = delimiter };
        var response = await _facade.ListObjectsAsync(bucketName, request);

        // Assert
        Assert.NotNull(response);
        Assert.True(response.IsSuccess);
        Assert.NotNull(response.Value);

        // Should only include completed object prefix, NOT multipart upload prefix
        Assert.Single(response.Value.CommonPrefixes);
        Assert.Contains("folder/", response.Value.CommonPrefixes);
        Assert.DoesNotContain("inprogress/", response.Value.CommonPrefixes);

        // Verify multipart upload list was never called for General Purpose buckets
        _multipartUploadStorageMock.Verify(x => x.ListMultipartUploadsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ListObjectsAsync_DirectoryBucket_WithoutDelimiter_DoesNotIncludeMultipartUploadPrefixes()
    {
        // Arrange
        var bucketName = "test-bucket";

        // Setup bucket as Directory bucket
        _bucketStorageMock.Setup(x => x.GetBucketAsync(bucketName, default))
            .ReturnsAsync(new Bucket
            {
                Name = bucketName,
                Type = BucketType.Directory,
                CreationDate = DateTime.UtcNow
            });

        // Setup data storage to return completed objects
        _dataStorageMock.Setup(x => x.ListDataKeysAsync(bucketName, BucketType.Directory, null, null, null, 1000, default))
            .ReturnsAsync(new ListDataResult
            {
                Keys = new List<string> { "file1.txt" },
                CommonPrefixes = new List<string>()
            });

        // Setup multipart uploads (should NOT be processed without delimiter)
        _multipartUploadStorageMock.Setup(x => x.ListMultipartUploadsAsync(bucketName, default))
            .ReturnsAsync(new List<MultipartUpload>
            {
                new MultipartUpload { Key = "inprogress/file1.txt", UploadId = "upload1" }
            });

        // Setup metadata for completed object
        _metadataStorageMock.Setup(x => x.GetMetadataAsync(bucketName, "file1.txt", default))
            .ReturnsAsync(new S3ObjectInfo
            {
                Key = "file1.txt",
                Size = 100,
                LastModified = DateTime.UtcNow,
                ETag = "etag1",
                ContentType = "text/plain"
            });

        // Act
        var response = await _facade.ListObjectsAsync(bucketName, null);

        // Assert
        Assert.NotNull(response);
        Assert.True(response.IsSuccess);
        Assert.NotNull(response.Value);
        Assert.Empty(response.Value.CommonPrefixes);

        // Verify multipart upload list was never called (no delimiter)
        _multipartUploadStorageMock.Verify(x => x.ListMultipartUploadsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ListObjectsAsync_DirectoryBucket_FiltersMultipartUploadsByPrefix()
    {
        // Arrange
        var bucketName = "test-bucket";
        var delimiter = "/";
        var prefix = "uploads/docs/";

        // Setup bucket as Directory bucket
        _bucketStorageMock.Setup(x => x.GetBucketAsync(bucketName, default))
            .ReturnsAsync(new Bucket
            {
                Name = bucketName,
                Type = BucketType.Directory,
                CreationDate = DateTime.UtcNow
            });

        // Setup data storage to return no completed objects
        _dataStorageMock.Setup(x => x.ListDataKeysAsync(bucketName, BucketType.Directory, prefix, delimiter, null, 1000, default))
            .ReturnsAsync(new ListDataResult
            {
                Keys = new List<string>(),
                CommonPrefixes = new List<string>()
            });

        // Setup multipart uploads - some match prefix, some don't
        _multipartUploadStorageMock.Setup(x => x.ListMultipartUploadsAsync(bucketName, default))
            .ReturnsAsync(new List<MultipartUpload>
            {
                new MultipartUpload { Key = "uploads/docs/report.pdf", UploadId = "upload1" },
                new MultipartUpload { Key = "uploads/docs/presentation.pptx", UploadId = "upload2" },
                new MultipartUpload { Key = "uploads/images/photo.jpg", UploadId = "upload3" }, // Different prefix
                new MultipartUpload { Key = "other/file.txt", UploadId = "upload4" } // Different prefix
            });

        // Act
        var request = new ListObjectsRequest { Prefix = prefix, Delimiter = delimiter };
        var response = await _facade.ListObjectsAsync(bucketName, request);

        // Assert
        Assert.NotNull(response);
        Assert.True(response.IsSuccess);
        Assert.NotNull(response.Value);

        // Should NOT include any CommonPrefixes because the multipart upload keys don't have
        // a delimiter after the prefix (they are at the prefix level itself)
        Assert.Empty(response.Value.CommonPrefixes);
    }

    [Fact]
    public async Task ListObjectsAsync_DirectoryBucket_MergesDuplicatePrefixes()
    {
        // Arrange
        var bucketName = "test-bucket";
        var delimiter = "/";

        // Setup bucket as Directory bucket
        _bucketStorageMock.Setup(x => x.GetBucketAsync(bucketName, default))
            .ReturnsAsync(new Bucket
            {
                Name = bucketName,
                Type = BucketType.Directory,
                CreationDate = DateTime.UtcNow
            });

        // Setup data storage to return completed objects with prefix
        _dataStorageMock.Setup(x => x.ListDataKeysAsync(bucketName, BucketType.Directory, null, delimiter, null, 1000, default))
            .ReturnsAsync(new ListDataResult
            {
                Keys = new List<string> { "folder/completed.txt" },
                CommonPrefixes = new List<string> { "folder/" }
            });

        // Setup multipart uploads with same prefix
        _multipartUploadStorageMock.Setup(x => x.ListMultipartUploadsAsync(bucketName, default))
            .ReturnsAsync(new List<MultipartUpload>
            {
                new MultipartUpload { Key = "folder/inprogress1.txt", UploadId = "upload1" },
                new MultipartUpload { Key = "folder/inprogress2.txt", UploadId = "upload2" }
            });

        // Setup metadata for completed object
        _metadataStorageMock.Setup(x => x.GetMetadataAsync(bucketName, "folder/completed.txt", default))
            .ReturnsAsync(new S3ObjectInfo
            {
                Key = "folder/completed.txt",
                Size = 100,
                LastModified = DateTime.UtcNow,
                ETag = "etag1",
                ContentType = "text/plain"
            });

        // Act
        var request = new ListObjectsRequest { Delimiter = delimiter };
        var response = await _facade.ListObjectsAsync(bucketName, request);

        // Assert
        Assert.NotNull(response);
        Assert.True(response.IsSuccess);
        Assert.NotNull(response.Value);

        // Should only have one "folder/" prefix (no duplicates)
        Assert.Single(response.Value.CommonPrefixes);
        Assert.Contains("folder/", response.Value.CommonPrefixes);
    }
}