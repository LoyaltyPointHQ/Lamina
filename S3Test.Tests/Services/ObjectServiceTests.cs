using System.Text;
using S3Test.Models;
using S3Test.Services;
using System.Xml.Serialization;
using S3Test.Tests.Helpers;
using System.IO.Pipelines;

namespace S3Test.Tests.Services;

public class ObjectServiceTests
{
    private readonly IBucketService _bucketService;
    private readonly IObjectService _objectService;
    private readonly IMultipartUploadService _multipartUploadService;

    public ObjectServiceTests()
    {
        _bucketService = new InMemoryBucketService();
        _objectService = new InMemoryObjectService(_bucketService);
        _multipartUploadService = new InMemoryMultipartUploadService(_bucketService, _objectService);
    }

    [Fact]
    public async Task PutObjectAsync_ValidRequest_CreatesObject()
    {
        await _bucketService.CreateBucketAsync("test-bucket");
        var data = Encoding.UTF8.GetBytes("Test content");
        var request = new PutObjectRequest
        {
            Key = "test-object.txt",
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string> { { "Author", "Test" } }
        };

        var reader = PipeHelpers.CreatePipeReader(data);
        var result = await _objectService.PutObjectAsync("test-bucket", "test-object.txt", reader, request);

        Assert.NotNull(result);
        Assert.Equal("test-object.txt", result.Key);
        Assert.Equal("test-bucket", result.BucketName);
        Assert.Equal(data.Length, result.Size);
        Assert.Equal("text/plain", result.ContentType);
        Assert.NotEmpty(result.ETag);
        Assert.Equal("Test", result.Metadata["Author"]);
    }

    [Fact]
    public async Task PutObjectAsync_NonExistingBucket_ReturnsNull()
    {
        var data = Encoding.UTF8.GetBytes("Test content");

        var reader = PipeHelpers.CreatePipeReader(data);
        var result = await _objectService.PutObjectAsync("non-existing-bucket", "object.txt", reader);

        Assert.Null(result);
    }

    [Fact]
    public async Task GetObjectAsync_ExistingObject_ReturnsObject()
    {
        await _bucketService.CreateBucketAsync("get-bucket");
        var data = Encoding.UTF8.GetBytes("Get test content");
        var reader = PipeHelpers.CreatePipeReader(data);
        await _objectService.PutObjectAsync("get-bucket", "get-object.txt", reader);

        var result = await _objectService.GetObjectAsync("get-bucket", "get-object.txt");

        Assert.NotNull(result);
        Assert.Equal(data, result.Data);
        Assert.Equal(data.Length, result.ContentLength);
        Assert.NotEmpty(result.ETag);
    }

    [Fact]
    public async Task GetObjectAsync_NonExistingObject_ReturnsNull()
    {
        await _bucketService.CreateBucketAsync("get-bucket-2");

        var result = await _objectService.GetObjectAsync("get-bucket-2", "non-existing.txt");

        Assert.Null(result);
    }

    [Fact]
    public async Task DeleteObjectAsync_ExistingObject_ReturnsTrue()
    {
        await _bucketService.CreateBucketAsync("delete-bucket");
        var data = Encoding.UTF8.GetBytes("Delete test");
        var reader = PipeHelpers.CreatePipeReader(data);
        await _objectService.PutObjectAsync("delete-bucket", "delete-object.txt", reader);

        var deleteResult = await _objectService.DeleteObjectAsync("delete-bucket", "delete-object.txt");
        var getResult = await _objectService.GetObjectAsync("delete-bucket", "delete-object.txt");

        Assert.True(deleteResult);
        Assert.Null(getResult);
    }

    [Fact]
    public async Task DeleteObjectAsync_NonExistingObject_ReturnsFalse()
    {
        await _bucketService.CreateBucketAsync("delete-bucket-2");

        var result = await _objectService.DeleteObjectAsync("delete-bucket-2", "non-existing.txt");

        Assert.False(result);
    }

    [Fact]
    public async Task ListObjectsAsync_ReturnsObjects()
    {
        await _bucketService.CreateBucketAsync("list-bucket");
        await _objectService.PutObjectAsync("list-bucket", "file1.txt", PipeHelpers.CreatePipeReader(Encoding.UTF8.GetBytes("Content 1")));
        await _objectService.PutObjectAsync("list-bucket", "file2.txt", PipeHelpers.CreatePipeReader(Encoding.UTF8.GetBytes("Content 2")));
        await _objectService.PutObjectAsync("list-bucket", "folder/file3.txt", PipeHelpers.CreatePipeReader(Encoding.UTF8.GetBytes("Content 3")));

        var result = await _objectService.ListObjectsAsync("list-bucket");

        Assert.NotNull(result);
        Assert.Equal(3, result.Contents.Count);
        Assert.Contains(result.Contents, o => o.Key == "file1.txt");
        Assert.Contains(result.Contents, o => o.Key == "file2.txt");
        Assert.Contains(result.Contents, o => o.Key == "folder/file3.txt");

        foreach (var item in result.Contents)
        {
            Assert.NotEqual(default(DateTime), item.LastModified);
            Assert.NotEmpty(item.ETag);
            Assert.True(item.Size > 0);
        }
    }

    [Fact]
    public async Task ListObjectsAsync_WithPrefix_FiltersObjects()
    {
        await _bucketService.CreateBucketAsync("prefix-bucket");
        await _objectService.PutObjectAsync("prefix-bucket", "doc1.txt", PipeHelpers.CreatePipeReader(Encoding.UTF8.GetBytes("Doc 1")));
        await _objectService.PutObjectAsync("prefix-bucket", "doc2.txt", PipeHelpers.CreatePipeReader(Encoding.UTF8.GetBytes("Doc 2")));
        await _objectService.PutObjectAsync("prefix-bucket", "image.png", PipeHelpers.CreatePipeReader(Encoding.UTF8.GetBytes("Image")));

        var request = new ListObjectsRequest { Prefix = "doc" };
        var result = await _objectService.ListObjectsAsync("prefix-bucket", request);

        Assert.NotNull(result);
        Assert.Equal(2, result.Contents.Count);
        Assert.All(result.Contents, o => Assert.StartsWith("doc", o.Key));
        Assert.All(result.Contents, o => Assert.NotEmpty(o.ETag));
        Assert.All(result.Contents, o => Assert.True(o.Size > 0));
    }

    [Fact]
    public async Task ListObjectsAsync_WithMaxKeys_LimitsResults()
    {
        await _bucketService.CreateBucketAsync("max-keys-bucket");
        for (int i = 1; i <= 5; i++)
        {
            await _objectService.PutObjectAsync("max-keys-bucket", $"file{i}.txt", PipeHelpers.CreatePipeReader(Encoding.UTF8.GetBytes($"Content {i}")));
        }

        var request = new ListObjectsRequest { MaxKeys = 2 };
        var result = await _objectService.ListObjectsAsync("max-keys-bucket", request);

        Assert.NotNull(result);
        Assert.Equal(2, result.Contents.Count);
        Assert.True(result.IsTruncated);
        Assert.NotNull(result.NextContinuationToken);
    }

    [Fact]
    public async Task ObjectExistsAsync_ExistingObject_ReturnsTrue()
    {
        await _bucketService.CreateBucketAsync("exists-bucket");
        await _objectService.PutObjectAsync("exists-bucket", "exists.txt", PipeHelpers.CreatePipeReader(Encoding.UTF8.GetBytes("Exists")));

        var result = await _objectService.ObjectExistsAsync("exists-bucket", "exists.txt");

        Assert.True(result);
    }

    [Fact]
    public async Task ObjectExistsAsync_NonExistingObject_ReturnsFalse()
    {
        await _bucketService.CreateBucketAsync("exists-bucket-2");

        var result = await _objectService.ObjectExistsAsync("exists-bucket-2", "not-exists.txt");

        Assert.False(result);
    }

    [Fact]
    public async Task GetObjectInfoAsync_ExistingObject_ReturnsInfo()
    {
        await _bucketService.CreateBucketAsync("info-bucket");
        var data = Encoding.UTF8.GetBytes("Info content");
        var reader = PipeHelpers.CreatePipeReader(data);
        await _objectService.PutObjectAsync("info-bucket", "info.txt", reader);

        var result = await _objectService.GetObjectInfoAsync("info-bucket", "info.txt");

        Assert.NotNull(result);
        Assert.Equal("info.txt", result.Key);
        Assert.Equal(data.Length, result.Size);
        Assert.NotEmpty(result.ETag);
    }

    [Fact]
    public async Task GetObjectInfoAsync_NonExistingObject_ReturnsNull()
    {
        await _bucketService.CreateBucketAsync("info-bucket-2");

        var result = await _objectService.GetObjectInfoAsync("info-bucket-2", "not-found.txt");

        Assert.Null(result);
    }

    [Fact]
    public async Task ListObjectsAsync_IncludesMultipartUploadedObjects()
    {
        // Arrange
        await _bucketService.CreateBucketAsync("multipart-list-bucket");

        // Add a regular object
        await _objectService.PutObjectAsync("multipart-list-bucket", "regular.txt",
            PipeHelpers.CreatePipeReader(Encoding.UTF8.GetBytes("Regular upload")));

        // Add a multipart uploaded object
        var initRequest = new InitiateMultipartUploadRequest { Key = "multipart.bin" };
        var initResult = await _multipartUploadService.InitiateMultipartUploadAsync("multipart-list-bucket", initRequest);

        var part1Data = Encoding.UTF8.GetBytes("Part 1 data");
        var part2Data = Encoding.UTF8.GetBytes("Part 2 data");

        var part1 = await _multipartUploadService.UploadPartAsync(
            "multipart-list-bucket", "multipart.bin", initResult.UploadId, 1, PipeHelpers.CreatePipeReader(part1Data));
        var part2 = await _multipartUploadService.UploadPartAsync(
            "multipart-list-bucket", "multipart.bin", initResult.UploadId, 2, PipeHelpers.CreatePipeReader(part2Data));

        var completeRequest = new CompleteMultipartUploadRequest
        {
            UploadId = initResult.UploadId,
            Parts = new List<CompletedPart>
            {
                new() { PartNumber = 1, ETag = part1.ETag },
                new() { PartNumber = 2, ETag = part2.ETag }
            }
        };

        var completeResult = await _multipartUploadService.CompleteMultipartUploadAsync(
            "multipart-list-bucket", "multipart.bin", completeRequest);
        Assert.NotNull(completeResult);

        // Act - List all objects
        var listResult = await _objectService.ListObjectsAsync("multipart-list-bucket");

        // Assert
        Assert.NotNull(listResult);
        Assert.Equal(2, listResult.Contents.Count);

        // Check that both objects are present
        var regularObject = listResult.Contents.FirstOrDefault(o => o.Key == "regular.txt");
        Assert.NotNull(regularObject);
        Assert.Equal(14, regularObject.Size); // "Regular upload" = 14 bytes

        var multipartObject = listResult.Contents.FirstOrDefault(o => o.Key == "multipart.bin");
        Assert.NotNull(multipartObject);
        Assert.Equal(22, multipartObject.Size); // "Part 1 data" + "Part 2 data" = 22 bytes

        // Verify the multipart object can be retrieved
        var getResult = await _objectService.GetObjectAsync("multipart-list-bucket", "multipart.bin");
        Assert.NotNull(getResult);
        var content = Encoding.UTF8.GetString(getResult.Data);
        Assert.Equal("Part 1 dataPart 2 data", content);
    }

    [Fact]
    public async Task ListObjectsAsync_MultipartUploadedObjectsWithPrefix()
    {
        // Arrange
        await _bucketService.CreateBucketAsync("prefix-multipart-bucket");

        // Add regular objects
        await _objectService.PutObjectAsync("prefix-multipart-bucket", "docs/regular.txt",
            PipeHelpers.CreatePipeReader(Encoding.UTF8.GetBytes("Regular")));
        await _objectService.PutObjectAsync("prefix-multipart-bucket", "images/photo.jpg",
            PipeHelpers.CreatePipeReader(Encoding.UTF8.GetBytes("Photo")));

        // Add multipart uploaded object with prefix
        var initRequest = new InitiateMultipartUploadRequest { Key = "docs/multipart.bin" };
        var initResult = await _multipartUploadService.InitiateMultipartUploadAsync("prefix-multipart-bucket", initRequest);

        var partData = Encoding.UTF8.GetBytes("Multipart data");
        var part = await _multipartUploadService.UploadPartAsync(
            "prefix-multipart-bucket", "docs/multipart.bin", initResult.UploadId, 1, PipeHelpers.CreatePipeReader(partData));

        var completeRequest = new CompleteMultipartUploadRequest
        {
            UploadId = initResult.UploadId,
            Parts = new List<CompletedPart> { new() { PartNumber = 1, ETag = part.ETag } }
        };

        await _multipartUploadService.CompleteMultipartUploadAsync(
            "prefix-multipart-bucket", "docs/multipart.bin", completeRequest);

        // Act - List objects with prefix
        var listRequest = new ListObjectsRequest { Prefix = "docs/" };
        var listResult = await _objectService.ListObjectsAsync("prefix-multipart-bucket", listRequest);

        // Assert
        Assert.NotNull(listResult);
        Assert.Equal(2, listResult.Contents.Count);
        Assert.All(listResult.Contents, o => Assert.StartsWith("docs/", o.Key));

        // Verify both regular and multipart objects are included
        Assert.Contains(listResult.Contents, o => o.Key == "docs/regular.txt");
        Assert.Contains(listResult.Contents, o => o.Key == "docs/multipart.bin");
    }
}