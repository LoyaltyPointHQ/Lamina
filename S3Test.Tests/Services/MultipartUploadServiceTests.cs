using System.Text;
using S3Test.Models;
using S3Test.Services;
using System.Xml.Serialization;

namespace S3Test.Tests.Services;

public class MultipartUploadServiceTests
{
    private readonly IBucketService _bucketService;
    private readonly IObjectService _objectService;
    private readonly IMultipartUploadService _multipartUploadService;

    public MultipartUploadServiceTests()
    {
        _bucketService = new InMemoryBucketService();
        _objectService = new InMemoryObjectService(_bucketService);
        _multipartUploadService = new InMemoryMultipartUploadService(_bucketService, _objectService);
    }

    [Fact]
    public async Task InitiateMultipartUploadAsync_ValidRequest_CreatesUpload()
    {
        await _bucketService.CreateBucketAsync("test-bucket");
        var request = new InitiateMultipartUploadRequest
        {
            Key = "large-file.bin",
            ContentType = "application/octet-stream",
            Metadata = new Dictionary<string, string> { { "Size", "Large" } }
        };

        var result = await _multipartUploadService.InitiateMultipartUploadAsync("test-bucket", request);

        Assert.NotNull(result);
        Assert.NotEmpty(result.UploadId);
        Assert.Equal("test-bucket", result.BucketName);
        Assert.Equal("large-file.bin", result.Key);
    }

    [Fact]
    public async Task InitiateMultipartUploadAsync_NonExistingBucket_ThrowsException()
    {
        var request = new InitiateMultipartUploadRequest { Key = "file.bin" };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _multipartUploadService.InitiateMultipartUploadAsync("non-existing-bucket", request));
    }

    [Fact]
    public async Task UploadPartAsync_ValidPart_ReturnsETag()
    {
        await _bucketService.CreateBucketAsync("part-bucket");
        var initRequest = new InitiateMultipartUploadRequest { Key = "multipart.bin" };
        var initResult = await _multipartUploadService.InitiateMultipartUploadAsync("part-bucket", initRequest);
        var partData = Encoding.UTF8.GetBytes("Part 1 content");

        var result = await _multipartUploadService.UploadPartAsync(
            "part-bucket", "multipart.bin", initResult.UploadId, 1, partData);

        Assert.NotNull(result);
        Assert.NotEmpty(result.ETag);
        Assert.Equal(1, result.PartNumber);
    }

    [Fact]
    public async Task UploadPartAsync_InvalidUploadId_ThrowsException()
    {
        await _bucketService.CreateBucketAsync("invalid-bucket");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _multipartUploadService.UploadPartAsync(
                "invalid-bucket", "file.bin", "invalid-upload-id", 1, new byte[100]));
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_ValidParts_CreatesObject()
    {
        await _bucketService.CreateBucketAsync("complete-bucket");
        var initRequest = new InitiateMultipartUploadRequest { Key = "complete-file.bin" };
        var initResult = await _multipartUploadService.InitiateMultipartUploadAsync("complete-bucket", initRequest);

        var part1Data = Encoding.UTF8.GetBytes("Part 1 ");
        var part2Data = Encoding.UTF8.GetBytes("Part 2");
        var part1 = await _multipartUploadService.UploadPartAsync(
            "complete-bucket", "complete-file.bin", initResult.UploadId, 1, part1Data);
        var part2 = await _multipartUploadService.UploadPartAsync(
            "complete-bucket", "complete-file.bin", initResult.UploadId, 2, part2Data);

        var completeRequest = new CompleteMultipartUploadRequest
        {
            UploadId = initResult.UploadId,
            Parts = new List<CompletedPart>
            {
                new() { PartNumber = 1, ETag = part1.ETag },
                new() { PartNumber = 2, ETag = part2.ETag }
            }
        };

        var result = await _multipartUploadService.CompleteMultipartUploadAsync(
            "complete-bucket", "complete-file.bin", completeRequest);

        Assert.NotNull(result);
        Assert.Equal("complete-bucket", result.BucketName);
        Assert.Equal("complete-file.bin", result.Key);
        Assert.NotEmpty(result.ETag);

        var obj = await _objectService.GetObjectAsync("complete-bucket", "complete-file.bin");
        Assert.NotNull(obj);
        var content = Encoding.UTF8.GetString(obj.Data);
        Assert.Equal("Part 1 Part 2", content);
    }

    [Fact]
    public async Task CompleteMultipartUploadAsync_MissingParts_ReturnsNull()
    {
        await _bucketService.CreateBucketAsync("missing-bucket");
        var initRequest = new InitiateMultipartUploadRequest { Key = "missing.bin" };
        var initResult = await _multipartUploadService.InitiateMultipartUploadAsync("missing-bucket", initRequest);

        var part1 = await _multipartUploadService.UploadPartAsync(
            "missing-bucket", "missing.bin", initResult.UploadId, 1, new byte[100]);

        var completeRequest = new CompleteMultipartUploadRequest
        {
            UploadId = initResult.UploadId,
            Parts = new List<CompletedPart>
            {
                new() { PartNumber = 1, ETag = part1.ETag },
                new() { PartNumber = 2, ETag = "invalid-etag" }
            }
        };

        var result = await _multipartUploadService.CompleteMultipartUploadAsync(
            "missing-bucket", "missing.bin", completeRequest);

        Assert.Null(result);
    }

    [Fact]
    public async Task AbortMultipartUploadAsync_ExistingUpload_ReturnsTrue()
    {
        await _bucketService.CreateBucketAsync("abort-bucket");
        var initRequest = new InitiateMultipartUploadRequest { Key = "abort.bin" };
        var initResult = await _multipartUploadService.InitiateMultipartUploadAsync("abort-bucket", initRequest);

        var result = await _multipartUploadService.AbortMultipartUploadAsync(
            "abort-bucket", "abort.bin", initResult.UploadId);

        Assert.True(result);

        var uploads = await _multipartUploadService.ListMultipartUploadsAsync("abort-bucket");
        Assert.DoesNotContain(uploads, u => u.UploadId == initResult.UploadId);
    }

    [Fact]
    public async Task AbortMultipartUploadAsync_NonExistingUpload_ReturnsFalse()
    {
        var result = await _multipartUploadService.AbortMultipartUploadAsync(
            "any-bucket", "any-file", "non-existing-id");

        Assert.False(result);
    }

    [Fact]
    public async Task ListPartsAsync_ReturnsUploadedParts()
    {
        await _bucketService.CreateBucketAsync("list-parts-bucket");
        var initRequest = new InitiateMultipartUploadRequest { Key = "list-parts.bin" };
        var initResult = await _multipartUploadService.InitiateMultipartUploadAsync("list-parts-bucket", initRequest);

        await _multipartUploadService.UploadPartAsync(
            "list-parts-bucket", "list-parts.bin", initResult.UploadId, 1, new byte[100]);
        await _multipartUploadService.UploadPartAsync(
            "list-parts-bucket", "list-parts.bin", initResult.UploadId, 2, new byte[200]);

        var parts = await _multipartUploadService.ListPartsAsync(
            "list-parts-bucket", "list-parts.bin", initResult.UploadId);

        Assert.NotNull(parts);
        Assert.Equal(2, parts.Count);
        Assert.Equal(1, parts[0].PartNumber);
        Assert.Equal(100, parts[0].Size);
        Assert.NotEmpty(parts[0].ETag);
        Assert.NotEqual(default(DateTime), parts[0].LastModified);
        Assert.Equal(2, parts[1].PartNumber);
        Assert.Equal(200, parts[1].Size);
        Assert.NotEmpty(parts[1].ETag);
        Assert.NotEqual(default(DateTime), parts[1].LastModified);
    }

    [Fact]
    public async Task ListMultipartUploadsAsync_ReturnsActiveUploads()
    {
        await _bucketService.CreateBucketAsync("list-uploads-bucket");

        var upload1 = await _multipartUploadService.InitiateMultipartUploadAsync(
            "list-uploads-bucket", new InitiateMultipartUploadRequest { Key = "file1.bin" });
        var upload2 = await _multipartUploadService.InitiateMultipartUploadAsync(
            "list-uploads-bucket", new InitiateMultipartUploadRequest { Key = "file2.bin" });

        var uploads = await _multipartUploadService.ListMultipartUploadsAsync("list-uploads-bucket");

        Assert.NotNull(uploads);
        Assert.True(uploads.Count >= 2);
        Assert.Contains(uploads, u => u.UploadId == upload1.UploadId && u.Key == "file1.bin");
        Assert.Contains(uploads, u => u.UploadId == upload2.UploadId && u.Key == "file2.bin");

        foreach (var upload in uploads)
        {
            Assert.NotEmpty(upload.UploadId);
            Assert.NotEmpty(upload.Key);
            Assert.NotEqual(default(DateTime), upload.Initiated);
        }
    }

    [Fact]
    public async Task UploadPartAsync_OverwritesPart_UpdatesData()
    {
        await _bucketService.CreateBucketAsync("overwrite-bucket");
        var initRequest = new InitiateMultipartUploadRequest { Key = "overwrite.bin" };
        var initResult = await _multipartUploadService.InitiateMultipartUploadAsync("overwrite-bucket", initRequest);

        var originalData = Encoding.UTF8.GetBytes("Original");
        var updatedData = Encoding.UTF8.GetBytes("Updated");

        await _multipartUploadService.UploadPartAsync(
            "overwrite-bucket", "overwrite.bin", initResult.UploadId, 1, originalData);
        var updatedPart = await _multipartUploadService.UploadPartAsync(
            "overwrite-bucket", "overwrite.bin", initResult.UploadId, 1, updatedData);

        var parts = await _multipartUploadService.ListPartsAsync(
            "overwrite-bucket", "overwrite.bin", initResult.UploadId);

        Assert.Single(parts);
        Assert.Equal(updatedData.Length, parts[0].Size);
        Assert.Equal(updatedPart.ETag, parts[0].ETag);
    }
}