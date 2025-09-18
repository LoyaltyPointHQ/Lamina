using S3Test.Models;
using S3Test.Services;
using System.Xml.Serialization;
using System.Text;

namespace S3Test.Tests.Services;

public class BucketServiceTests
{
    private readonly IBucketService _bucketService;

    public BucketServiceTests()
    {
        _bucketService = new InMemoryBucketService();
    }

    [Fact]
    public async Task CreateBucketAsync_ValidRequest_CreatesBucket()
    {
        var request = new CreateBucketRequest
        {
            Region = "us-west-2"
        };

        var result = await _bucketService.CreateBucketAsync("test-bucket", request);

        Assert.NotNull(result);
        Assert.Equal("test-bucket", result.Name);
        Assert.Equal("us-west-2", result.Region);
        Assert.NotEqual(default(DateTime), result.CreationDate);
    }

    [Fact]
    public async Task CreateBucketAsync_DuplicateName_ReturnsNull()
    {
        var firstResult = await _bucketService.CreateBucketAsync("duplicate-bucket");
        var secondResult = await _bucketService.CreateBucketAsync("duplicate-bucket");

        Assert.NotNull(firstResult);
        Assert.Null(secondResult);
    }

    [Fact]
    public async Task CreateBucketAsync_EmptyName_ReturnsNull()
    {
        var result = await _bucketService.CreateBucketAsync("");

        Assert.Null(result);
    }

    [Fact]
    public async Task GetBucketAsync_ExistingBucket_ReturnsBucket()
    {
        await _bucketService.CreateBucketAsync("get-test-bucket");

        var result = await _bucketService.GetBucketAsync("get-test-bucket");

        Assert.NotNull(result);
        Assert.Equal("get-test-bucket", result.Name);
    }

    [Fact]
    public async Task GetBucketAsync_NonExistingBucket_ReturnsNull()
    {
        var result = await _bucketService.GetBucketAsync("non-existing-bucket");

        Assert.Null(result);
    }

    [Fact]
    public async Task ListBucketsAsync_ReturnsAllBuckets()
    {
        await _bucketService.CreateBucketAsync("bucket-1");
        await _bucketService.CreateBucketAsync("bucket-2");
        await _bucketService.CreateBucketAsync("bucket-3");

        var result = await _bucketService.ListBucketsAsync();

        Assert.NotNull(result);
        Assert.True(result.Buckets.Count >= 3);
        Assert.Contains(result.Buckets, b => b.Name == "bucket-1");
        Assert.Contains(result.Buckets, b => b.Name == "bucket-2");
        Assert.Contains(result.Buckets, b => b.Name == "bucket-3");

        foreach (var bucket in result.Buckets)
        {
            Assert.NotEqual(default(DateTime), bucket.CreationDate);
        }
    }

    [Fact]
    public async Task DeleteBucketAsync_ExistingBucket_ReturnsTrue()
    {
        await _bucketService.CreateBucketAsync("delete-bucket");

        var deleteResult = await _bucketService.DeleteBucketAsync("delete-bucket");
        var getResult = await _bucketService.GetBucketAsync("delete-bucket");

        Assert.True(deleteResult);
        Assert.Null(getResult);
    }

    [Fact]
    public async Task DeleteBucketAsync_NonExistingBucket_ReturnsFalse()
    {
        var result = await _bucketService.DeleteBucketAsync("non-existing-bucket-delete");

        Assert.False(result);
    }

    [Fact]
    public async Task BucketExistsAsync_ExistingBucket_ReturnsTrue()
    {
        await _bucketService.CreateBucketAsync("exists-bucket");

        var result = await _bucketService.BucketExistsAsync("exists-bucket");

        Assert.True(result);
    }

    [Fact]
    public async Task BucketExistsAsync_NonExistingBucket_ReturnsFalse()
    {
        var result = await _bucketService.BucketExistsAsync("non-existing-bucket-exists");

        Assert.False(result);
    }

    [Fact]
    public async Task UpdateBucketTagsAsync_ExistingBucket_UpdatesTags()
    {
        await _bucketService.CreateBucketAsync("tags-bucket");
        var tags = new Dictionary<string, string>
        {
            { "Environment", "Test" },
            { "Owner", "TestUser" }
        };

        var result = await _bucketService.UpdateBucketTagsAsync("tags-bucket", tags);

        Assert.NotNull(result);
        Assert.Equal(2, result.Tags.Count);
        Assert.Equal("Test", result.Tags["Environment"]);
        Assert.Equal("TestUser", result.Tags["Owner"]);
    }

    [Fact]
    public async Task UpdateBucketTagsAsync_NonExistingBucket_ReturnsNull()
    {
        var tags = new Dictionary<string, string> { { "Key", "Value" } };

        var result = await _bucketService.UpdateBucketTagsAsync("non-existing-bucket-tags", tags);

        Assert.Null(result);
    }
}