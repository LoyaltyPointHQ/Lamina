using Lamina.Core.Models;
using Microsoft.EntityFrameworkCore;
using Lamina.Storage.Sql;
using Lamina.Storage.Sql.Context;

namespace Lamina.Storage.Sql.Tests;

public class SqlBucketMetadataStorageTests : IDisposable
{
    private readonly LaminaDbContext _context;
    private readonly SqlBucketMetadataStorage _storage;

    public SqlBucketMetadataStorageTests()
    {
        var options = new DbContextOptionsBuilder<LaminaDbContext>()
            .UseSqlite("Data Source=:memory:")
            .Options;

        _context = new LaminaDbContext(options);
        _context.Database.OpenConnection();
        _context.Database.EnsureCreated();
        _storage = new SqlBucketMetadataStorage(_context);
    }

    [Fact]
    public async Task StoreBucketMetadataAsync_NewBucket_ReturnsCreatedBucket()
    {
        // Arrange
        var bucketName = "test-bucket";
        var request = new CreateBucketRequest
        {
            Type = BucketType.GeneralPurpose,
            StorageClass = "STANDARD"
        };

        // Act
        var result = await _storage.StoreBucketMetadataAsync(bucketName, request);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(bucketName, result.Name);
        Assert.Equal(BucketType.GeneralPurpose, result.Type);
        Assert.Equal("STANDARD", result.StorageClass);
        Assert.True(result.CreationDate <= DateTime.UtcNow);
    }

    [Fact]
    public async Task StoreBucketMetadataAsync_DuplicateBucket_ReturnsNull()
    {
        // Arrange
        var bucketName = "test-bucket";
        var request = new CreateBucketRequest { Type = BucketType.Directory };

        // Create first bucket
        await _storage.StoreBucketMetadataAsync(bucketName, request);

        // Act
        var result = await _storage.StoreBucketMetadataAsync(bucketName, request);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task GetBucketMetadataAsync_ExistingBucket_ReturnsBucket()
    {
        // Arrange
        var bucketName = "test-bucket";
        var request = new CreateBucketRequest { Type = BucketType.GeneralPurpose };
        var created = await _storage.StoreBucketMetadataAsync(bucketName, request);

        // Act
        var result = await _storage.GetBucketMetadataAsync(bucketName);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(bucketName, result.Name);
        Assert.Equal(BucketType.GeneralPurpose, result.Type);
        Assert.Equal(created!.CreationDate, result.CreationDate);
    }

    [Fact]
    public async Task GetBucketMetadataAsync_NonExistentBucket_ReturnsNull()
    {
        // Act
        var result = await _storage.GetBucketMetadataAsync("non-existent");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task GetAllBucketsMetadataAsync_MultipleBuckets_ReturnsAllBuckets()
    {
        // Arrange
        var buckets = new[]
        {
            new { Name = "bucket-1", Type = BucketType.Directory },
            new { Name = "bucket-2", Type = BucketType.GeneralPurpose },
            new { Name = "bucket-3", Type = BucketType.Directory }
        };

        foreach (var bucket in buckets)
        {
            await _storage.StoreBucketMetadataAsync(bucket.Name, new CreateBucketRequest { Type = bucket.Type });
        }

        // Act
        var result = await _storage.GetAllBucketsMetadataAsync();

        // Assert
        Assert.Equal(3, result.Count);
        Assert.Contains(result, b => b.Name == "bucket-1" && b.Type == BucketType.Directory);
        Assert.Contains(result, b => b.Name == "bucket-2" && b.Type == BucketType.GeneralPurpose);
        Assert.Contains(result, b => b.Name == "bucket-3" && b.Type == BucketType.Directory);
    }

    [Fact]
    public async Task DeleteBucketMetadataAsync_ExistingBucket_ReturnsTrue()
    {
        // Arrange
        var bucketName = "test-bucket";
        await _storage.StoreBucketMetadataAsync(bucketName, new CreateBucketRequest());

        // Act
        var result = await _storage.DeleteBucketMetadataAsync(bucketName);

        // Assert
        Assert.True(result);

        // Verify deletion
        var bucket = await _storage.GetBucketMetadataAsync(bucketName);
        Assert.Null(bucket);
    }

    [Fact]
    public async Task DeleteBucketMetadataAsync_NonExistentBucket_ReturnsFalse()
    {
        // Act
        var result = await _storage.DeleteBucketMetadataAsync("non-existent");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task UpdateBucketTagsAsync_ExistingBucket_UpdatesTags()
    {
        // Arrange
        var bucketName = "test-bucket";
        await _storage.StoreBucketMetadataAsync(bucketName, new CreateBucketRequest());

        var tags = new Dictionary<string, string>
        {
            { "Environment", "Test" },
            { "Project", "Lamina" }
        };

        // Act
        var result = await _storage.UpdateBucketTagsAsync(bucketName, tags);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(2, result.Tags.Count);
        Assert.Equal("Test", result.Tags["Environment"]);
        Assert.Equal("Lamina", result.Tags["Project"]);

        // Verify persistence
        var bucket = await _storage.GetBucketMetadataAsync(bucketName);
        Assert.Equal(2, bucket!.Tags.Count);
        Assert.Equal("Test", bucket.Tags["Environment"]);
    }

    [Fact]
    public async Task UpdateBucketTagsAsync_NonExistentBucket_ReturnsNull()
    {
        // Arrange
        var tags = new Dictionary<string, string> { { "key", "value" } };

        // Act
        var result = await _storage.UpdateBucketTagsAsync("non-existent", tags);

        // Assert
        Assert.Null(result);
    }

    public void Dispose()
    {
        _context.Database.CloseConnection();
        _context.Dispose();
    }
}