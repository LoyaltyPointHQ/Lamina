using Lamina.Core.Models;
using Microsoft.EntityFrameworkCore;
using Lamina.Storage.Sql;
using Lamina.Storage.Sql.Context;

namespace Lamina.Tests.Storage.Sql;

public class SqlMultipartUploadMetadataStorageTests : IDisposable
{
    private readonly LaminaDbContext _context;
    private readonly SqlMultipartUploadMetadataStorage _storage;

    public SqlMultipartUploadMetadataStorageTests()
    {
        var options = new DbContextOptionsBuilder<LaminaDbContext>()
            .UseSqlite("Data Source=:memory:")
            .Options;

        _context = new LaminaDbContext(options);
        _context.Database.OpenConnection();
        _context.Database.EnsureCreated();
        _storage = new SqlMultipartUploadMetadataStorage(_context);
    }

    [Fact]
    public async Task InitiateUploadAsync_NewUpload_ReturnsMultipartUpload()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var request = new InitiateMultipartUploadRequest
        {
            Key = key,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string> { { "custom", "value" } }
        };

        // Act
        var result = await _storage.InitiateUploadAsync(bucketName, key, request);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(bucketName, result.BucketName);
        Assert.Equal(key, result.Key);
        Assert.False(string.IsNullOrEmpty(result.UploadId));
        Assert.Equal("text/plain", result.ContentType);
        Assert.Single(result.Metadata);
        Assert.Equal("value", result.Metadata["custom"]);
        Assert.True(result.Initiated <= DateTime.UtcNow);
        Assert.Empty(result.Parts);
    }

    [Fact]
    public async Task GetUploadMetadataAsync_ExistingUpload_ReturnsUpload()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var request = new InitiateMultipartUploadRequest { Key = key };
        var initiated = await _storage.InitiateUploadAsync(bucketName, key, request);

        // Act
        var result = await _storage.GetUploadMetadataAsync(bucketName, key, initiated.UploadId);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(bucketName, result.BucketName);
        Assert.Equal(key, result.Key);
        Assert.Equal(initiated.UploadId, result.UploadId);
        Assert.Equal(initiated.Initiated, result.Initiated);
    }

    [Fact]
    public async Task GetUploadMetadataAsync_NonExistentUpload_ReturnsNull()
    {
        // Act
        var result = await _storage.GetUploadMetadataAsync("bucket", "key", "non-existent-id");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task DeleteUploadMetadataAsync_ExistingUpload_ReturnsTrue()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var request = new InitiateMultipartUploadRequest { Key = key };
        var initiated = await _storage.InitiateUploadAsync(bucketName, key, request);

        // Act
        var result = await _storage.DeleteUploadMetadataAsync(bucketName, key, initiated.UploadId);

        // Assert
        Assert.True(result);

        // Verify deletion
        var upload = await _storage.GetUploadMetadataAsync(bucketName, key, initiated.UploadId);
        Assert.Null(upload);
    }

    [Fact]
    public async Task DeleteUploadMetadataAsync_NonExistentUpload_ReturnsFalse()
    {
        // Act
        var result = await _storage.DeleteUploadMetadataAsync("bucket", "key", "non-existent-id");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ListUploadsAsync_MultipleBuckets_ReturnsOnlyBucketUploads()
    {
        // Arrange
        var bucket1 = "bucket1";
        var bucket2 = "bucket2";
        var request = new InitiateMultipartUploadRequest { Key = "key" };

        await _storage.InitiateUploadAsync(bucket1, "key1", request);
        await _storage.InitiateUploadAsync(bucket1, "key2", request);
        await _storage.InitiateUploadAsync(bucket2, "key1", request);

        // Act
        var result = await _storage.ListUploadsAsync(bucket1);

        // Assert
        Assert.Equal(2, result.Count);
        Assert.All(result, upload => Assert.Equal(bucket1, upload.BucketName));
    }

    [Fact]
    public async Task UploadExistsAsync_ExistingUpload_ReturnsTrue()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var request = new InitiateMultipartUploadRequest { Key = key };
        var initiated = await _storage.InitiateUploadAsync(bucketName, key, request);

        // Act
        var result = await _storage.UploadExistsAsync(bucketName, key, initiated.UploadId);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task UploadExistsAsync_NonExistentUpload_ReturnsFalse()
    {
        // Act
        var result = await _storage.UploadExistsAsync("bucket", "key", "non-existent-id");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ListUploadsAsync_EmptyBucket_ReturnsEmptyList()
    {
        // Act
        var result = await _storage.ListUploadsAsync("empty-bucket");

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public async Task ListUploadsAsync_OrderedByInitiated()
    {
        // Arrange
        var bucketName = "test-bucket";
        var request = new InitiateMultipartUploadRequest { Key = "key" };

        var upload1 = await _storage.InitiateUploadAsync(bucketName, "key1", request);
        await Task.Delay(10); // Ensure different timestamps
        var upload2 = await _storage.InitiateUploadAsync(bucketName, "key2", request);
        await Task.Delay(10); // Ensure different timestamps
        var upload3 = await _storage.InitiateUploadAsync(bucketName, "key3", request);

        // Act
        var result = await _storage.ListUploadsAsync(bucketName);

        // Assert
        Assert.Equal(3, result.Count);
        Assert.True(result[0].Initiated <= result[1].Initiated);
        Assert.True(result[1].Initiated <= result[2].Initiated);
    }

    public void Dispose()
    {
        _context.Database.CloseConnection();
        _context.Dispose();
    }
}