using Lamina.Core.Models;
using Microsoft.EntityFrameworkCore;
using Lamina.Storage.Sql;
using Lamina.Storage.Sql.Context;

namespace Lamina.Tests.Storage.Sql;

public class SqlObjectMetadataStorageTests : IDisposable
{
    private readonly LaminaDbContext _context;
    private readonly SqlObjectMetadataStorage _storage;

    public SqlObjectMetadataStorageTests()
    {
        var options = new DbContextOptionsBuilder<LaminaDbContext>()
            .UseSqlite("Data Source=:memory:")
            .Options;

        _context = new LaminaDbContext(options);
        _context.Database.OpenConnection();
        _context.Database.EnsureCreated();
        _storage = new SqlObjectMetadataStorage(_context);
    }

    [Fact]
    public async Task StoreMetadataAsync_NewObject_ReturnsS3Object()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var etag = "test-etag";
        var size = 1024L;
        var request = new PutObjectRequest
        {
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string> { { "custom", "value" } }
        };

        // Act
        var result = await _storage.StoreMetadataAsync(bucketName, key, etag, size, request);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(bucketName, result.BucketName);
        Assert.Equal(key, result.Key);
        Assert.Equal(etag, result.ETag);
        Assert.Equal(size, result.Size);
        Assert.Equal("text/plain", result.ContentType);
        Assert.Single(result.Metadata);
        Assert.Equal("value", result.Metadata["custom"]);
    }

    [Fact]
    public async Task StoreMetadataAsync_ExistingObject_UpdatesObject()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var etag1 = "etag-1";
        var etag2 = "etag-2";
        var size1 = 1024L;
        var size2 = 2048L;

        // Create initial object
        await _storage.StoreMetadataAsync(bucketName, key, etag1, size1);

        // Act
        var result = await _storage.StoreMetadataAsync(bucketName, key, etag2, size2);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(etag2, result.ETag);
        Assert.Equal(size2, result.Size);

        // Verify only one object exists
        var metadata = await _storage.GetMetadataAsync(bucketName, key);
        Assert.NotNull(metadata);
        Assert.Equal(etag2, metadata.ETag);
        Assert.Equal(size2, metadata.Size);
    }

    [Fact]
    public async Task GetMetadataAsync_ExistingObject_ReturnsMetadata()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var etag = "test-etag";
        var size = 1024L;
        await _storage.StoreMetadataAsync(bucketName, key, etag, size);

        // Act
        var result = await _storage.GetMetadataAsync(bucketName, key);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(key, result.Key);
        Assert.Equal(etag, result.ETag);
        Assert.Equal(size, result.Size);
    }

    [Fact]
    public async Task GetMetadataAsync_NonExistentObject_ReturnsNull()
    {
        // Act
        var result = await _storage.GetMetadataAsync("bucket", "non-existent");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task DeleteMetadataAsync_ExistingObject_ReturnsTrue()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        await _storage.StoreMetadataAsync(bucketName, key, "etag", 1024L);

        // Act
        var result = await _storage.DeleteMetadataAsync(bucketName, key);

        // Assert
        Assert.True(result);

        // Verify deletion
        var metadata = await _storage.GetMetadataAsync(bucketName, key);
        Assert.Null(metadata);
    }

    [Fact]
    public async Task DeleteMetadataAsync_NonExistentObject_ReturnsFalse()
    {
        // Act
        var result = await _storage.DeleteMetadataAsync("bucket", "non-existent");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task MetadataExistsAsync_ExistingObject_ReturnsTrue()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        await _storage.StoreMetadataAsync(bucketName, key, "etag", 1024L);

        // Act
        var result = await _storage.MetadataExistsAsync(bucketName, key);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task MetadataExistsAsync_NonExistentObject_ReturnsFalse()
    {
        // Act
        var result = await _storage.MetadataExistsAsync("bucket", "non-existent");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ListAllMetadataKeysAsync_MultipleObjects_ReturnsAllKeys()
    {
        // Arrange
        var objects = new[]
        {
            new { Bucket = "bucket1", Key = "key1" },
            new { Bucket = "bucket1", Key = "key2" },
            new { Bucket = "bucket2", Key = "key1" }
        };

        foreach (var obj in objects)
        {
            await _storage.StoreMetadataAsync(obj.Bucket, obj.Key, "etag", 1024L);
        }

        // Act
        var result = new List<(string bucketName, string key)>();
        await foreach (var item in _storage.ListAllMetadataKeysAsync())
        {
            result.Add(item);
        }

        // Assert
        Assert.Equal(3, result.Count);
        Assert.Contains(result, item => item.bucketName == "bucket1" && item.key == "key1");
        Assert.Contains(result, item => item.bucketName == "bucket1" && item.key == "key2");
        Assert.Contains(result, item => item.bucketName == "bucket2" && item.key == "key1");
    }

    [Theory]
    [InlineData("valid-key", true)]
    [InlineData("path/to/object", true)]
    [InlineData("", false)]
    [InlineData("/starts-with-slash", false)]
    [InlineData("contains\0null", false)]
    [InlineData("contains\rnewline", false)]
    public void IsValidObjectKey_VariousKeys_ReturnsExpected(string key, bool expected)
    {
        // Act
        var result = _storage.IsValidObjectKey(key);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void IsValidObjectKey_TooLongKey_ReturnsFalse()
    {
        // Arrange
        var longKey = new string('a', 1025);

        // Act
        var result = _storage.IsValidObjectKey(longKey);

        // Assert
        Assert.False(result);
    }

    public void Dispose()
    {
        _context.Database.CloseConnection();
        _context.Dispose();
    }
}