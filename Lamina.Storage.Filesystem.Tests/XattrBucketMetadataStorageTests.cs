using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.InMemory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.InteropServices;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;

namespace Lamina.Storage.Filesystem.Tests;

public class XattrBucketMetadataStorageTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly XattrBucketMetadataStorage _storage;
    private readonly IBucketDataStorage _bucketDataStorage;

    public XattrBucketMetadataStorageTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);

        var settings = new FilesystemStorageSettings
        {
            DataDirectory = _testDirectory,
            MetadataMode = MetadataStorageMode.Xattr,
            XattrPrefix = "user.lamina-test"
        };

        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        var logger = loggerFactory.CreateLogger<XattrBucketMetadataStorage>();

        // Create bucket data storage for testing
        _bucketDataStorage = new FilesystemBucketDataStorage(
            Options.Create(settings),
            loggerFactory.CreateLogger<FilesystemBucketDataStorage>());

        _storage = new XattrBucketMetadataStorage(
            Options.Create(settings),
            _bucketDataStorage,
            logger,
            loggerFactory);
    }

    [Fact]
    public void Constructor_OnUnsupportedPlatform_ThrowsNotSupportedException()
    {
        // This test will only pass on unsupported platforms (Windows)
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            var settings = new FilesystemStorageSettings
            {
                DataDirectory = _testDirectory,
                MetadataMode = MetadataStorageMode.Xattr
            };

            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var logger = loggerFactory.CreateLogger<XattrBucketMetadataStorage>();

            var bucketDataStorage = new FilesystemBucketDataStorage(
                Options.Create(settings),
                loggerFactory.CreateLogger<FilesystemBucketDataStorage>());

            Assert.Throws<NotSupportedException>(() => new XattrBucketMetadataStorage(
                Options.Create(settings),
                bucketDataStorage,
                logger,
                loggerFactory));
        }
        else
        {
            // On supported platforms, constructor should succeed
            Assert.NotNull(_storage);
        }
    }

    [Fact]
    public async Task StoreBucketMetadataAsync_WithValidBucket_ReturnsBucket()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucketName = "test-bucket";
        await _bucketDataStorage.CreateBucketAsync(bucketName);

        var request = new CreateBucketRequest();

        // Act
        var result = await _storage.StoreBucketMetadataAsync(bucketName, request);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(bucketName, result.Name);
        Assert.NotEqual(DateTime.MinValue, result.CreationDate);
        Assert.Empty(result.Tags);
    }

    [Fact]
    public async Task StoreBucketMetadataAsync_WithNonExistentBucket_ReturnsNull()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Act
        var result = await _storage.StoreBucketMetadataAsync("non-existent-bucket", new CreateBucketRequest());

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task GetBucketMetadataAsync_WithStoredMetadata_ReturnsBucket()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucketName = "test-bucket";
        await _bucketDataStorage.CreateBucketAsync(bucketName);

        var request = new CreateBucketRequest();
        await _storage.StoreBucketMetadataAsync(bucketName, request);

        // Act
        var result = await _storage.GetBucketMetadataAsync(bucketName);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(bucketName, result.Name);
        Assert.NotEqual(DateTime.MinValue, result.CreationDate);
        Assert.Empty(result.Tags);
    }

    [Fact]
    public async Task GetBucketMetadataAsync_WithoutStoredMetadata_ReturnsDefaultBucket()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucketName = "test-bucket";
        await _bucketDataStorage.CreateBucketAsync(bucketName);

        // Act (get metadata without storing any)
        var result = await _storage.GetBucketMetadataAsync(bucketName);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(bucketName, result.Name);
        Assert.NotEqual(DateTime.MinValue, result.CreationDate);
        Assert.Empty(result.Tags);
    }

    [Fact]
    public async Task GetBucketMetadataAsync_WithNonExistentBucket_ReturnsNull()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Act
        var result = await _storage.GetBucketMetadataAsync("non-existent-bucket");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task UpdateBucketTagsAsync_WithValidBucket_UpdatesTags()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucketName = "test-bucket";
        await _bucketDataStorage.CreateBucketAsync(bucketName);
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
        Assert.Equal(bucketName, result.Name);
        Assert.Equal(2, result.Tags.Count);
        Assert.Equal("Test", result.Tags["Environment"]);
        Assert.Equal("Lamina", result.Tags["Project"]);
    }

    [Fact]
    public async Task UpdateBucketTagsAsync_WithEmptyTags_ClearsTags()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucketName = "test-bucket";
        await _bucketDataStorage.CreateBucketAsync(bucketName);
        await _storage.StoreBucketMetadataAsync(bucketName, new CreateBucketRequest());

        // First set some tags
        var initialTags = new Dictionary<string, string> { { "ToRemove", "Value" } };
        await _storage.UpdateBucketTagsAsync(bucketName, initialTags);

        // Act - clear tags
        var result = await _storage.UpdateBucketTagsAsync(bucketName, new Dictionary<string, string>());

        // Assert
        Assert.NotNull(result);
        Assert.Empty(result.Tags);
    }

    [Fact]
    public async Task DeleteBucketMetadataAsync_WithExistingBucket_ReturnsTrue()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucketName = "test-bucket";
        await _bucketDataStorage.CreateBucketAsync(bucketName);

        // Act
        var result = await _storage.DeleteBucketMetadataAsync(bucketName);

        // Assert
        Assert.True(result);

        // Verify metadata is gone (should return default values)
        var bucket = await _storage.GetBucketMetadataAsync(bucketName);
        Assert.NotNull(bucket);
    }

    [Fact]
    public async Task DeleteBucketMetadataAsync_WithNonExistentBucket_ReturnsTrue()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Act
        var result = await _storage.DeleteBucketMetadataAsync("non-existent-bucket");

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task GetAllBucketsMetadataAsync_WithMultipleBuckets_ReturnsAllBuckets()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucket1 = "bucket-1";
        const string bucket2 = "bucket-2";

        await _bucketDataStorage.CreateBucketAsync(bucket1);
        await _bucketDataStorage.CreateBucketAsync(bucket2);


        // Act
        var result = await _storage.GetAllBucketsMetadataAsync();

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Contains(result, b => b.Name == bucket1);
        Assert.Contains(result, b => b.Name == bucket2);
    }

    [Fact]
    public async Task TagsIntegration_StoreRetrieveUpdateDelete_WorksCorrectly()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucketName = "test-bucket";
        await _bucketDataStorage.CreateBucketAsync(bucketName);
        await _storage.StoreBucketMetadataAsync(bucketName, new CreateBucketRequest());

        // Act & Assert - Initial state (no tags)
        var bucket = await _storage.GetBucketMetadataAsync(bucketName);
        Assert.Empty(bucket!.Tags);

        // Act & Assert - Add tags
        var tags1 = new Dictionary<string, string>
        {
            { "Environment", "Production" },
            { "Owner", "DevTeam" }
        };
        bucket = await _storage.UpdateBucketTagsAsync(bucketName, tags1);
        Assert.Equal(2, bucket!.Tags.Count);
        Assert.Equal("Production", bucket.Tags["Environment"]);
        Assert.Equal("DevTeam", bucket.Tags["Owner"]);

        // Act & Assert - Update tags (partial update)
        var tags2 = new Dictionary<string, string>
        {
            { "Environment", "Staging" },
            { "Cost-Center", "Engineering" }
        };
        bucket = await _storage.UpdateBucketTagsAsync(bucketName, tags2);
        Assert.Equal(2, bucket!.Tags.Count);
        Assert.Equal("Staging", bucket.Tags["Environment"]);
        Assert.Equal("Engineering", bucket.Tags["Cost-Center"]);
        Assert.False(bucket.Tags.ContainsKey("Owner")); // Should be removed

        // Act & Assert - Clear tags
        bucket = await _storage.UpdateBucketTagsAsync(bucketName, new Dictionary<string, string>());
        Assert.Empty(bucket!.Tags);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
        GC.SuppressFinalize(this);
    }
}