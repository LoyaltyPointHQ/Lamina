using Lamina.Configuration;
using Lamina.Models;
using Lamina.Storage.Abstract;
using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.InMemory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.InteropServices;

namespace Lamina.Tests.Storage;

public class XattrObjectMetadataStorageTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly XattrObjectMetadataStorage _storage;
    private readonly IBucketStorageFacade _bucketStorage;

    public XattrObjectMetadataStorageTests()
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
        var logger = loggerFactory.CreateLogger<XattrObjectMetadataStorage>();

        // Create bucket storage for testing
        var bucketDataStorage = new InMemoryBucketDataStorage();
        var bucketMetadataStorage = new InMemoryBucketMetadataStorage(bucketDataStorage);
        _bucketStorage = new BucketStorageFacade(bucketDataStorage, bucketMetadataStorage, Options.Create(new BucketDefaultsSettings()), loggerFactory.CreateLogger<BucketStorageFacade>());

        _storage = new XattrObjectMetadataStorage(
            Options.Create(settings),
            _bucketStorage,
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
            var logger = loggerFactory.CreateLogger<XattrObjectMetadataStorage>();

            var bucketDataStorage = new InMemoryBucketDataStorage();
            var bucketMetadataStorage = new InMemoryBucketMetadataStorage(bucketDataStorage);
            var bucketStorage = new BucketStorageFacade(bucketDataStorage, bucketMetadataStorage, Options.Create(new BucketDefaultsSettings()), loggerFactory.CreateLogger<BucketStorageFacade>());

            Assert.Throws<NotSupportedException>(() => new XattrObjectMetadataStorage(
                Options.Create(settings),
                bucketStorage,
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
    public async Task StoreMetadataAsync_WithValidData_ReturnsS3Object()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object";
        const string etag = "test-etag";
        const long size = 1024;

        await _bucketStorage.CreateBucketAsync(bucketName);

        // Create a test data file
        var dataPath = Path.Combine(_testDirectory, bucketName, key);
        Directory.CreateDirectory(Path.GetDirectoryName(dataPath)!);
        await File.WriteAllTextAsync(dataPath, "test data");

        var request = new PutObjectRequest
        {
            Key = key,
            ContentType = "text/plain",
            Metadata = new Dictionary<string, string> { { "custom-key", "custom-value" } }
        };

        // Act
        var result = await _storage.StoreMetadataAsync(bucketName, key, etag, size, request);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(key, result.Key);
        Assert.Equal(bucketName, result.BucketName);
        Assert.Equal(etag, result.ETag);
        Assert.Equal(size, result.Size);
        Assert.Equal("text/plain", result.ContentType);
        Assert.Contains("custom-key", result.Metadata);
        Assert.Equal("custom-value", result.Metadata["custom-key"]);
    }

    [Fact]
    public async Task GetMetadataAsync_WithStoredMetadata_ReturnsObjectInfo()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object";
        const string etag = "test-etag";
        const long size = 1024;

        await _bucketStorage.CreateBucketAsync(bucketName);

        // Create and store metadata
        var dataPath = Path.Combine(_testDirectory, bucketName, key);
        Directory.CreateDirectory(Path.GetDirectoryName(dataPath)!);
        await File.WriteAllTextAsync(dataPath, "test data");

        var request = new PutObjectRequest
        {
            Key = key,
            ContentType = "application/json",
            Metadata = new Dictionary<string, string> { { "author", "test-user" } }
        };

        await _storage.StoreMetadataAsync(bucketName, key, etag, size, request);

        // Act
        var result = await _storage.GetMetadataAsync(bucketName, key);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(key, result.Key);
        Assert.Equal(etag, result.ETag);
        Assert.Equal("application/json", result.ContentType);
        Assert.Contains("author", result.Metadata);
        Assert.Equal("test-user", result.Metadata["author"]);
    }

    [Fact]
    public async Task GetMetadataAsync_WithNoMetadata_ReturnsNull()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object";

        await _bucketStorage.CreateBucketAsync(bucketName);

        // Create data file without metadata
        var dataPath = Path.Combine(_testDirectory, bucketName, key);
        Directory.CreateDirectory(Path.GetDirectoryName(dataPath)!);
        await File.WriteAllTextAsync(dataPath, "test data");

        // Act
        var result = await _storage.GetMetadataAsync(bucketName, key);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task DeleteMetadataAsync_WithExistingMetadata_ReturnsTrue()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object";
        const string etag = "test-etag";
        const long size = 1024;

        await _bucketStorage.CreateBucketAsync(bucketName);

        var dataPath = Path.Combine(_testDirectory, bucketName, key);
        Directory.CreateDirectory(Path.GetDirectoryName(dataPath)!);
        await File.WriteAllTextAsync(dataPath, "test data");

        var request = new PutObjectRequest { Key = key, ContentType = "text/plain" };
        await _storage.StoreMetadataAsync(bucketName, key, etag, size, request);

        // Act
        var result = await _storage.DeleteMetadataAsync(bucketName, key);

        // Assert
        Assert.True(result);

        // Verify metadata is gone
        var metadata = await _storage.GetMetadataAsync(bucketName, key);
        Assert.Null(metadata);
    }

    [Fact]
    public async Task MetadataExistsAsync_WithStoredMetadata_ReturnsTrue()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object";
        const string etag = "test-etag";
        const long size = 1024;

        await _bucketStorage.CreateBucketAsync(bucketName);

        var dataPath = Path.Combine(_testDirectory, bucketName, key);
        Directory.CreateDirectory(Path.GetDirectoryName(dataPath)!);
        await File.WriteAllTextAsync(dataPath, "test data");

        var request = new PutObjectRequest { Key = key };
        await _storage.StoreMetadataAsync(bucketName, key, etag, size, request);

        // Act
        var result = await _storage.MetadataExistsAsync(bucketName, key);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task MetadataExistsAsync_WithoutMetadata_ReturnsFalse()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object";

        await _bucketStorage.CreateBucketAsync(bucketName);

        var dataPath = Path.Combine(_testDirectory, bucketName, key);
        Directory.CreateDirectory(Path.GetDirectoryName(dataPath)!);
        await File.WriteAllTextAsync(dataPath, "test data");

        // Act
        var result = await _storage.MetadataExistsAsync(bucketName, key);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void IsValidObjectKey_WithValidKey_ReturnsTrue()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Act & Assert
        Assert.True(_storage.IsValidObjectKey("valid-key"));
        Assert.True(_storage.IsValidObjectKey("path/to/object"));
        Assert.True(_storage.IsValidObjectKey("file.txt"));
    }

    [Fact]
    public void IsValidObjectKey_WithInvalidKey_ReturnsFalse()
    {
        // Skip test on unsupported platforms
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        // Act & Assert
        Assert.False(_storage.IsValidObjectKey(""));
        Assert.False(_storage.IsValidObjectKey(" "));
        Assert.False(_storage.IsValidObjectKey(null!));
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