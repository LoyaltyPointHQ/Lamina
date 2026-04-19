using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.InMemory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Runtime.InteropServices;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Moq;

namespace Lamina.Storage.Filesystem.Tests;

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

        var networkHelper = new NetworkFileSystemHelper(Options.Create(settings), NullLogger<NetworkFileSystemHelper>.Instance);
        var dataStorage = new FilesystemObjectDataStorage(
            Options.Create(settings),
            networkHelper,
            new LinuxZeroCopyHelper(NullLogger<LinuxZeroCopyHelper>.Instance),
            NullLogger<FilesystemObjectDataStorage>.Instance,
            Mock.Of<IChunkedDataParser>());

        _storage = new XattrObjectMetadataStorage(
            Options.Create(settings),
            _bucketStorage,
            dataStorage,
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

            var networkHelper = new NetworkFileSystemHelper(Options.Create(settings), NullLogger<NetworkFileSystemHelper>.Instance);
            var dataStorage = new FilesystemObjectDataStorage(
                Options.Create(settings),
                networkHelper,
                new LinuxZeroCopyHelper(NullLogger<LinuxZeroCopyHelper>.Instance),
                NullLogger<FilesystemObjectDataStorage>.Instance,
                Mock.Of<IChunkedDataParser>());

            Assert.Throws<NotSupportedException>(() => new XattrObjectMetadataStorage(
                Options.Create(settings),
                bucketStorage,
                dataStorage,
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
        var result = await _storage.StoreMetadataAsync(bucketName, key, etag, size, request, null);

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

        await _storage.StoreMetadataAsync(bucketName, key, etag, size, request, null);

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
        await _storage.StoreMetadataAsync(bucketName, key, etag, size, request, null);

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
        await _storage.StoreMetadataAsync(bucketName, key, etag, size, request, null);

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

    [Fact]
    public async Task GetMetadataAsync_DataModifiedAfterStore_RecomputesETag()
    {
        // Data-first scenario: an external writer touches the file after metadata was stored.
        // The recorded xattr timestamp lags behind the file mtime - xattr layer must detect it
        // and recompute the ETag rather than return the stale one.
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) return;

        var bucketName = "test-bucket";
        var key = "stale.bin";
        var originalContent = "aaa"u8.ToArray();
        var modifiedContent = "bbbb"u8.ToArray();

        Directory.CreateDirectory(Path.Combine(_testDirectory, bucketName));
        var dataPath = Path.Combine(_testDirectory, bucketName, key);
        await File.WriteAllBytesAsync(dataPath, originalContent);

        await _bucketStorage.CreateBucketAsync(bucketName);
        const string storedEtag = "etag-of-aaa";
        await _storage.StoreMetadataAsync(bucketName, key, storedEtag, originalContent.Length);

        // Simulate an external write: replace content AND bump mtime forward.
        await Task.Delay(50);
        await File.WriteAllBytesAsync(dataPath, modifiedContent);
        File.SetLastWriteTimeUtc(dataPath, DateTime.UtcNow.AddSeconds(10));

        var result = await _storage.GetMetadataAsync(bucketName, key);

        Assert.NotNull(result);
        Assert.NotEqual(storedEtag, result.ETag); // ETag was recomputed from the modified content
    }

    [Fact]
    public async Task GetMetadataAsync_DataModifiedButMultipartETag_PreservesETag()
    {
        // Multipart ETag formula needs per-part MD5s and cannot be rebuilt from the merged
        // bytes, so even when the file mtime says the data moved, the stored ETag must survive.
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) return;

        var bucketName = "test-bucket";
        var key = "mp.bin";
        var originalContent = new byte[] { 1, 2, 3, 4 };

        Directory.CreateDirectory(Path.Combine(_testDirectory, bucketName));
        var dataPath = Path.Combine(_testDirectory, bucketName, key);
        await File.WriteAllBytesAsync(dataPath, originalContent);

        await _bucketStorage.CreateBucketAsync(bucketName);
        const string multipartEtag = "deadbeefdeadbeefdeadbeefdeadbeef-3";
        await _storage.StoreMetadataAsync(bucketName, key, multipartEtag, originalContent.Length);

        await Task.Delay(50);
        File.SetLastWriteTimeUtc(dataPath, DateTime.UtcNow.AddSeconds(10));

        var result = await _storage.GetMetadataAsync(bucketName, key);

        Assert.NotNull(result);
        Assert.Equal(multipartEtag, result.ETag);
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