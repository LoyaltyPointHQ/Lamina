using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace Lamina.Storage.Filesystem.Tests;

/// <summary>
/// Tests for stale metadata detection and selective checksum recomputation
/// in FilesystemObjectMetadataStorage.
/// </summary>
public class FilesystemObjectMetadataStorageStaleTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly string _dataDirectory;
    private readonly string _metadataDirectory;
    private readonly FilesystemObjectMetadataStorage _storage;
    private readonly FilesystemObjectDataStorage _dataStorage;
    private readonly Mock<IBucketStorageFacade> _bucketStorageMock;

    public FilesystemObjectMetadataStorageStaleTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"lamina-test-{Guid.NewGuid():N}");
        _dataDirectory = Path.Combine(_testDirectory, "data");
        _metadataDirectory = Path.Combine(_testDirectory, "metadata");

        Directory.CreateDirectory(_dataDirectory);
        Directory.CreateDirectory(_metadataDirectory);

        var settings = new FilesystemStorageSettings
        {
            DataDirectory = _dataDirectory,
            MetadataDirectory = _metadataDirectory,
            MetadataMode = MetadataStorageMode.SeparateDirectory
        };

        _bucketStorageMock = new Mock<IBucketStorageFacade>();
        _bucketStorageMock.Setup(x => x.BucketExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var lockManager = new InMemoryLockManager();
        var networkHelper = new NetworkFileSystemHelper(Options.Create(settings), Mock.Of<ILogger<NetworkFileSystemHelper>>());
        var metadataLogger = Mock.Of<ILogger<FilesystemObjectMetadataStorage>>();

        var cacheSettings = new MetadataCacheSettings { Enabled = false };

        _storage = new FilesystemObjectMetadataStorage(
            Options.Create(settings),
            Options.Create(cacheSettings),
            _bucketStorageMock.Object,
            lockManager,
            networkHelper,
            metadataLogger,
            null);

        var dataLogger = Mock.Of<ILogger<FilesystemObjectDataStorage>>();
        var chunkedDataParser = Mock.Of<Lamina.Core.Streaming.IChunkedDataParser>();

        _dataStorage = new FilesystemObjectDataStorage(
            Options.Create(settings),
            networkHelper,
            dataLogger,
            chunkedDataParser);
    }

    [Fact]
    public async Task GetMetadataAsync_FreshMetadata_ReturnsOriginalChecksums()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-file.bin";
        var testData = "test content"u8.ToArray();

        // Create bucket and write data
        Directory.CreateDirectory(Path.Combine(_dataDirectory, bucketName));
        var dataPath = Path.Combine(_dataDirectory, bucketName, key);
        await File.WriteAllBytesAsync(dataPath, testData);

        // Store metadata with checksums
        var checksums = new Dictionary<string, string>
        {
            { "CRC32", "crc32-value" },
            { "SHA256", "sha256-value" }
        };
        await _storage.StoreMetadataAsync(bucketName, key, "etag-123", testData.Length, null, checksums);

        // Act
        var result = await _storage.GetMetadataAsync(bucketName, key);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("etag-123", result.ETag);
        Assert.Equal("crc32-value", result.ChecksumCRC32);
        Assert.Equal("sha256-value", result.ChecksumSHA256);
        Assert.Null(result.ChecksumCRC32C); // Not stored
    }

    [Fact]
    public async Task GetMetadataAsync_StaleMetadata_RecomputesETagAndSelectiveChecksums()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-file.bin";
        var testData = "test content"u8.ToArray();

        // Create bucket and write data
        Directory.CreateDirectory(Path.Combine(_dataDirectory, bucketName));
        var dataPath = Path.Combine(_dataDirectory, bucketName, key);
        await File.WriteAllBytesAsync(dataPath, testData);

        // Store metadata with specific checksums
        var originalChecksums = new Dictionary<string, string>
        {
            { "CRC32", "old-crc32" },
            { "SHA256", "old-sha256" }
        };
        await _storage.StoreMetadataAsync(bucketName, key, "old-etag", testData.Length, null, originalChecksums);

        // Wait a bit to ensure timestamp difference
        await Task.Delay(100);

        // Modify the file to make metadata stale
        testData = "modified content"u8.ToArray();
        await File.WriteAllBytesAsync(dataPath, testData);

        // Act
        var result = await _storage.GetMetadataAsync(bucketName, key);

        // Assert
        Assert.NotNull(result);
        Assert.NotEqual("old-etag", result.ETag); // ETag was recomputed
        Assert.NotNull(result.ChecksumCRC32); // CRC32 was recomputed (was in original)
        Assert.NotNull(result.ChecksumSHA256); // SHA256 was recomputed (was in original)
        Assert.Null(result.ChecksumCRC32C); // Was not in original, so not computed
        Assert.Null(result.ChecksumCRC64NVME); // Was not in original, so not computed
    }

    [Fact]
    public async Task GetMetadataAsync_StaleMetadataNoChecksums_RecomputesOnlyETag()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-file.bin";
        var testData = "test content"u8.ToArray();

        Directory.CreateDirectory(Path.Combine(_dataDirectory, bucketName));
        var dataPath = Path.Combine(_dataDirectory, bucketName, key);
        await File.WriteAllBytesAsync(dataPath, testData);

        // Store metadata WITHOUT checksums
        await _storage.StoreMetadataAsync(bucketName, key, "old-etag", testData.Length, null, null);

        await Task.Delay(100);

        // Modify file
        testData = "modified content"u8.ToArray();
        await File.WriteAllBytesAsync(dataPath, testData);

        // Act
        var result = await _storage.GetMetadataAsync(bucketName, key);

        // Assert
        Assert.NotNull(result);
        Assert.NotEqual("old-etag", result.ETag); // ETag recomputed
        Assert.Null(result.ChecksumCRC32); // No checksums to recompute
        Assert.Null(result.ChecksumSHA256);
    }

    [Fact]
    public async Task GetMetadataAsync_OrphanedMetadata_ReturnsNull()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-file.bin";

        Directory.CreateDirectory(Path.Combine(_dataDirectory, bucketName));

        // Create metadata file directly (without data)
        var metadataPath = Path.Combine(_metadataDirectory, bucketName, $"{key}.json");
        Directory.CreateDirectory(Path.GetDirectoryName(metadataPath)!);
        await File.WriteAllTextAsync(metadataPath, "{\"BucketName\":\"test-bucket\",\"ETag\":\"test\",\"LastModified\":\"2025-01-01T00:00:00Z\"}");

        // Act
        var result = await _storage.GetMetadataAsync(bucketName, key);

        // Assert
        Assert.Null(result); // Orphaned metadata should be cleaned up and return null
        Assert.False(File.Exists(metadataPath)); // Metadata should be deleted
    }

    [Fact]
    public async Task StoreMetadataAsync_SetsLastModifiedToFileTimestamp()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-file.bin";
        var testData = "test content"u8.ToArray();

        Directory.CreateDirectory(Path.Combine(_dataDirectory, bucketName));
        var dataPath = Path.Combine(_dataDirectory, bucketName, key);
        await File.WriteAllBytesAsync(dataPath, testData);

        var fileTimestamp = File.GetLastWriteTimeUtc(dataPath);

        // Act
        await _storage.StoreMetadataAsync(bucketName, key, "etag", testData.Length, null, null);

        // Read back metadata to check timestamp
        var result = await _storage.GetMetadataAsync(bucketName, key);

        // Assert
        Assert.NotNull(result);
        // Timestamps should be very close (within 1 second due to filesystem precision)
        Assert.True(Math.Abs((result.LastModified - fileTimestamp).TotalSeconds) < 1);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDirectory))
        {
            try
            {
                Directory.Delete(_testDirectory, true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }
}
