using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Lamina.Storage.Filesystem.Tests;

/// <summary>
/// Raw metadata persistence: data freshness and checksum computation belong to the facade.
/// </summary>
public class FilesystemObjectMetadataStorageStaleTests : IDisposable
{
    private readonly MemoryCache _cache = new(new MemoryCacheOptions());
    private readonly string _testDirectory;
    private readonly string _dataDirectory;
    private readonly string _metadataDirectory;
    private readonly SeparateDirectoryObjectMetadataStorage _storage;
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
        var networkHelper = new NetworkFileSystemHelper(Options.Create(settings), NullLogger<NetworkFileSystemHelper>.Instance);

        var cacheSettings = new MetadataCacheSettings { Enabled = true };

        _dataStorage = new FilesystemObjectDataStorage(
            Options.Create(settings),
            networkHelper,
            new LinuxZeroCopyHelper(NullLogger<LinuxZeroCopyHelper>.Instance),
            NullLogger<FilesystemObjectDataStorage>.Instance);

        _storage = new SeparateDirectoryObjectMetadataStorage(
            Options.Create(settings),
            Options.Create(cacheSettings),
            _bucketStorageMock.Object,
            _dataStorage,
            lockManager,
            networkHelper,
            NullLogger<SeparateDirectoryObjectMetadataStorage>.Instance,
            _cache);
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
        var result = (await _storage.GetMetadataAsync(bucketName, key))?.Metadata;

        // Assert
        Assert.NotNull(result);
        Assert.Equal("etag-123", result.ETag);
        Assert.Equal("crc32-value", result.ChecksumCRC32);
        Assert.Equal("sha256-value", result.ChecksumSHA256);
        Assert.Null(result.ChecksumCRC32C); // Not stored
    }

    [Fact]
    public async Task GetMetadataAsync_StaleMetadata_ReturnsStoredChecksums()
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
        var result = (await _storage.GetMetadataAsync(bucketName, key))?.Metadata;

        // Assert
        Assert.NotNull(result);
        Assert.Equal("old-etag", result.ETag); // Stored value is returned without refreshing.
        Assert.Equal("old-crc32", result.ChecksumCRC32);
        Assert.Equal("old-sha256", result.ChecksumSHA256);
        Assert.Null(result.ChecksumCRC32C); // Was not in original, so not computed
        Assert.Null(result.ChecksumCRC64NVME); // Was not in original, so not computed
    }

    [Fact]
    public async Task GetMetadataAsync_StaleMetadataNoChecksums_ReturnsStoredETag()
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
        var result = (await _storage.GetMetadataAsync(bucketName, key))?.Metadata;

        // Assert
        Assert.NotNull(result);
        Assert.Equal("old-etag", result.ETag); // Stored value is returned without refreshing.
        Assert.Null(result.ChecksumCRC32); // No checksums to recompute
        Assert.Null(result.ChecksumSHA256);
    }

    [Fact]
    public async Task GetMetadataAsync_OrphanedMetadata_ReturnsRawSnapshot()
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
        var result = (await _storage.GetMetadataAsync(bucketName, key))?.Metadata;

        // Assert
        Assert.NotNull(result);
        Assert.True(File.Exists(metadataPath));
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
        var result = (await _storage.GetMetadataAsync(bucketName, key))?.Metadata;

        // Assert
        Assert.NotNull(result);
        // Timestamps should be very close (within 1 second due to filesystem precision)
        Assert.True(Math.Abs((result.LastModified - fileTimestamp).TotalSeconds) < 1);
    }

    [Fact]
    public async Task GetMetadataAsync_StaleMetadata_DoesNotRewriteMetadataFile()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-file.bin";
        var originalData = "original content"u8.ToArray();

        Directory.CreateDirectory(Path.Combine(_dataDirectory, bucketName));
        var dataPath = Path.Combine(_dataDirectory, bucketName, key);
        await File.WriteAllBytesAsync(dataPath, originalData);

        var originalChecksums = new Dictionary<string, string> { { "SHA256", "old-sha256" } };
        await _storage.StoreMetadataAsync(bucketName, key, "old-etag", originalData.Length, null, originalChecksums);

        await Task.Delay(100);

        var newData = "modified content"u8.ToArray();
        await File.WriteAllBytesAsync(dataPath, newData);

        // Act — a raw read must not rewrite metadata.
        var result = (await _storage.GetMetadataAsync(bucketName, key))?.Metadata;
        Assert.NotNull(result);
        var storedETag = result.ETag;
        var storedSha256 = result.ChecksumSHA256;

        // Read the metadata JSON directly from disk
        var metadataPath = Path.Combine(_metadataDirectory, bucketName, $"{key}.json");
        var json = await File.ReadAllTextAsync(metadataPath);

        // Parse JSON to compare field values (avoids unicode-escaping issues with '+' in base64)
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var root = doc.RootElement;

        // Assert — persisted fields remain unchanged.
        Assert.Equal(storedETag, root.GetProperty("ETag").GetString());
        Assert.NotNull(storedSha256);
        Assert.Equal(storedSha256, root.GetProperty("ChecksumSHA256").GetString());
        Assert.Contains("old-etag", json);
        Assert.Contains("old-sha256", json);
    }

    [Fact]
    public async Task GetMetadataAsync_StaleWithMultipartETag_PreservesETag()
    {
        // Multipart ETags ("<32 hex>-<N>") are a function of individual part MD5s and the part
        // count - they cannot be reconstructed from the merged file. If staleness triggers (e.g.
        // external touch or clock skew), raw reads must preserve the stored value.
        var bucketName = "test-bucket";
        var key = "multipart.bin";
        var testData = new byte[] { 0x01, 0x02, 0x03, 0x04 };

        Directory.CreateDirectory(Path.Combine(_dataDirectory, bucketName));
        var dataPath = Path.Combine(_dataDirectory, bucketName, key);
        await File.WriteAllBytesAsync(dataPath, testData);

        const string persistedMultipartETag = "deadbeefdeadbeefdeadbeefdeadbeef-7";
        await _storage.StoreMetadataAsync(bucketName, key, persistedMultipartETag, testData.Length, null, null);

        // Force staleness: bump data mtime past the persisted metadata LastModified.
        await Task.Delay(100);
        File.SetLastWriteTimeUtc(dataPath, DateTime.UtcNow);

        var result = (await _storage.GetMetadataAsync(bucketName, key))?.Metadata;

        Assert.NotNull(result);
        Assert.Equal(persistedMultipartETag, result.ETag);
    }

    [Fact]
    public async Task UpdateIntegrity_ChangesOnlyIntegrityWithoutUpsert()
    {
        const string bucket = "bucket";
        const string key = "key";
        Assert.False(await _storage.UpdateIntegrityAsync(bucket, key, "etag", 1, DateTime.UtcNow, new()));
        Directory.CreateDirectory(Path.Combine(_dataDirectory, bucket));
        await File.WriteAllTextAsync(Path.Combine(_dataDirectory, bucket, key), "data");
        await _storage.StoreMetadataAsync(bucket, key, "old", 4,
            new PutObjectRequest { Key = key, ContentType = "text/plain", Metadata = new() { ["user"] = "saved" } },
            new() { ["SHA1"] = "old" });
        await _storage.SetObjectTagsAsync(bucket, key, new() { ["tag"] = "latest" });
        var time = DateTime.UtcNow;
        Assert.True(await _storage.UpdateIntegrityAsync(bucket, key, "new", 9, time, new() { ["CRC32"] = "crc" }));
        var snapshot = (await _storage.GetMetadataAsync(bucket, key))!;
        Assert.Equal(time, snapshot.DataLastModified);
        Assert.Equal("new", snapshot.Metadata.ETag);
        Assert.Equal(9, snapshot.Metadata.Size);
        Assert.Equal("latest", snapshot.Metadata.Tags["tag"]);
        Assert.Equal("saved", snapshot.Metadata.Metadata["user"]);
        Assert.Equal("text/plain", snapshot.Metadata.ContentType);
        Assert.Null(snapshot.Metadata.ChecksumSHA1);
        Assert.Equal("crc", snapshot.Metadata.ChecksumCRC32);
    }

    [Fact]
    public async Task CachedSnapshot_IsDetachedAndDoesNotInspectOrphanedData()
    {
        var time = DateTime.UtcNow;
        await _storage.StoreMetadataAsync("bucket", "key", "etag", 4,
            new PutObjectRequest { Key = "key", Tags = new() { ["tag"] = "original" } }, lastModified: time);
        var first = (await _storage.GetMetadataAsync("bucket", "key"))!;
        first.Metadata.Tags["tag"] = "mutated";
        var second = (await _storage.GetMetadataAsync("bucket", "key"))!;
        Assert.Equal("original", second.Metadata.Tags["tag"]);
        Assert.Equal(time, second.DataLastModified);
        Assert.Equal("etag", second.Metadata.ETag);
    }

    public void Dispose()
    {
        _cache.Dispose();
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
