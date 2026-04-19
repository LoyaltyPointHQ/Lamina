using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Sql;
using Lamina.Storage.Sql.Context;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Moq;

namespace Lamina.Storage.Sql.Tests;

/// <summary>
/// Tests for stale metadata detection and recomputation in SqlObjectMetadataStorage.
/// </summary>
public class SqlObjectMetadataStorageStaleTests : IDisposable
{
    private readonly LaminaDbContext _context;
    private readonly Mock<IObjectDataStorage> _dataStorageMock;
    private readonly Mock<ILogger<SqlObjectMetadataStorage>> _loggerMock;
    private readonly SqlObjectMetadataStorage _storage;

    public SqlObjectMetadataStorageStaleTests()
    {
        var options = new DbContextOptionsBuilder<LaminaDbContext>()
            .UseSqlite("Data Source=:memory:")
            .Options;

        _context = new LaminaDbContext(options);
        _context.Database.OpenConnection();
        _context.Database.EnsureCreated();

        _dataStorageMock = new Mock<IObjectDataStorage>();
        _loggerMock = new Mock<ILogger<SqlObjectMetadataStorage>>();

        _storage = new SqlObjectMetadataStorage(_context, _dataStorageMock.Object, _loggerMock.Object);
    }

    [Fact]
    public async Task GetMetadataAsync_FreshMetadata_ReturnsOriginalChecksums()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var etag = "original-etag";
        var size = 1024L;
        var checksums = new Dictionary<string, string>
        {
            { "CRC32", "crc32-value" },
            { "SHA256", "sha256-value" }
        };

        // Store metadata with checksums
        await _storage.StoreMetadataAsync(bucketName, key, etag, size, null, checksums);

        // Setup mock: data timestamp is OLDER than metadata (metadata is fresh)
        _dataStorageMock.Setup(x => x.GetDataInfoAsync(bucketName, key, It.IsAny<CancellationToken>()))
            .ReturnsAsync((size, DateTime.UtcNow.AddMinutes(-10)));

        // Act
        var result = await _storage.GetMetadataAsync(bucketName, key);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(etag, result.ETag);
        Assert.Equal("crc32-value", result.ChecksumCRC32);
        Assert.Equal("sha256-value", result.ChecksumSHA256);
        Assert.Null(result.ChecksumCRC32C); // Not stored originally
    }

    [Fact]
    public async Task GetMetadataAsync_StaleMetadata_RecomputesETagAndSelectiveChecksums()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var originalEtag = "original-etag";
        var newEtag = "recomputed-etag";
        var size = 1024L;
        var checksums = new Dictionary<string, string>
        {
            { "CRC32", "crc32-value" },
            { "SHA256", "sha256-value" }
        };

        // Store metadata with checksums
        await _storage.StoreMetadataAsync(bucketName, key, originalEtag, size, null, checksums);

        // Setup mock: data timestamp is NEWER than metadata (metadata is stale)
        _dataStorageMock.Setup(x => x.GetDataInfoAsync(bucketName, key, It.IsAny<CancellationToken>()))
            .ReturnsAsync((size, DateTime.UtcNow.AddMinutes(10))); // Future timestamp = stale metadata

        // Setup mock: recomputed ETag
        _dataStorageMock.Setup(x => x.ComputeETagAsync(bucketName, key, It.IsAny<CancellationToken>()))
            .ReturnsAsync(newEtag);

        // Setup mock: recomputed checksums (only the ones originally stored)
        _dataStorageMock.Setup(x => x.ComputeChecksumsAsync(
                bucketName, key,
                It.Is<IEnumerable<string>>(algs => algs.Contains("CRC32") && algs.Contains("SHA256") && !algs.Contains("SHA1")),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new Dictionary<string, string>
            {
                { "CRC32", "new-crc32" },
                { "SHA256", "new-sha256" }
            });

        // Act
        var result = await _storage.GetMetadataAsync(bucketName, key);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(newEtag, result.ETag); // ETag was recomputed
        Assert.Equal("new-crc32", result.ChecksumCRC32);   // CRC32 was recomputed (was in original)
        Assert.Equal("new-sha256", result.ChecksumSHA256); // SHA256 was recomputed (was in original)
        Assert.Null(result.ChecksumCRC32C);                 // Not in original, stays null
        Assert.Null(result.ChecksumSHA1);                   // Not in original, stays null

        _dataStorageMock.Verify(x => x.ComputeETagAsync(bucketName, key, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GetMetadataAsync_StaleMetadataWithMultipartETag_PreservesETag()
    {
        // Multipart ETag ("{hex32}-{N}") is a function of part MD5s, not derivable from merged
        // file. Recomputing from file would silently corrupt it. Must be preserved verbatim.
        var bucketName = "test-bucket";
        var key = "multipart-key";
        const string multipartEtag = "deadbeefdeadbeefdeadbeefdeadbeef-5";
        var size = 1024L;

        await _storage.StoreMetadataAsync(bucketName, key, multipartEtag, size, null, null);

        _dataStorageMock.Setup(x => x.GetDataInfoAsync(bucketName, key, It.IsAny<CancellationToken>()))
            .ReturnsAsync((size, DateTime.UtcNow.AddMinutes(10)));

        var result = await _storage.GetMetadataAsync(bucketName, key);

        Assert.NotNull(result);
        Assert.Equal(multipartEtag, result.ETag);
        // ComputeETagAsync must NOT be called for multipart ETags
        _dataStorageMock.Verify(x => x.ComputeETagAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task GetMetadataAsync_StaleMetadataETagComputationFails_UsesOldETag()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var originalEtag = "original-etag";
        var size = 1024L;

        await _storage.StoreMetadataAsync(bucketName, key, originalEtag, size, null, null);

        // Setup mock: data is newer (stale)
        _dataStorageMock.Setup(x => x.GetDataInfoAsync(bucketName, key, It.IsAny<CancellationToken>()))
            .ReturnsAsync((size, DateTime.UtcNow.AddMinutes(10)));

        // Setup mock: ETag computation fails
        _dataStorageMock.Setup(x => x.ComputeETagAsync(bucketName, key, It.IsAny<CancellationToken>()))
            .ReturnsAsync((string?)null);

        // Act
        var result = await _storage.GetMetadataAsync(bucketName, key);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(originalEtag, result.ETag); // Falls back to old ETag
    }

    [Fact]
    public async Task GetMetadataAsync_DataDoesNotExist_ReturnsNull()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        await _storage.StoreMetadataAsync(bucketName, key, "etag", 1024L, null, null);

        // Setup mock: data doesn't exist
        _dataStorageMock.Setup(x => x.GetDataInfoAsync(bucketName, key, It.IsAny<CancellationToken>()))
            .ReturnsAsync((ValueTuple<long, DateTime>?)null);

        // Act
        var result = await _storage.GetMetadataAsync(bucketName, key);

        // Assert
        Assert.Null(result); // Metadata is orphaned, should return null
    }

    [Fact]
    public async Task GetMetadataAsync_StaleMetadata_LogsInformation()
    {
        // Arrange
        var bucketName = "test-bucket";
        var key = "test-key";
        var originalEtag = "original-etag";
        var newEtag = "recomputed-etag";
        var size = 1024L;

        await _storage.StoreMetadataAsync(bucketName, key, originalEtag, size, null, null);

        // Setup mocks
        _dataStorageMock.Setup(x => x.GetDataInfoAsync(bucketName, key, It.IsAny<CancellationToken>()))
            .ReturnsAsync((size, DateTime.UtcNow.AddMinutes(10)));
        _dataStorageMock.Setup(x => x.ComputeETagAsync(bucketName, key, It.IsAny<CancellationToken>()))
            .ReturnsAsync(newEtag);

        // Act
        await _storage.GetMetadataAsync(bucketName, key);

        // Assert - verify logging
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Detected stale metadata")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task GetMetadataAsync_StaleMetadataWithoutChecksums_DoesNotCallComputeChecksums()
    {
        // When original metadata had no checksums, nothing to recompute - avoid the read.
        var bucketName = "test-bucket";
        var key = "test-key";

        await _storage.StoreMetadataAsync(bucketName, key, "etag", 1024L, null, null);

        _dataStorageMock.Setup(x => x.GetDataInfoAsync(bucketName, key, It.IsAny<CancellationToken>()))
            .ReturnsAsync((1024L, DateTime.UtcNow.AddMinutes(10)));
        _dataStorageMock.Setup(x => x.ComputeETagAsync(bucketName, key, It.IsAny<CancellationToken>()))
            .ReturnsAsync("new-etag");

        await _storage.GetMetadataAsync(bucketName, key);

        _dataStorageMock.Verify(
            x => x.ComputeChecksumsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    public void Dispose()
    {
        _context.Database.CloseConnection();
        _context.Dispose();
    }
}
