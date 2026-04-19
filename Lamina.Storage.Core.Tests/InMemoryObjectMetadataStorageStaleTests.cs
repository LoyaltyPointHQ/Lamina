using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.InMemory;
using Moq;

namespace Lamina.Storage.Core.Tests;

/// <summary>
/// Staleness detection in InMemoryObjectMetadataStorage is only relevant when paired with an
/// external data backend (e.g. Filesystem). When no IObjectDataStorage is injected, the
/// storage trusts its own state - this is the default for pure in-memory setups.
/// </summary>
public class InMemoryObjectMetadataStorageStaleTests
{
    private const string Bucket = "bucket";
    private const string Key = "file";

    [Fact]
    public async Task GetMetadataAsync_NoDataStorageInjected_ReturnsStoredEntryVerbatim()
    {
        var storage = new InMemoryObjectMetadataStorage();
        await storage.StoreMetadataAsync(Bucket, Key, "etag-1", 10, null, null, new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        var result = await storage.GetMetadataAsync(Bucket, Key);

        Assert.NotNull(result);
        Assert.Equal("etag-1", result.ETag);
        Assert.Equal(10, result.Size);
    }

    [Fact]
    public async Task GetMetadataAsync_DataGoneFromBackend_ReturnsNullOrphan()
    {
        var dataStorageMock = new Mock<IObjectDataStorage>();
        dataStorageMock.Setup(x => x.GetDataInfoAsync(Bucket, Key, It.IsAny<CancellationToken>()))
            .ReturnsAsync((ValueTuple<long, DateTime>?)null);

        var storage = new InMemoryObjectMetadataStorage(dataStorageMock.Object);
        await storage.StoreMetadataAsync(Bucket, Key, "etag", 10, null, null, DateTime.UtcNow);

        var result = await storage.GetMetadataAsync(Bucket, Key);

        Assert.Null(result);
    }

    [Fact]
    public async Task GetMetadataAsync_DataNewerThanMetadata_RecomputesETagAndChecksums()
    {
        var storedTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var newerDataTime = storedTime.AddHours(1);

        var dataStorageMock = new Mock<IObjectDataStorage>();
        dataStorageMock.Setup(x => x.GetDataInfoAsync(Bucket, Key, It.IsAny<CancellationToken>()))
            .ReturnsAsync((42L, newerDataTime));
        dataStorageMock.Setup(x => x.ComputeETagAndChecksumsAsync(
                Bucket, Key,
                It.Is<IEnumerable<string>>(a => a.Contains("CRC32") && !a.Contains("SHA1")),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(("recomputed-etag", new Dictionary<string, string> { { "CRC32", "new-crc32" } }));

        var storage = new InMemoryObjectMetadataStorage(dataStorageMock.Object);
        await storage.StoreMetadataAsync(Bucket, Key, "old-etag", 10, null,
            new Dictionary<string, string> { { "CRC32", "old-crc32" } }, storedTime);

        var result = await storage.GetMetadataAsync(Bucket, Key);

        Assert.NotNull(result);
        Assert.Equal("recomputed-etag", result.ETag);
        Assert.Equal(42L, result.Size);
        Assert.Equal(newerDataTime, result.LastModified);
        Assert.Equal("new-crc32", result.ChecksumCRC32);
        Assert.Null(result.ChecksumSHA1); // not stored, not recomputed
    }

    [Fact]
    public async Task GetMetadataAsync_DataNewerWithMultipartETag_PreservesETag()
    {
        var storedTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        const string multipartEtag = "deadbeefdeadbeefdeadbeefdeadbeef-4";

        var dataStorageMock = new Mock<IObjectDataStorage>();
        dataStorageMock.Setup(x => x.GetDataInfoAsync(Bucket, Key, It.IsAny<CancellationToken>()))
            .ReturnsAsync((42L, storedTime.AddHours(1)));

        var storage = new InMemoryObjectMetadataStorage(dataStorageMock.Object);
        await storage.StoreMetadataAsync(Bucket, Key, multipartEtag, 10, null, null, storedTime);

        dataStorageMock.Setup(x => x.ComputeETagAndChecksumsAsync(Bucket, Key, It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(("some-computed-etag", new Dictionary<string, string>()));

        var result = await storage.GetMetadataAsync(Bucket, Key);

        Assert.NotNull(result);
        // Multipart ETag must be preserved, not replaced with the computed MD5
        Assert.Equal(multipartEtag, result.ETag);
    }

    [Fact]
    public async Task GetMetadataAsync_DataOlderThanMetadata_ReturnsStoredEntry()
    {
        var storedTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var olderDataTime = storedTime.AddMinutes(-5);

        var dataStorageMock = new Mock<IObjectDataStorage>();
        dataStorageMock.Setup(x => x.GetDataInfoAsync(Bucket, Key, It.IsAny<CancellationToken>()))
            .ReturnsAsync((10L, olderDataTime));

        var storage = new InMemoryObjectMetadataStorage(dataStorageMock.Object);
        await storage.StoreMetadataAsync(Bucket, Key, "etag", 10, null, null, storedTime);

        var result = await storage.GetMetadataAsync(Bucket, Key);

        Assert.NotNull(result);
        Assert.Equal("etag", result.ETag);
        dataStorageMock.Verify(x => x.ComputeETagAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}
