using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Lamina.Storage.Filesystem.Tests;

public class FilesystemBucketMetadataStorageTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly string _dataDirectory;
    private readonly string _metadataDirectory;

    public FilesystemBucketMetadataStorageTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"lamina-bucket-md-test-{Guid.NewGuid():N}");
        _dataDirectory = Path.Combine(_testDirectory, "data");
        _metadataDirectory = Path.Combine(_testDirectory, "metadata");
        Directory.CreateDirectory(_dataDirectory);
        Directory.CreateDirectory(_metadataDirectory);
    }

    private FilesystemBucketMetadataStorage CreateStorage(IBucketDataStorage dataStorage)
    {
        var settings = new FilesystemStorageSettings
        {
            DataDirectory = _dataDirectory,
            MetadataDirectory = _metadataDirectory,
            MetadataMode = MetadataStorageMode.SeparateDirectory
        };
        var networkHelper = new NetworkFileSystemHelper(
            Options.Create(settings),
            NullLogger<NetworkFileSystemHelper>.Instance);
        var lockManager = new InMemoryLockManager();

        return new FilesystemBucketMetadataStorage(
            Options.Create(settings),
            Options.Create(new MetadataCacheSettings { Enabled = false }),
            networkHelper,
            lockManager,
            dataStorage,
            NullLogger<FilesystemBucketMetadataStorage>.Instance);
    }

    [Fact]
    public async Task GetBucketMetadataAsync_ReadsCreationTimeFromDataStorage_NotFromDirectoryInfo()
    {
        // The bucket directory does NOT exist on disk - DirectoryInfo(nonexistent).CreationTimeUtc
        // returns Windows epoch (1601-01-01). The data storage mock returns 2020-01-15.
        // The metadata storage MUST use the mocked value, proving it delegates to the abstraction
        // instead of sampling the filesystem directly.
        var expectedCreationDate = new DateTime(2020, 1, 15, 10, 30, 0, DateTimeKind.Utc);
        const string bucketName = "test-bucket";

        var dataStorageMock = new Mock<IBucketDataStorage>();
        dataStorageMock.Setup(x => x.BucketExistsAsync(bucketName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        dataStorageMock.Setup(x => x.GetBucketCreationTimeAsync(bucketName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedCreationDate);

        var storage = CreateStorage(dataStorageMock.Object);

        var result = await storage.GetBucketMetadataAsync(bucketName);

        Assert.NotNull(result);
        Assert.Equal(expectedCreationDate, result.CreationDate);
        dataStorageMock.Verify(
            x => x.GetBucketCreationTimeAsync(bucketName, It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }

    [Fact]
    public async Task StoreBucketMetadataAsync_ReadsCreationTimeFromDataStorage()
    {
        var expectedCreationDate = new DateTime(2021, 6, 1, 0, 0, 0, DateTimeKind.Utc);
        const string bucketName = "store-test";

        var dataStorageMock = new Mock<IBucketDataStorage>();
        dataStorageMock.Setup(x => x.BucketExistsAsync(bucketName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        dataStorageMock.Setup(x => x.GetBucketCreationTimeAsync(bucketName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedCreationDate);

        var storage = CreateStorage(dataStorageMock.Object);

        var result = await storage.StoreBucketMetadataAsync(bucketName, new CreateBucketRequest
        {
            Type = BucketType.GeneralPurpose
        });

        Assert.NotNull(result);
        Assert.Equal(expectedCreationDate, result.CreationDate);
    }

    [Fact]
    public async Task GetBucketMetadataAsync_ReturnsNullWhenBucketDoesNotExist()
    {
        const string bucketName = "missing-bucket";
        var dataStorageMock = new Mock<IBucketDataStorage>();
        dataStorageMock.Setup(x => x.BucketExistsAsync(bucketName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var storage = CreateStorage(dataStorageMock.Object);

        var result = await storage.GetBucketMetadataAsync(bucketName);

        Assert.Null(result);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDirectory))
        {
            try { Directory.Delete(_testDirectory, true); } catch { }
        }
    }
}
