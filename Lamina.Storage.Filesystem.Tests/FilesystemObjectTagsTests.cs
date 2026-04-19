using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Lamina.Storage.Filesystem.Tests;

public class FilesystemObjectTagsTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly string _dataDirectory;
    private readonly string _metadataDirectory;
    private readonly SeparateDirectoryObjectMetadataStorage _storage;

    public FilesystemObjectTagsTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"lamina-tags-{Guid.NewGuid():N}");
        _dataDirectory = Path.Combine(_testDirectory, "data");
        _metadataDirectory = Path.Combine(_testDirectory, "metadata");

        Directory.CreateDirectory(_dataDirectory);
        Directory.CreateDirectory(_metadataDirectory);

        _storage = CreateStorage();
    }

    private SeparateDirectoryObjectMetadataStorage CreateStorage()
    {
        var settings = new FilesystemStorageSettings
        {
            DataDirectory = _dataDirectory,
            MetadataDirectory = _metadataDirectory,
            MetadataMode = MetadataStorageMode.SeparateDirectory
        };

        var bucketStorageMock = new Mock<IBucketStorageFacade>();
        bucketStorageMock.Setup(x => x.BucketExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var lockManager = new InMemoryLockManager();
        var networkHelper = new NetworkFileSystemHelper(Options.Create(settings), NullLogger<NetworkFileSystemHelper>.Instance);

        var dataStorage = new FilesystemObjectDataStorage(
            Options.Create(settings),
            networkHelper,
            new LinuxZeroCopyHelper(NullLogger<LinuxZeroCopyHelper>.Instance),
            NullLogger<FilesystemObjectDataStorage>.Instance,
            Mock.Of<IChunkedDataParser>());

        return new SeparateDirectoryObjectMetadataStorage(
            Options.Create(settings),
            Options.Create(new MetadataCacheSettings { Enabled = false }),
            bucketStorageMock.Object,
            dataStorage,
            lockManager,
            networkHelper,
            NullLogger<SeparateDirectoryObjectMetadataStorage>.Instance,
            null);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    private async Task SeedObjectAsync(string bucket, string key)
    {
        Directory.CreateDirectory(Path.Combine(_dataDirectory, bucket));
        await File.WriteAllBytesAsync(Path.Combine(_dataDirectory, bucket, key), "data"u8.ToArray());
        await _storage.StoreMetadataAsync(bucket, key, "etag", 4);
    }

    [Fact]
    public async Task SetTags_ThenGet_ReturnsTags()
    {
        await SeedObjectAsync("b", "k");
        var tags = new Dictionary<string, string> { { "env", "prod" } };

        var setResult = await _storage.SetObjectTagsAsync("b", "k", tags);

        Assert.True(setResult);
        var retrieved = await _storage.GetObjectTagsAsync("b", "k");
        Assert.NotNull(retrieved);
        Assert.Equal("prod", retrieved["env"]);
    }

    [Fact]
    public async Task GetTags_NonExistentObject_ReturnsNull()
    {
        var result = await _storage.GetObjectTagsAsync("b", "missing");

        Assert.Null(result);
    }

    [Fact]
    public async Task GetTags_ObjectWithoutTags_ReturnsEmpty()
    {
        await SeedObjectAsync("b", "k");

        var result = await _storage.GetObjectTagsAsync("b", "k");

        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public async Task SetTags_ReplacesExistingTags()
    {
        await SeedObjectAsync("b", "k");
        await _storage.SetObjectTagsAsync("b", "k", new Dictionary<string, string> { { "a", "1" } });

        await _storage.SetObjectTagsAsync("b", "k", new Dictionary<string, string> { { "b", "2" } });

        var tags = await _storage.GetObjectTagsAsync("b", "k");
        Assert.NotNull(tags);
        Assert.Single(tags);
        Assert.Equal("2", tags["b"]);
    }

    [Fact]
    public async Task DeleteTags_RemovesAllTags()
    {
        await SeedObjectAsync("b", "k");
        await _storage.SetObjectTagsAsync("b", "k", new Dictionary<string, string> { { "a", "1" } });

        var result = await _storage.DeleteObjectTagsAsync("b", "k");

        Assert.True(result);
        var tags = await _storage.GetObjectTagsAsync("b", "k");
        Assert.NotNull(tags);
        Assert.Empty(tags);
    }

    [Fact]
    public async Task SetTags_NonExistentObject_ReturnsFalse()
    {
        var result = await _storage.SetObjectTagsAsync("b", "missing", new Dictionary<string, string> { { "a", "1" } });

        Assert.False(result);
    }

    [Fact]
    public async Task StoreMetadata_WithTagsInRequest_PersistsThem()
    {
        Directory.CreateDirectory(Path.Combine(_dataDirectory, "b"));
        await File.WriteAllBytesAsync(Path.Combine(_dataDirectory, "b", "k"), "data"u8.ToArray());

        var request = new PutObjectRequest
        {
            Key = "k",
            Tags = new Dictionary<string, string> { { "env", "dev" } }
        };
        await _storage.StoreMetadataAsync("b", "k", "etag", 4, request);

        var tags = await _storage.GetObjectTagsAsync("b", "k");
        Assert.NotNull(tags);
        Assert.Equal("dev", tags["env"]);
    }

    [Fact]
    public async Task GetMetadata_IncludesTags()
    {
        await SeedObjectAsync("b", "k");
        await _storage.SetObjectTagsAsync("b", "k", new Dictionary<string, string> { { "env", "prod" } });

        var info = await _storage.GetMetadataAsync("b", "k");

        Assert.NotNull(info);
        Assert.Equal("prod", info.Tags["env"]);
    }

    [Fact]
    public async Task SetTags_SurvivesRestart_PersistsToFile()
    {
        await SeedObjectAsync("b", "k");
        await _storage.SetObjectTagsAsync("b", "k", new Dictionary<string, string> { { "project", "lamina" } });

        // Use a new storage instance with same directory (simulating restart)
        var freshStorage = CreateStorage();

        var tags = await freshStorage.GetObjectTagsAsync("b", "k");

        Assert.NotNull(tags);
        Assert.Equal("lamina", tags["project"]);
    }
}
