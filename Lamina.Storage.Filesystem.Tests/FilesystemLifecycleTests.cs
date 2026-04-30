using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace Lamina.Storage.Filesystem.Tests;

public class FilesystemLifecycleTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly FilesystemBucketMetadataStorage _storage;
    private readonly FilesystemBucketDataStorage _dataStorage;

    public FilesystemLifecycleTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"lamina-lc-{Guid.NewGuid():N}");
        var dataDir = Path.Combine(_testDirectory, "data");
        var metaDir = Path.Combine(_testDirectory, "meta");
        Directory.CreateDirectory(dataDir);
        Directory.CreateDirectory(metaDir);

        var settings = new FilesystemStorageSettings
        {
            DataDirectory = dataDir,
            MetadataDirectory = metaDir,
            MetadataMode = MetadataStorageMode.SeparateDirectory
        };

        var networkHelper = new NetworkFileSystemHelper(Options.Create(settings), Mock.Of<ILogger<NetworkFileSystemHelper>>());
        var lockManager = new InMemoryLockManager();

        _dataStorage = new FilesystemBucketDataStorage(Options.Create(settings), networkHelper, Mock.Of<ILogger<FilesystemBucketDataStorage>>());
        _storage = new FilesystemBucketMetadataStorage(
            Options.Create(settings),
            Options.Create(new MetadataCacheSettings { Enabled = false }),
            networkHelper,
            lockManager,
            _dataStorage,
            Mock.Of<ILogger<FilesystemBucketMetadataStorage>>(),
            null);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    private async Task SeedBucketAsync(string name)
    {
        await _dataStorage.CreateBucketAsync(name);
        await _storage.StoreBucketMetadataAsync(name, new CreateBucketRequest());
    }

    private static LifecycleConfiguration MakeConfig() =>
        new()
        {
            Rules = new()
            {
                new LifecycleRule
                {
                    Id = "r1",
                    Status = LifecycleRuleStatus.Enabled,
                    Filter = new LifecycleFilter { Prefix = "logs/" },
                    Expiration = new LifecycleExpiration { Days = 7 }
                }
            }
        };

    [Fact]
    public async Task Set_ThenGet_RoundTripsThroughFile()
    {
        await SeedBucketAsync("b");
        await _storage.UpdateBucketLifecycleAsync("b", MakeConfig());

        var bucket = await _storage.GetBucketMetadataAsync("b");

        Assert.NotNull(bucket?.Lifecycle);
        Assert.Single(bucket.Lifecycle.Rules);
        Assert.Equal("r1", bucket.Lifecycle.Rules[0].Id);
        Assert.Equal(7, bucket.Lifecycle.Rules[0].Expiration?.Days);
    }

    [Fact]
    public async Task Get_NoConfig_ReturnsNull()
    {
        await SeedBucketAsync("b");

        var bucket = await _storage.GetBucketMetadataAsync("b");

        Assert.Null(bucket?.Lifecycle);
    }

    [Fact]
    public async Task Get_NonExistentBucket_ReturnsNull()
    {
        var bucket = await _storage.GetBucketMetadataAsync("missing");

        Assert.Null(bucket);
    }

    [Fact]
    public async Task Delete_RemovesConfig()
    {
        await SeedBucketAsync("b");
        await _storage.UpdateBucketLifecycleAsync("b", MakeConfig());

        var result = await _storage.UpdateBucketLifecycleAsync("b", null);

        Assert.True(result);
        var bucket = await _storage.GetBucketMetadataAsync("b");
        Assert.Null(bucket?.Lifecycle);
    }

    [Fact]
    public async Task Set_ThenDelete_ThenSet_Works()
    {
        await SeedBucketAsync("b");
        await _storage.UpdateBucketLifecycleAsync("b", MakeConfig());
        await _storage.UpdateBucketLifecycleAsync("b", null);

        var newCfg = new LifecycleConfiguration
        {
            Rules = new() { new LifecycleRule { Id = "new-rule", Filter = new LifecycleFilter { Prefix = "" }, Expiration = new LifecycleExpiration { Days = 30 } } }
        };
        await _storage.UpdateBucketLifecycleAsync("b", newCfg);

        var bucket = await _storage.GetBucketMetadataAsync("b");
        Assert.NotNull(bucket?.Lifecycle);
        Assert.Equal("new-rule", bucket.Lifecycle.Rules[0].Id);
    }
}
