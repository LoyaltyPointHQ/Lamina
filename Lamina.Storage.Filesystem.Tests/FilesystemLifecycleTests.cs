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
        await _storage.SetLifecycleConfigurationAsync("b", MakeConfig());

        var cfg = await _storage.GetLifecycleConfigurationAsync("b");

        Assert.NotNull(cfg);
        Assert.Single(cfg.Rules);
        Assert.Equal("r1", cfg.Rules[0].Id);
        Assert.Equal(7, cfg.Rules[0].Expiration?.Days);
    }

    [Fact]
    public async Task Get_NoConfig_ReturnsNull()
    {
        await SeedBucketAsync("b");

        var cfg = await _storage.GetLifecycleConfigurationAsync("b");

        Assert.Null(cfg);
    }

    [Fact]
    public async Task Get_NonExistentBucket_ReturnsNull()
    {
        var cfg = await _storage.GetLifecycleConfigurationAsync("missing");

        Assert.Null(cfg);
    }

    [Fact]
    public async Task Delete_RemovesConfig()
    {
        await SeedBucketAsync("b");
        await _storage.SetLifecycleConfigurationAsync("b", MakeConfig());

        var result = await _storage.DeleteLifecycleConfigurationAsync("b");

        Assert.True(result);
        var cfg = await _storage.GetLifecycleConfigurationAsync("b");
        Assert.Null(cfg);
    }
}
