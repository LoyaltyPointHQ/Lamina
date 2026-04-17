using Lamina.Core.Models;
using Lamina.Storage.InMemory;

namespace Lamina.Storage.Core.Tests;

public class InMemoryLifecycleTests
{
    private static async Task<InMemoryBucketMetadataStorage> CreateWithBucketAsync(string name)
    {
        var dataStorage = new InMemoryBucketDataStorage();
        var storage = new InMemoryBucketMetadataStorage(dataStorage);
        await dataStorage.CreateBucketAsync(name);
        await storage.StoreBucketMetadataAsync(name, new CreateBucketRequest());
        return storage;
    }

    private static LifecycleConfiguration MakeConfig()
    {
        return new LifecycleConfiguration
        {
            Rules = new List<LifecycleRule>
            {
                new()
                {
                    Id = "expire-logs",
                    Status = LifecycleRuleStatus.Enabled,
                    Filter = new LifecycleFilter { Prefix = "logs/" },
                    Expiration = new LifecycleExpiration { Days = 7 }
                }
            }
        };
    }

    [Fact]
    public async Task Set_ThenGet_ReturnsConfig()
    {
        var storage = await CreateWithBucketAsync("b");

        var result = await storage.SetLifecycleConfigurationAsync("b", MakeConfig());

        Assert.True(result);
        var cfg = await storage.GetLifecycleConfigurationAsync("b");
        Assert.NotNull(cfg);
        Assert.Single(cfg.Rules);
        Assert.Equal("expire-logs", cfg.Rules[0].Id);
        Assert.Equal(7, cfg.Rules[0].Expiration?.Days);
    }

    [Fact]
    public async Task Get_NoConfig_ReturnsNull()
    {
        var storage = await CreateWithBucketAsync("b");

        var cfg = await storage.GetLifecycleConfigurationAsync("b");

        Assert.Null(cfg);
    }

    [Fact]
    public async Task Get_NonExistentBucket_ReturnsNull()
    {
        var dataStorage = new InMemoryBucketDataStorage();
        var storage = new InMemoryBucketMetadataStorage(dataStorage);

        var cfg = await storage.GetLifecycleConfigurationAsync("missing");

        Assert.Null(cfg);
    }

    [Fact]
    public async Task Set_NonExistentBucket_ReturnsFalse()
    {
        var dataStorage = new InMemoryBucketDataStorage();
        var storage = new InMemoryBucketMetadataStorage(dataStorage);

        var result = await storage.SetLifecycleConfigurationAsync("missing", MakeConfig());

        Assert.False(result);
    }

    [Fact]
    public async Task Delete_RemovesConfig()
    {
        var storage = await CreateWithBucketAsync("b");
        await storage.SetLifecycleConfigurationAsync("b", MakeConfig());

        var result = await storage.DeleteLifecycleConfigurationAsync("b");

        Assert.True(result);
        var cfg = await storage.GetLifecycleConfigurationAsync("b");
        Assert.Null(cfg);
    }

    [Fact]
    public async Task Delete_NoConfig_ReturnsFalse()
    {
        var storage = await CreateWithBucketAsync("b");

        var result = await storage.DeleteLifecycleConfigurationAsync("b");

        Assert.False(result);
    }

    [Fact]
    public async Task Set_ReplacesExistingConfig()
    {
        var storage = await CreateWithBucketAsync("b");
        await storage.SetLifecycleConfigurationAsync("b", MakeConfig());

        var newCfg = new LifecycleConfiguration
        {
            Rules = new() { new LifecycleRule { Id = "new", Filter = new LifecycleFilter { Prefix = "x/" }, Expiration = new LifecycleExpiration { Days = 1 } } }
        };
        await storage.SetLifecycleConfigurationAsync("b", newCfg);

        var cfg = await storage.GetLifecycleConfigurationAsync("b");
        Assert.Single(cfg!.Rules);
        Assert.Equal("new", cfg.Rules[0].Id);
    }
}
