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

        var result = await storage.UpdateBucketLifecycleAsync("b", MakeConfig());

        Assert.True(result);
        var bucket = await storage.GetBucketMetadataAsync("b");
        Assert.NotNull(bucket?.Lifecycle);
        Assert.Single(bucket.Lifecycle.Rules);
        Assert.Equal("expire-logs", bucket.Lifecycle.Rules[0].Id);
        Assert.Equal(7, bucket.Lifecycle.Rules[0].Expiration?.Days);
    }

    [Fact]
    public async Task Get_NoConfig_ReturnsNull()
    {
        var storage = await CreateWithBucketAsync("b");

        var bucket = await storage.GetBucketMetadataAsync("b");

        Assert.Null(bucket?.Lifecycle);
    }

    [Fact]
    public async Task Get_NonExistentBucket_ReturnsNull()
    {
        var dataStorage = new InMemoryBucketDataStorage();
        var storage = new InMemoryBucketMetadataStorage(dataStorage);

        var bucket = await storage.GetBucketMetadataAsync("missing");

        Assert.Null(bucket);
    }

    [Fact]
    public async Task Set_NonExistentBucket_ReturnsFalse()
    {
        var dataStorage = new InMemoryBucketDataStorage();
        var storage = new InMemoryBucketMetadataStorage(dataStorage);

        var result = await storage.UpdateBucketLifecycleAsync("missing", MakeConfig());

        Assert.False(result);
    }

    [Fact]
    public async Task Delete_RemovesConfig()
    {
        var storage = await CreateWithBucketAsync("b");
        await storage.UpdateBucketLifecycleAsync("b", MakeConfig());

        var result = await storage.UpdateBucketLifecycleAsync("b", null);

        Assert.True(result);
        var bucket = await storage.GetBucketMetadataAsync("b");
        Assert.Null(bucket?.Lifecycle);
    }

    [Fact]
    public async Task Delete_NoConfig_ReturnsTrue()
    {
        var storage = await CreateWithBucketAsync("b");

        var result = await storage.UpdateBucketLifecycleAsync("b", null);

        Assert.True(result);
    }

    [Fact]
    public async Task Set_ReplacesExistingConfig()
    {
        var storage = await CreateWithBucketAsync("b");
        await storage.UpdateBucketLifecycleAsync("b", MakeConfig());

        var newCfg = new LifecycleConfiguration
        {
            Rules = new() { new LifecycleRule { Id = "new", Filter = new LifecycleFilter { Prefix = "x/" }, Expiration = new LifecycleExpiration { Days = 1 } } }
        };
        await storage.UpdateBucketLifecycleAsync("b", newCfg);

        var bucket = await storage.GetBucketMetadataAsync("b");
        Assert.Single(bucket!.Lifecycle!.Rules);
        Assert.Equal("new", bucket.Lifecycle.Rules[0].Id);
    }
}
