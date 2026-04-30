using Lamina.Core.Models;
using Lamina.Storage.Sql.Context;
using Microsoft.EntityFrameworkCore;

namespace Lamina.Storage.Sql.Tests;

public class SqlLifecycleTests : IDisposable
{
    private readonly LaminaDbContext _context;
    private readonly SqlBucketMetadataStorage _storage;

    public SqlLifecycleTests()
    {
        var options = new DbContextOptionsBuilder<LaminaDbContext>()
            .UseSqlite("Data Source=:memory:")
            .Options;

        _context = new LaminaDbContext(options);
        _context.Database.OpenConnection();
        _context.Database.EnsureCreated();

        _storage = new SqlBucketMetadataStorage(_context);
    }

    public void Dispose() => _context.Dispose();

    private Task SeedAsync(string bucketName) =>
        _storage.StoreBucketMetadataAsync(bucketName, new CreateBucketRequest());

    private static LifecycleConfiguration MakeConfig() => new()
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
    public async Task Set_ThenGet_RoundTrips()
    {
        await SeedAsync("b");
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
        await SeedAsync("b");

        var bucket = await _storage.GetBucketMetadataAsync("b");

        Assert.Null(bucket?.Lifecycle);
    }

    [Fact]
    public async Task Set_NonExistentBucket_ReturnsFalse()
    {
        var result = await _storage.UpdateBucketLifecycleAsync("missing", MakeConfig());

        Assert.False(result);
    }

    [Fact]
    public async Task Delete_RemovesConfig()
    {
        await SeedAsync("b");
        await _storage.UpdateBucketLifecycleAsync("b", MakeConfig());

        var result = await _storage.UpdateBucketLifecycleAsync("b", null);

        Assert.True(result);
        var bucket = await _storage.GetBucketMetadataAsync("b");
        Assert.Null(bucket?.Lifecycle);
    }

    [Fact]
    public async Task Delete_NoConfig_ReturnsTrue()
    {
        await SeedAsync("b");

        var result = await _storage.UpdateBucketLifecycleAsync("b", null);

        Assert.True(result);
    }
}
