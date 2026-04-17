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
        await SeedAsync("b");

        var cfg = await _storage.GetLifecycleConfigurationAsync("b");

        Assert.Null(cfg);
    }

    [Fact]
    public async Task Set_NonExistentBucket_ReturnsFalse()
    {
        var result = await _storage.SetLifecycleConfigurationAsync("missing", MakeConfig());

        Assert.False(result);
    }

    [Fact]
    public async Task Delete_RemovesConfig()
    {
        await SeedAsync("b");
        await _storage.SetLifecycleConfigurationAsync("b", MakeConfig());

        var result = await _storage.DeleteLifecycleConfigurationAsync("b");

        Assert.True(result);
        var cfg = await _storage.GetLifecycleConfigurationAsync("b");
        Assert.Null(cfg);
    }

    [Fact]
    public async Task Delete_NoConfig_ReturnsFalse()
    {
        await SeedAsync("b");

        var result = await _storage.DeleteLifecycleConfigurationAsync("b");

        Assert.False(result);
    }
}
