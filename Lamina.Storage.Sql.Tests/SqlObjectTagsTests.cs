using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Sql.Context;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Moq;

namespace Lamina.Storage.Sql.Tests;

public class SqlObjectTagsTests : IDisposable
{
    private readonly LaminaDbContext _context;
    private readonly SqlObjectMetadataStorage _storage;

    public SqlObjectTagsTests()
    {
        var options = new DbContextOptionsBuilder<LaminaDbContext>()
            .UseSqlite("Data Source=:memory:")
            .Options;

        _context = new LaminaDbContext(options);
        _context.Database.OpenConnection();
        _context.Database.EnsureCreated();

        var dataStorageMock = new Mock<IObjectDataStorage>();
        dataStorageMock.Setup(x => x.GetDataInfoAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string _, string _, CancellationToken _) => (1024L, DateTime.UtcNow.AddMinutes(-10)));

        _storage = new SqlObjectMetadataStorage(_context, dataStorageMock.Object, Mock.Of<ILogger<SqlObjectMetadataStorage>>());
    }

    public void Dispose()
    {
        _context.Dispose();
    }

    private Task SeedObjectAsync(string bucket, string key) =>
        _storage.StoreMetadataAsync(bucket, key, "etag", 10);

    [Fact]
    public async Task SetTags_ThenGet_ReturnsTags()
    {
        await SeedObjectAsync("b", "k");

        var tags = new Dictionary<string, string> { { "env", "prod" } };
        var result = await _storage.SetObjectTagsAsync("b", "k", tags);

        Assert.True(result);
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
    public async Task SetTags_NonExistentObject_ReturnsFalse()
    {
        var result = await _storage.SetObjectTagsAsync("b", "missing", new Dictionary<string, string> { { "a", "1" } });

        Assert.False(result);
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
    public async Task StoreMetadata_WithTagsInRequest_PersistsThem()
    {
        var request = new PutObjectRequest
        {
            Key = "k",
            Tags = new Dictionary<string, string> { { "env", "dev" } }
        };
        await _storage.StoreMetadataAsync("b", "k", "etag", 5, request);

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
}
