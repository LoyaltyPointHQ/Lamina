using Lamina.Core.Models;
using Lamina.Storage.InMemory;

namespace Lamina.Storage.Core.Tests;

public class InMemoryObjectTagsTests
{
    private static InMemoryObjectMetadataStorage CreateStorage() => new();

    private static async Task SeedObjectAsync(InMemoryObjectMetadataStorage storage, string bucket, string key)
    {
        await storage.StoreMetadataAsync(bucket, key, "etag", 10);
    }

    [Fact]
    public async Task SetTags_ThenGet_ReturnsTags()
    {
        var storage = CreateStorage();
        await SeedObjectAsync(storage, "b", "k");

        var tags = new Dictionary<string, string> { { "env", "prod" }, { "team", "core" } };
        var result = await storage.SetObjectTagsAsync("b", "k", tags);

        Assert.True(result);
        var retrieved = await storage.GetObjectTagsAsync("b", "k");
        Assert.NotNull(retrieved);
        Assert.Equal(tags, retrieved);
    }

    [Fact]
    public async Task GetTags_NonExistentObject_ReturnsNull()
    {
        var storage = CreateStorage();

        var result = await storage.GetObjectTagsAsync("b", "missing");

        Assert.Null(result);
    }

    [Fact]
    public async Task GetTags_ObjectWithoutTags_ReturnsEmpty()
    {
        var storage = CreateStorage();
        await SeedObjectAsync(storage, "b", "k");

        var result = await storage.GetObjectTagsAsync("b", "k");

        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public async Task SetTags_ReplacesExistingTags()
    {
        var storage = CreateStorage();
        await SeedObjectAsync(storage, "b", "k");
        await storage.SetObjectTagsAsync("b", "k", new Dictionary<string, string> { { "a", "1" } });

        await storage.SetObjectTagsAsync("b", "k", new Dictionary<string, string> { { "b", "2" } });

        var tags = await storage.GetObjectTagsAsync("b", "k");
        Assert.NotNull(tags);
        Assert.Single(tags);
        Assert.Equal("2", tags["b"]);
    }

    [Fact]
    public async Task DeleteTags_RemovesAllTags()
    {
        var storage = CreateStorage();
        await SeedObjectAsync(storage, "b", "k");
        await storage.SetObjectTagsAsync("b", "k", new Dictionary<string, string> { { "a", "1" } });

        var result = await storage.DeleteObjectTagsAsync("b", "k");

        Assert.True(result);
        var tags = await storage.GetObjectTagsAsync("b", "k");
        Assert.NotNull(tags);
        Assert.Empty(tags);
    }

    [Fact]
    public async Task SetTags_NonExistentObject_ReturnsFalse()
    {
        var storage = CreateStorage();

        var result = await storage.SetObjectTagsAsync("b", "missing", new Dictionary<string, string> { { "a", "1" } });

        Assert.False(result);
    }

    [Fact]
    public async Task DeleteTags_NonExistentObject_ReturnsFalse()
    {
        var storage = CreateStorage();

        var result = await storage.DeleteObjectTagsAsync("b", "missing");

        Assert.False(result);
    }

    [Fact]
    public async Task StoreMetadata_WithTagsInRequest_PersistsThem()
    {
        var storage = CreateStorage();

        var request = new PutObjectRequest
        {
            Key = "k",
            Tags = new Dictionary<string, string> { { "env", "dev" } }
        };
        await storage.StoreMetadataAsync("b", "k", "etag", 5, request);

        var tags = await storage.GetObjectTagsAsync("b", "k");
        Assert.NotNull(tags);
        Assert.Equal("dev", tags["env"]);
    }

    [Fact]
    public async Task GetMetadata_IncludesTags()
    {
        var storage = CreateStorage();
        await SeedObjectAsync(storage, "b", "k");
        await storage.SetObjectTagsAsync("b", "k", new Dictionary<string, string> { { "env", "prod" } });

        var info = await storage.GetMetadataAsync("b", "k");

        Assert.NotNull(info);
        Assert.Equal("prod", info.Tags["env"]);
    }
}
