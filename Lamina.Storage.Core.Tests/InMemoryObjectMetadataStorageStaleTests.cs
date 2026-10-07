using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.InMemory;

namespace Lamina.Storage.Core.Tests;

public class InMemoryObjectMetadataStorageStaleTests
{
    [Fact]
    public async Task SingleAndBatch_ReturnDetachedStoredMetadataWithoutReadingData()
    {
        var storage = new InMemoryObjectMetadataStorage();
        var time = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        await storage.StoreMetadataAsync("bucket", "key", "old", 3,
            new PutObjectRequest { Key = "key", Tags = new() { ["tag"] = "original" } },
            new() { ["SHA256"] = "saved" }, time);
        var first = await storage.GetMetadataAsync("bucket", "key");
        Assert.NotNull(first);
        first.Metadata.Tags["tag"] = "mutated";
        var batch = await storage.GetMetadataBatchAsync("bucket", ["key", "missing"]);
        Assert.Equal(time, batch["key"]!.DataLastModified);
        Assert.Equal("original", batch["key"]!.Metadata.Tags["tag"]);
        Assert.Equal("saved", batch["key"]!.Metadata.ChecksumSHA256);
        Assert.Equal("old", batch["key"]!.Metadata.ETag);
        Assert.Null(batch["missing"]);
    }

    [Fact]
    public async Task UpdateIntegrity_PreservesLatestTagsAndReplacesChecksumsWithoutUpsert()
    {
        var storage = new InMemoryObjectMetadataStorage();
        Assert.False(await storage.UpdateIntegrityAsync("bucket", "missing", "etag", 3, DateTime.UtcNow, new()));
        await storage.StoreMetadataAsync("bucket", "key", "old", 3,
            new PutObjectRequest { Key = "key", ContentType = "text/plain", Metadata = new() { ["user"] = "value" } },
            new() { ["SHA1"] = "old" });
        await storage.SetObjectTagsAsync("bucket", "key", new() { ["tag"] = "latest" });
        var time = DateTime.UtcNow;
        Assert.True(await storage.UpdateIntegrityAsync("bucket", "key", "new", 7, time, new() { ["CRC32"] = "crc" }));
        var result = (await storage.GetMetadataAsync("bucket", "key"))!;
        Assert.Equal("latest", result.Metadata.Tags["tag"]);
        Assert.Equal("value", result.Metadata.Metadata["user"]);
        Assert.Equal("text/plain", result.Metadata.ContentType);
        Assert.Equal("new", result.Metadata.ETag);
        Assert.Equal(7, result.Metadata.Size);
        Assert.Equal(time, result.DataLastModified);
        Assert.Null(result.Metadata.ChecksumSHA1);
        Assert.Equal("crc", result.Metadata.ChecksumCRC32);
    }
}
