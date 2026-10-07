using Lamina.Core.Models;
using Lamina.Storage.InMemory;

namespace Lamina.Storage.Core.Tests;

public class ContentTypeNormalizationTests
{
    private const string Bucket = "bucket";
    private const string Key = "file";

    [Fact]
    public async Task StoreMetadata_WithEmptyContentType_NormalizesToOctetStream()
    {
        var storage = new InMemoryObjectMetadataStorage();

        await storage.StoreMetadataAsync(Bucket, Key, "etag", 10,
            new PutObjectRequest { Key = Key, ContentType = "" });

        var result = (await storage.GetMetadataAsync(Bucket, Key))?.Metadata;

        Assert.NotNull(result);
        Assert.Equal("application/octet-stream", result.ContentType);
    }

    [Fact]
    public async Task StoreMetadata_WithNullContentType_NormalizesToOctetStream()
    {
        var storage = new InMemoryObjectMetadataStorage();

        await storage.StoreMetadataAsync(Bucket, Key, "etag", 10,
            new PutObjectRequest { Key = Key, ContentType = null });

        var result = (await storage.GetMetadataAsync(Bucket, Key))?.Metadata;

        Assert.NotNull(result);
        Assert.Equal("application/octet-stream", result.ContentType);
    }

    [Fact]
    public async Task StoreMetadata_WithValidContentType_PreservesIt()
    {
        var storage = new InMemoryObjectMetadataStorage();

        await storage.StoreMetadataAsync(Bucket, Key, "etag", 10,
            new PutObjectRequest { Key = Key, ContentType = "text/plain" });

        var result = (await storage.GetMetadataAsync(Bucket, Key))?.Metadata;

        Assert.NotNull(result);
        Assert.Equal("text/plain", result.ContentType);
    }
}
