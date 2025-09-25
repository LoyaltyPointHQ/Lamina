using Lamina.Models;
using Lamina.Storage.InMemory;
using Microsoft.Extensions.Logging.Abstractions;

namespace Lamina.Tests.Storage;

public class BucketTypeStorageTests
{
    [Fact]
    public async Task InMemoryBucketMetadataStorage_StoreBucketType_PersistsCorrectly()
    {
        var dataStorage = new InMemoryBucketDataStorage();
        var storage = new InMemoryBucketMetadataStorage(dataStorage);

        var bucketName = "test-directory-bucket";
        await dataStorage.CreateBucketAsync(bucketName);

        var createRequest = new CreateBucketRequest
        {
            Type = BucketType.Directory,
            StorageClass = "EXPRESS_ONEZONE",
        };

        var bucket = await storage.StoreBucketMetadataAsync(bucketName, createRequest);

        Assert.NotNull(bucket);
        Assert.Equal(BucketType.Directory, bucket.Type);
        Assert.Equal("EXPRESS_ONEZONE", bucket.StorageClass);
        Assert.Equal(bucketName, bucket.Name);

        // Verify retrieval
        var retrievedBucket = await storage.GetBucketMetadataAsync(bucketName);
        Assert.NotNull(retrievedBucket);
        Assert.Equal(BucketType.Directory, retrievedBucket.Type);
        Assert.Equal("EXPRESS_ONEZONE", retrievedBucket.StorageClass);
    }

    [Fact]
    public async Task InMemoryBucketMetadataStorage_DefaultValues_AppliedCorrectly()
    {
        var dataStorage = new InMemoryBucketDataStorage();
        var storage = new InMemoryBucketMetadataStorage(dataStorage);

        var bucketName = "test-default-bucket";
        await dataStorage.CreateBucketAsync(bucketName);

        // Create bucket with default request
        var bucket = await storage.StoreBucketMetadataAsync(bucketName, new CreateBucketRequest());

        Assert.NotNull(bucket);
        Assert.Equal(BucketType.GeneralPurpose, bucket.Type);
        Assert.Null(bucket.StorageClass);
    }

    [Fact]
    public async Task InMemoryBucketMetadataStorage_PartialRequest_FillsDefaults()
    {
        var dataStorage = new InMemoryBucketDataStorage();
        var storage = new InMemoryBucketMetadataStorage(dataStorage);

        var bucketName = "test-partial-bucket";
        await dataStorage.CreateBucketAsync(bucketName);

        var createRequest = new CreateBucketRequest
        {
            Type = BucketType.Directory
            // StorageClass not specified
        };

        var bucket = await storage.StoreBucketMetadataAsync(bucketName, createRequest);

        Assert.NotNull(bucket);
        Assert.Equal(BucketType.Directory, bucket.Type);
        Assert.Null(bucket.StorageClass);
    }

    [Fact]
    public async Task InMemoryBucketMetadataStorage_ListBuckets_ReturnsAllTypes()
    {
        var dataStorage = new InMemoryBucketDataStorage();
        var storage = new InMemoryBucketMetadataStorage(dataStorage);

        var gpBucket = "gp-bucket";
        var dirBucket = "dir-bucket";

        await dataStorage.CreateBucketAsync(gpBucket);
        await dataStorage.CreateBucketAsync(dirBucket);

        await storage.StoreBucketMetadataAsync(gpBucket, new CreateBucketRequest { Type = BucketType.GeneralPurpose });
        await storage.StoreBucketMetadataAsync(dirBucket, new CreateBucketRequest { Type = BucketType.Directory });

        var buckets = await storage.GetAllBucketsMetadataAsync();

        Assert.Equal(2, buckets.Count);

        var gpBucketResult = buckets.FirstOrDefault(b => b.Name == gpBucket);
        var dirBucketResult = buckets.FirstOrDefault(b => b.Name == dirBucket);

        Assert.NotNull(gpBucketResult);
        Assert.NotNull(dirBucketResult);

        Assert.Equal(BucketType.GeneralPurpose, gpBucketResult.Type);
        Assert.Equal(BucketType.Directory, dirBucketResult.Type);
    }
}