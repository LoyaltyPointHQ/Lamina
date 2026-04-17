using System.Collections.Concurrent;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;

namespace Lamina.Storage.InMemory;

public class InMemoryBucketMetadataStorage : IBucketMetadataStorage
{
    private readonly ConcurrentDictionary<string, Bucket> _bucketMetadata = new();
    private readonly ConcurrentDictionary<string, LifecycleConfiguration> _lifecycleConfigs = new();
    private readonly IBucketDataStorage _dataStorage;

    public InMemoryBucketMetadataStorage(IBucketDataStorage dataStorage)
    {
        _dataStorage = dataStorage;
    }

    public async Task<Bucket?> StoreBucketMetadataAsync(string bucketName, CreateBucketRequest request, CancellationToken cancellationToken = default)
    {
        // Check if bucket exists in data service
        if (!await _dataStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var bucket = new Bucket
        {
            Name = bucketName,
            CreationDate = DateTime.UtcNow,
            Type = request.Type ?? BucketType.GeneralPurpose,
            StorageClass = request.StorageClass,
            Tags = new Dictionary<string, string>(),
            OwnerId = request.OwnerId,
            OwnerDisplayName = request.OwnerDisplayName
        };

        _bucketMetadata[bucketName] = bucket;
        return bucket;
    }

    public Task<Bucket?> GetBucketMetadataAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        _bucketMetadata.TryGetValue(bucketName, out var bucket);
        return Task.FromResult(bucket);
    }

    public async Task<List<Bucket>> GetAllBucketsMetadataAsync(CancellationToken cancellationToken = default)
    {
        var bucketNames = await _dataStorage.ListBucketNamesAsync(cancellationToken);
        var buckets = new List<Bucket>();

        foreach (var name in bucketNames)
        {
            if (_bucketMetadata.TryGetValue(name, out var bucket))
            {
                buckets.Add(bucket);
            }
        }

        return buckets.OrderBy(b => b.Name).ToList();
    }

    public Task<bool> DeleteBucketMetadataAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_bucketMetadata.TryRemove(bucketName, out _));
    }

    public async Task<Bucket?> UpdateBucketTagsAsync(string bucketName, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        if (!await _dataStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        if (_bucketMetadata.TryGetValue(bucketName, out var bucket))
        {
            bucket.Tags = tags ?? new Dictionary<string, string>();
            return bucket;
        }

        return null;
    }

    public Task<LifecycleConfiguration?> GetLifecycleConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        if (!_bucketMetadata.ContainsKey(bucketName))
        {
            return Task.FromResult<LifecycleConfiguration?>(null);
        }
        _lifecycleConfigs.TryGetValue(bucketName, out var config);
        return Task.FromResult(config);
    }

    public Task<bool> SetLifecycleConfigurationAsync(string bucketName, LifecycleConfiguration configuration, CancellationToken cancellationToken = default)
    {
        if (!_bucketMetadata.ContainsKey(bucketName))
        {
            return Task.FromResult(false);
        }
        _lifecycleConfigs[bucketName] = configuration;
        return Task.FromResult(true);
    }

    public Task<bool> DeleteLifecycleConfigurationAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_lifecycleConfigs.TryRemove(bucketName, out _));
    }
}