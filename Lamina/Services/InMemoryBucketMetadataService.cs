using System.Collections.Concurrent;
using Lamina.Models;

namespace Lamina.Services;

public class InMemoryBucketMetadataService : IBucketMetadataService
{
    private readonly ConcurrentDictionary<string, Bucket> _bucketMetadata = new();
    private readonly IBucketDataService _dataService;

    public InMemoryBucketMetadataService(IBucketDataService dataService)
    {
        _dataService = dataService;
    }

    public async Task<Bucket?> StoreBucketMetadataAsync(string bucketName, CreateBucketRequest? request = null, CancellationToken cancellationToken = default)
    {
        // Check if bucket exists in data service
        if (!await _dataService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var bucket = new Bucket
        {
            Name = bucketName,
            CreationDate = DateTime.UtcNow,
            Region = request?.Region ?? "us-east-1",
            Tags = new Dictionary<string, string>()
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
        var bucketNames = await _dataService.ListBucketNamesAsync(cancellationToken);
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
        if (!await _dataService.BucketExistsAsync(bucketName, cancellationToken))
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
}