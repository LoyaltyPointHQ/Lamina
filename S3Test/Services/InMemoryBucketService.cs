using System.Collections.Concurrent;
using S3Test.Models;

namespace S3Test.Services;

public class InMemoryBucketService : IBucketService
{
    private readonly ConcurrentDictionary<string, Bucket> _buckets = new();

    public Task<Bucket?> CreateBucketAsync(string bucketName, CreateBucketRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(bucketName))
        {
            return Task.FromResult<Bucket?>(null);
        }

        var bucket = new Bucket
        {
            Name = bucketName,
            CreationDate = DateTime.UtcNow,
            Region = request?.Region ?? "us-east-1",
            Tags = new Dictionary<string, string>()
        };

        if (_buckets.TryAdd(bucketName, bucket))
        {
            return Task.FromResult<Bucket?>(bucket);
        }

        return Task.FromResult<Bucket?>(null);
    }

    public Task<Bucket?> GetBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        _buckets.TryGetValue(bucketName, out var bucket);
        return Task.FromResult(bucket);
    }

    public Task<ListBucketsResponse> ListBucketsAsync(CancellationToken cancellationToken = default)
    {
        var response = new ListBucketsResponse
        {
            Buckets = _buckets.Values.OrderBy(b => b.Name).ToList()
        };
        return Task.FromResult(response);
    }

    public Task<bool> DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_buckets.TryRemove(bucketName, out _));
    }

    public Task<bool> BucketExistsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_buckets.ContainsKey(bucketName));
    }

    public Task<Bucket?> UpdateBucketTagsAsync(string bucketName, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        if (_buckets.TryGetValue(bucketName, out var bucket))
        {
            bucket.Tags = tags ?? new Dictionary<string, string>();
            return Task.FromResult<Bucket?>(bucket);
        }

        return Task.FromResult<Bucket?>(null);
    }
}