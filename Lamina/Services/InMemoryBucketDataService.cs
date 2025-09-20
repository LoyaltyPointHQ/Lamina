using System.Collections.Concurrent;

namespace Lamina.Services;

public class InMemoryBucketDataService : IBucketDataService
{
    private readonly ConcurrentDictionary<string, bool> _buckets = new();

    public Task<bool> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_buckets.TryAdd(bucketName, true));
    }

    public Task<bool> DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_buckets.TryRemove(bucketName, out _));
    }

    public Task<bool> BucketExistsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_buckets.ContainsKey(bucketName));
    }

    public Task<List<string>> ListBucketNamesAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_buckets.Keys.OrderBy(k => k).ToList());
    }
}