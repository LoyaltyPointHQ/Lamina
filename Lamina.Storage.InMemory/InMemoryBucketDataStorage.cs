using System.Collections.Concurrent;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;

namespace Lamina.Storage.InMemory;

public class InMemoryBucketDataStorage : IBucketDataStorage
{
    private readonly ConcurrentDictionary<string, DateTime> _buckets = new();
    private readonly IObjectDataStorage? _objectDataStorage;

    public InMemoryBucketDataStorage()
    {
    }

    public InMemoryBucketDataStorage(IObjectDataStorage objectDataStorage)
    {
        _objectDataStorage = objectDataStorage;
    }

    public Task<bool> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_buckets.TryAdd(bucketName, DateTime.UtcNow));
    }

    public async Task<DeleteBucketResult> DeleteBucketAsync(string bucketName, bool force = false, CancellationToken cancellationToken = default)
    {
        if (!_buckets.ContainsKey(bucketName))
        {
            return DeleteBucketResult.NotFound;
        }

        if (!force && _objectDataStorage != null)
        {
            var objects = await _objectDataStorage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose, maxKeys: 1, cancellationToken: cancellationToken);
            if (objects.Keys.Count > 0)
            {
                return DeleteBucketResult.NotEmpty;
            }
        }

        return _buckets.TryRemove(bucketName, out _)
            ? DeleteBucketResult.Success
            : DeleteBucketResult.NotFound;
    }

    public Task<bool> BucketExistsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_buckets.ContainsKey(bucketName));
    }

    public Task<DateTime?> GetBucketCreationTimeAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_buckets.TryGetValue(bucketName, out var created) ? (DateTime?)created : null);
    }

    public Task<List<string>> ListBucketNamesAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_buckets.Keys.OrderBy(k => k).ToList());
    }
}