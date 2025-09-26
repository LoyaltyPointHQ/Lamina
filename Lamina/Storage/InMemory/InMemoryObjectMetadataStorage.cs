using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Lamina.Models;
using Lamina.Storage.Abstract;

namespace Lamina.Storage.InMemory;

public class InMemoryObjectMetadataStorage : IObjectMetadataStorage
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, S3Object>> _metadata = new();
    private readonly IBucketStorageFacade _bucketStorage;

    public InMemoryObjectMetadataStorage(IBucketStorageFacade bucketStorage)
    {
        _bucketStorage = bucketStorage;
    }

    public async Task<S3Object?> StoreMetadataAsync(string bucketName, string key, string etag, long size, PutObjectRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (!await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var bucketMetadata = _metadata.GetOrAdd(bucketName, _ => new ConcurrentDictionary<string, S3Object>());

        var s3Object = new S3Object
        {
            Key = key,
            BucketName = bucketName,
            Size = size,
            LastModified = DateTime.UtcNow,
            ETag = etag,
            ContentType = request?.ContentType ?? "application/octet-stream",
            Metadata = request?.Metadata ?? new Dictionary<string, string>(),
            OwnerId = request?.OwnerId,
            OwnerDisplayName = request?.OwnerDisplayName
        };

        bucketMetadata[key] = s3Object;
        return s3Object;
    }

    public Task<S3ObjectInfo?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (_metadata.TryGetValue(bucketName, out var bucketMetadata) &&
            bucketMetadata.TryGetValue(key, out var s3Object))
        {
            return Task.FromResult<S3ObjectInfo?>(new S3ObjectInfo
            {
                Key = s3Object.Key,
                LastModified = s3Object.LastModified,
                ETag = s3Object.ETag,
                Size = s3Object.Size,
                ContentType = s3Object.ContentType,
                Metadata = s3Object.Metadata,
                OwnerId = s3Object.OwnerId,
                OwnerDisplayName = s3Object.OwnerDisplayName
            });
        }
        return Task.FromResult<S3ObjectInfo?>(null);
    }

    public Task<bool> DeleteMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (_metadata.TryGetValue(bucketName, out var bucketMetadata))
        {
            return Task.FromResult(bucketMetadata.TryRemove(key, out _));
        }
        return Task.FromResult(false);
    }

    public Task<bool> MetadataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(
            _metadata.TryGetValue(bucketName, out var bucketMetadata) &&
            bucketMetadata.ContainsKey(key));
    }

    public async IAsyncEnumerable<(string bucketName, string key)> ListAllMetadataKeysAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var bucket in _metadata)
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (var key in bucket.Value.Keys)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return (bucket.Key, key);
            }
        }

        await Task.CompletedTask; // Satisfy async requirement
    }

    public bool IsValidObjectKey(string key) => true;
}