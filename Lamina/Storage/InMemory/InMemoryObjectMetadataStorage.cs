using System.Collections.Concurrent;
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
            Metadata = request?.Metadata ?? new Dictionary<string, string>()
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
                Metadata = s3Object.Metadata
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

    public async Task<ListObjectsResponse> ListObjectsAsync(string bucketName, ListObjectsRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (!await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return new ListObjectsResponse
            {
                IsTruncated = false,
                Contents = new List<S3ObjectInfo>()
            };
        }

        if (!_metadata.TryGetValue(bucketName, out var bucketMetadata))
        {
            return new ListObjectsResponse
            {
                IsTruncated = false,
                Contents = new List<S3ObjectInfo>()
            };
        }

        var query = bucketMetadata.Values.AsEnumerable();

        if (!string.IsNullOrEmpty(request?.Prefix))
        {
            query = query.Where(o => o.Key.StartsWith(request.Prefix));
        }

        // Use ContinuationToken instead of Marker
        if (!string.IsNullOrEmpty(request?.ContinuationToken))
        {
            query = query.Where(o => string.Compare(o.Key, request.ContinuationToken, StringComparison.Ordinal) > 0);
        }

        var ordered = query.OrderBy(o => o.Key);
        var maxKeys = request?.MaxKeys ?? 1000;
        var objects = ordered.Take(maxKeys + 1).ToList();

        var isTruncated = objects.Count > maxKeys;
        if (isTruncated)
        {
            objects = objects.Take(maxKeys).ToList();
        }

        return new ListObjectsResponse
        {
            Prefix = request?.Prefix,
            MaxKeys = maxKeys,
            IsTruncated = isTruncated,
            NextContinuationToken = isTruncated ? objects.Last().Key : null,
            Contents = objects.Select(o => new S3ObjectInfo
            {
                Key = o.Key,
                LastModified = o.LastModified,
                ETag = o.ETag,
                Size = o.Size,
                ContentType = o.ContentType,
                Metadata = o.Metadata
            }).ToList()
        };
    }

    public Task<bool> MetadataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(
            _metadata.TryGetValue(bucketName, out var bucketMetadata) &&
            bucketMetadata.ContainsKey(key));
    }
}