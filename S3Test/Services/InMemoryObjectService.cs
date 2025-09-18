using System.Collections.Concurrent;
using System.Security.Cryptography;
using S3Test.Models;

namespace S3Test.Services;

public class InMemoryObjectService : IObjectService
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, S3Object>> _objects = new();
    private readonly IBucketService _bucketService;

    public InMemoryObjectService(IBucketService bucketService)
    {
        _bucketService = bucketService;
    }

    public async Task<S3Object?> PutObjectAsync(string bucketName, string key, byte[] data, PutObjectRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var bucketObjects = _objects.GetOrAdd(bucketName, _ => new ConcurrentDictionary<string, S3Object>());

        var s3Object = new S3Object
        {
            Key = key,
            BucketName = bucketName,
            Size = data.Length,
            LastModified = DateTime.UtcNow,
            ETag = ComputeETag(data),
            ContentType = request?.ContentType ?? "application/octet-stream",
            Metadata = request?.Metadata ?? new Dictionary<string, string>(),
            Data = data
        };

        bucketObjects[key] = s3Object;
        return s3Object;
    }

    public async Task<GetObjectResponse?> GetObjectAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        if (_objects.TryGetValue(bucketName, out var bucketObjects) &&
            bucketObjects.TryGetValue(key, out var s3Object))
        {
            return new GetObjectResponse
            {
                Data = s3Object.Data,
                ContentType = s3Object.ContentType,
                ContentLength = s3Object.Size,
                ETag = s3Object.ETag,
                LastModified = s3Object.LastModified,
                Metadata = new Dictionary<string, string>(s3Object.Metadata)
            };
        }

        return null;
    }

    public async Task<bool> DeleteObjectAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return false;
        }

        if (_objects.TryGetValue(bucketName, out var bucketObjects))
        {
            return bucketObjects.TryRemove(key, out _);
        }

        return false;
    }

    public async Task<ListObjectsResponse> ListObjectsAsync(string bucketName, ListObjectsRequest? request = null, CancellationToken cancellationToken = default)
    {
        var response = new ListObjectsResponse
        {
            Contents = new List<S3ObjectInfo>(),
            IsTruncated = false,
            Prefix = request?.Prefix,
            Delimiter = request?.Delimiter,
            MaxKeys = request?.MaxKeys ?? 1000
        };

        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return response;
        }

        if (_objects.TryGetValue(bucketName, out var bucketObjects))
        {
            var query = bucketObjects.Values.AsEnumerable();

            if (!string.IsNullOrEmpty(request?.Prefix))
            {
                query = query.Where(o => o.Key.StartsWith(request.Prefix));
            }

            var orderedObjects = query.OrderBy(o => o.Key).ToList();

            int startIndex = 0;
            if (!string.IsNullOrEmpty(request?.ContinuationToken))
            {
                if (int.TryParse(request.ContinuationToken, out var token))
                {
                    startIndex = token;
                }
            }

            var maxKeys = request?.MaxKeys ?? 1000;
            var objectsToReturn = orderedObjects.Skip(startIndex).Take(maxKeys).ToList();

            response.Contents = objectsToReturn.Select(o => new S3ObjectInfo
            {
                Key = o.Key,
                Size = o.Size,
                LastModified = o.LastModified,
                ETag = o.ETag
            }).ToList();

            if (startIndex + objectsToReturn.Count < orderedObjects.Count)
            {
                response.IsTruncated = true;
                response.NextContinuationToken = (startIndex + objectsToReturn.Count).ToString();
            }
        }

        return response;
    }

    public async Task<bool> ObjectExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return false;
        }

        return _objects.TryGetValue(bucketName, out var bucketObjects) &&
               bucketObjects.ContainsKey(key);
    }

    public async Task<S3ObjectInfo?> GetObjectInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        if (_objects.TryGetValue(bucketName, out var bucketObjects) &&
            bucketObjects.TryGetValue(key, out var s3Object))
        {
            return new S3ObjectInfo
            {
                Key = s3Object.Key,
                Size = s3Object.Size,
                LastModified = s3Object.LastModified,
                ETag = s3Object.ETag
            };
        }

        return null;
    }

    private static string ComputeETag(byte[] data)
    {
        using var md5 = MD5.Create();
        var hash = md5.ComputeHash(data);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}