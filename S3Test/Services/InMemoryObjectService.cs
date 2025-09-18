using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
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

    public async Task<S3Object?> PutObjectAsync(string bucketName, string key, PipeReader dataReader, PutObjectRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var bucketObjects = _objects.GetOrAdd(bucketName, _ => new ConcurrentDictionary<string, S3Object>());

        // Read data from PipeReader
        var dataSegments = new List<byte[]>();
        long totalSize = 0;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await dataReader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (buffer.Length > 0)
                {
                    // Copy data from buffer
                    var data = buffer.ToArray();
                    dataSegments.Add(data);
                    totalSize += data.Length;
                }

                dataReader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }

            // Combine all segments
            var combinedData = new byte[totalSize];
            int offset = 0;
            foreach (var segment in dataSegments)
            {
                Buffer.BlockCopy(segment, 0, combinedData, offset, segment.Length);
                offset += segment.Length;
            }

            var s3Object = new S3Object
            {
                Key = key,
                BucketName = bucketName,
                Size = combinedData.Length,
                LastModified = DateTime.UtcNow,
                ETag = ComputeETag(combinedData),
                ContentType = request?.ContentType ?? "application/octet-stream",
                Metadata = request?.Metadata ?? new Dictionary<string, string>(),
                Data = combinedData
            };

            bucketObjects[key] = s3Object;
            return s3Object;
        }
        finally
        {
            await dataReader.CompleteAsync();
        }
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

    public async Task<bool> WriteObjectToPipeAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default)
    {
        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return false;
        }

        if (_objects.TryGetValue(bucketName, out var bucketObjects) &&
            bucketObjects.TryGetValue(key, out var s3Object))
        {
            // Write data to PipeWriter in chunks
            const int chunkSize = 4096;
            var data = s3Object.Data;
            int offset = 0;

            while (offset < data.Length && !cancellationToken.IsCancellationRequested)
            {
                var remaining = data.Length - offset;
                var bytesToWrite = Math.Min(chunkSize, remaining);

                var memory = writer.GetMemory(bytesToWrite);
                data.AsMemory(offset, bytesToWrite).CopyTo(memory);
                writer.Advance(bytesToWrite);

                offset += bytesToWrite;

                // Flush periodically
                var flushResult = await writer.FlushAsync(cancellationToken);
                if (flushResult.IsCanceled || flushResult.IsCompleted)
                {
                    break;
                }
            }

            await writer.CompleteAsync();
            return true;
        }

        return false;
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