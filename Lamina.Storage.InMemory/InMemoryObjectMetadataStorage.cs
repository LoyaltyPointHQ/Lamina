using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;

namespace Lamina.Storage.InMemory;

public class InMemoryObjectMetadataStorage : IObjectMetadataStorage
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, S3Object>> _metadata = new();

    public InMemoryObjectMetadataStorage()
    {
    }

    public Task<S3Object?> StoreMetadataAsync(string bucketName, string key, string etag, long size, PutObjectRequest? request = null, Dictionary<string, string>? calculatedChecksums = null, CancellationToken cancellationToken = default)
    {
        // Bucket existence validation is handled by the facade layer
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

        // Populate checksum fields from calculated checksums (these take precedence)
        if (calculatedChecksums != null)
        {
            if (calculatedChecksums.TryGetValue("CRC32", out var crc32))
                s3Object.ChecksumCRC32 = crc32;
            if (calculatedChecksums.TryGetValue("CRC32C", out var crc32c))
                s3Object.ChecksumCRC32C = crc32c;
            if (calculatedChecksums.TryGetValue("CRC64NVME", out var crc64nvme))
                s3Object.ChecksumCRC64NVME = crc64nvme;
            if (calculatedChecksums.TryGetValue("SHA1", out var sha1))
                s3Object.ChecksumSHA1 = sha1;
            if (calculatedChecksums.TryGetValue("SHA256", out var sha256))
                s3Object.ChecksumSHA256 = sha256;
        }
        else if (request != null)
        {
            // Fall back to request checksums (e.g. from CopyObject)
            s3Object.ChecksumCRC32 = request.ChecksumCRC32;
            s3Object.ChecksumCRC32C = request.ChecksumCRC32C;
            s3Object.ChecksumCRC64NVME = request.ChecksumCRC64NVME;
            s3Object.ChecksumSHA1 = request.ChecksumSHA1;
            s3Object.ChecksumSHA256 = request.ChecksumSHA256;
        }

        bucketMetadata[key] = s3Object;
        return Task.FromResult<S3Object?>(s3Object);
    }

    public Task<S3ObjectInfo?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (!_metadata.TryGetValue(bucketName, out var bucketMetadata) ||
            !bucketMetadata.TryGetValue(key, out var s3Object))
        {
            return Task.FromResult<S3ObjectInfo?>(null);
        }

        // InMemory storage doesn't support external data modifications,
        // so no staleness check is needed (unlike filesystem storage).

        return Task.FromResult<S3ObjectInfo?>(new S3ObjectInfo
        {
            Key = s3Object.Key,
            LastModified = s3Object.LastModified,
            ETag = s3Object.ETag,
            Size = s3Object.Size,
            ContentType = s3Object.ContentType,
            Metadata = s3Object.Metadata,
            OwnerId = s3Object.OwnerId,
            OwnerDisplayName = s3Object.OwnerDisplayName,
            ChecksumCRC32 = s3Object.ChecksumCRC32,
            ChecksumCRC32C = s3Object.ChecksumCRC32C,
            ChecksumCRC64NVME = s3Object.ChecksumCRC64NVME,
            ChecksumSHA1 = s3Object.ChecksumSHA1,
            ChecksumSHA256 = s3Object.ChecksumSHA256
        });
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