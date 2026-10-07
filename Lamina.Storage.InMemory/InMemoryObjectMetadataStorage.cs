using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;

namespace Lamina.Storage.InMemory;

public class InMemoryObjectMetadataStorage : IObjectMetadataStorage, IBatchObjectMetadataStorage
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, S3Object>> _metadata = new();
    public Task<S3Object?> StoreMetadataAsync(string bucketName, string key, string etag, long size, PutObjectRequest? request = null, Dictionary<string, string>? calculatedChecksums = null, DateTime? lastModified = null, CancellationToken cancellationToken = default)
    {
        // Bucket existence validation is handled by the facade layer
        var bucketMetadata = _metadata.GetOrAdd(bucketName, _ => new ConcurrentDictionary<string, S3Object>());

        var s3Object = new S3Object
        {
            Key = key,
            BucketName = bucketName,
            Size = size,
            LastModified = lastModified ?? DateTime.UtcNow,
            ETag = etag,
            ContentType = string.IsNullOrEmpty(request?.ContentType) ? "application/octet-stream" : request.ContentType,
            Metadata = request?.Metadata is { } metadata ? new(metadata) : new(),
            Tags = request?.Tags is { } tags ? new(tags) : new(),
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

    public Task<ObjectMetadataSnapshot?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_metadata.TryGetValue(bucketName, out var bucket) && bucket.TryGetValue(key, out var value)
            ? Snapshot(value) : null);
    }

    public Task<bool> UpdateIntegrityAsync(string bucketName, string key, string etag, long size, DateTime lastModified, Dictionary<string, string> checksums, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_metadata.TryGetValue(bucketName, out var bucket) || !bucket.TryGetValue(key, out var value))
            return Task.FromResult(false);
        lock (value)
        {
            value.ETag = etag;
            value.Size = size;
            value.LastModified = lastModified;
            value.ChecksumCRC32 = checksums.GetValueOrDefault("CRC32");
            value.ChecksumCRC32C = checksums.GetValueOrDefault("CRC32C");
            value.ChecksumCRC64NVME = checksums.GetValueOrDefault("CRC64NVME");
            value.ChecksumSHA1 = checksums.GetValueOrDefault("SHA1");
            value.ChecksumSHA256 = checksums.GetValueOrDefault("SHA256");
        }
        return Task.FromResult(true);
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

    public Task<Dictionary<string, string>?> GetObjectTagsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (!_metadata.TryGetValue(bucketName, out var bucketMetadata) ||
            !bucketMetadata.TryGetValue(key, out var s3Object))
        {
            return Task.FromResult<Dictionary<string, string>?>(null);
        }
        return Task.FromResult<Dictionary<string, string>?>(new Dictionary<string, string>(s3Object.Tags));
    }

    public Task<bool> SetObjectTagsAsync(string bucketName, string key, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        if (!_metadata.TryGetValue(bucketName, out var bucketMetadata) ||
            !bucketMetadata.TryGetValue(key, out var s3Object))
        {
            return Task.FromResult(false);
        }
        s3Object.Tags = new Dictionary<string, string>(tags);
        return Task.FromResult(true);
    }

    public Task<bool> DeleteObjectTagsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (!_metadata.TryGetValue(bucketName, out var bucketMetadata) ||
            !bucketMetadata.TryGetValue(key, out var s3Object))
        {
            return Task.FromResult(false);
        }
        s3Object.Tags = new Dictionary<string, string>();
        return Task.FromResult(true);
    }

    public Task<Dictionary<string, ObjectMetadataSnapshot?>> GetMetadataBatchAsync(
        string bucketName,
        IEnumerable<string> keys,
        CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<string, ObjectMetadataSnapshot?>();
        _metadata.TryGetValue(bucketName, out var bucketMeta);
        foreach (var key in keys)
        {
            ObjectMetadataSnapshot? info = null;
            if (bucketMeta != null && bucketMeta.TryGetValue(key, out var s3Object))
                info = Snapshot(s3Object);
            result[key] = info;
        }
        return Task.FromResult(result);
    }

    private static ObjectMetadataSnapshot Snapshot(S3Object value)
    {
        lock (value)
            return new ObjectMetadataSnapshot(MapToInfo(value), value.LastModified);
    }

    private static S3ObjectInfo MapToInfo(S3Object s3Object) => new()
    {
        Key = s3Object.Key,
        LastModified = s3Object.LastModified,
        ETag = s3Object.ETag,
        Size = s3Object.Size,
        ContentType = s3Object.ContentType,
        Metadata = new(s3Object.Metadata),
        Tags = new(s3Object.Tags),
        OwnerId = s3Object.OwnerId,
        OwnerDisplayName = s3Object.OwnerDisplayName,
        ChecksumCRC32 = s3Object.ChecksumCRC32,
        ChecksumCRC32C = s3Object.ChecksumCRC32C,
        ChecksumCRC64NVME = s3Object.ChecksumCRC64NVME,
        ChecksumSHA1 = s3Object.ChecksumSHA1,
        ChecksumSHA256 = s3Object.ChecksumSHA256
    };
}