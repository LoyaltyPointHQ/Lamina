using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.InMemory;

public class InMemoryObjectMetadataStorage : IObjectMetadataStorage
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, S3Object>> _metadata = new();
    private readonly IObjectDataStorage? _dataStorage;

    /// <summary>
    /// Parameterless constructor kept for tests that exercise metadata in isolation. When no
    /// data storage is injected, staleness detection against an external data backend is
    /// disabled - metadata is assumed to be the source of truth.
    /// </summary>
    public InMemoryObjectMetadataStorage()
    {
    }

    public InMemoryObjectMetadataStorage(IObjectDataStorage dataStorage)
    {
        _dataStorage = dataStorage;
    }

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
            ContentType = request?.ContentType ?? "application/octet-stream",
            Metadata = request?.Metadata ?? new Dictionary<string, string>(),
            Tags = request?.Tags ?? new Dictionary<string, string>(),
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

    public async Task<S3ObjectInfo?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (!_metadata.TryGetValue(bucketName, out var bucketMetadata) ||
            !bucketMetadata.TryGetValue(key, out var s3Object))
        {
            return null;
        }

        // If a data backend is wired in, honour data-first: verify data exists and check
        // whether it has moved past the timestamp we stored metadata for. Useful for hybrid
        // setups (e.g. InMemory metadata + Filesystem data) where the data file can be
        // modified outside the API.
        if (_dataStorage != null)
        {
            var dataInfo = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
            if (dataInfo == null)
            {
                return null; // orphaned metadata
            }

            if (dataInfo.Value.lastModified > s3Object.LastModified)
            {
                var algorithms = CollectStoredChecksumAlgorithms(s3Object);

                var etag = ETagHelper.IsMultipartETag(s3Object.ETag)
                    ? s3Object.ETag
                    : (await _dataStorage.ComputeETagAsync(bucketName, key, cancellationToken)) ?? s3Object.ETag;

                var checksums = algorithms.Count > 0
                    ? await _dataStorage.ComputeChecksumsAsync(bucketName, key, algorithms, cancellationToken)
                    : new Dictionary<string, string>();

                s3Object.ETag = etag;
                s3Object.Size = dataInfo.Value.size;
                s3Object.LastModified = dataInfo.Value.lastModified;
                s3Object.ChecksumCRC32 = checksums.GetValueOrDefault("CRC32");
                s3Object.ChecksumCRC32C = checksums.GetValueOrDefault("CRC32C");
                s3Object.ChecksumCRC64NVME = checksums.GetValueOrDefault("CRC64NVME");
                s3Object.ChecksumSHA1 = checksums.GetValueOrDefault("SHA1");
                s3Object.ChecksumSHA256 = checksums.GetValueOrDefault("SHA256");
            }
        }

        return new S3ObjectInfo
        {
            Key = s3Object.Key,
            LastModified = s3Object.LastModified,
            ETag = s3Object.ETag,
            Size = s3Object.Size,
            ContentType = s3Object.ContentType,
            Metadata = s3Object.Metadata,
            Tags = s3Object.Tags,
            OwnerId = s3Object.OwnerId,
            OwnerDisplayName = s3Object.OwnerDisplayName,
            ChecksumCRC32 = s3Object.ChecksumCRC32,
            ChecksumCRC32C = s3Object.ChecksumCRC32C,
            ChecksumCRC64NVME = s3Object.ChecksumCRC64NVME,
            ChecksumSHA1 = s3Object.ChecksumSHA1,
            ChecksumSHA256 = s3Object.ChecksumSHA256
        };
    }

    private static List<string> CollectStoredChecksumAlgorithms(S3Object s3Object)
    {
        var list = new List<string>();
        if (!string.IsNullOrEmpty(s3Object.ChecksumCRC32)) list.Add("CRC32");
        if (!string.IsNullOrEmpty(s3Object.ChecksumCRC32C)) list.Add("CRC32C");
        if (!string.IsNullOrEmpty(s3Object.ChecksumCRC64NVME)) list.Add("CRC64NVME");
        if (!string.IsNullOrEmpty(s3Object.ChecksumSHA1)) list.Add("SHA1");
        if (!string.IsNullOrEmpty(s3Object.ChecksumSHA256)) list.Add("SHA256");
        return list;
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
}