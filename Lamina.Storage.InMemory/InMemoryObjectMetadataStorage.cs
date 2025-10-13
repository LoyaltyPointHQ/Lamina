using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Microsoft.Extensions.Logging;

namespace Lamina.Storage.InMemory;

public class InMemoryObjectMetadataStorage : IObjectMetadataStorage
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, S3Object>> _metadata = new();
    private readonly IBucketStorageFacade _bucketStorage;
    private readonly IObjectDataStorage _dataStorage;
    private readonly ILogger<InMemoryObjectMetadataStorage> _logger;

    public InMemoryObjectMetadataStorage(
        IBucketStorageFacade bucketStorage,
        IObjectDataStorage dataStorage,
        ILogger<InMemoryObjectMetadataStorage> logger)
    {
        _bucketStorage = bucketStorage;
        _dataStorage = dataStorage;
        _logger = logger;
    }

    public async Task<S3Object?> StoreMetadataAsync(string bucketName, string key, string etag, long size, PutObjectRequest? request = null, Dictionary<string, string>? calculatedChecksums = null, CancellationToken cancellationToken = default)
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

        bucketMetadata[key] = s3Object;
        return s3Object;
    }

    public async Task<S3ObjectInfo?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (!_metadata.TryGetValue(bucketName, out var bucketMetadata) ||
            !bucketMetadata.TryGetValue(key, out var s3Object))
        {
            return null;
        }

        // Check if the data has been modified after the metadata was stored
        var dataInfo = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
        if (dataInfo == null)
        {
            // Data doesn't exist, metadata is orphaned
            return null;
        }

        // If data is newer than metadata, recompute ETag and clear checksums
        if (dataInfo.Value.lastModified > s3Object.LastModified)
        {
            _logger.LogInformation("Detected stale metadata for {Key} in bucket {BucketName} (data mtime: {DataTime}, metadata mtime: {MetadataTime}), recomputing ETag",
                key, bucketName, dataInfo.Value.lastModified, s3Object.LastModified);

            var recomputed = await RecomputeStaleMetadataAsync(s3Object, bucketName, key, cancellationToken);
            s3Object.ETag = recomputed.etag;
            // Clear checksums for stale metadata (return null for all)
            s3Object.ChecksumCRC32 = null;
            s3Object.ChecksumCRC32C = null;
            s3Object.ChecksumCRC64NVME = null;
            s3Object.ChecksumSHA1 = null;
            s3Object.ChecksumSHA256 = null;
        }

        return new S3ObjectInfo
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
        };
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

    /// <summary>
    /// Recomputes ETag for stale metadata and clears checksums.
    /// For InMemory storage, we only recompute the ETag and clear checksums.
    /// </summary>
    private async Task<(string etag, Dictionary<string, string> checksums)> RecomputeStaleMetadataAsync(
        S3Object s3Object,
        string bucketName,
        string key,
        CancellationToken cancellationToken)
    {
        // Always recompute ETag (it's relatively cheap and essential)
        var etag = await _dataStorage.ComputeETagAsync(bucketName, key, cancellationToken);
        if (etag == null)
        {
            // If we can't compute ETag, return the existing one
            _logger.LogWarning("Failed to recompute ETag for {Key} in bucket {BucketName}, using cached value", key, bucketName);
            return (s3Object.ETag, new Dictionary<string, string>());
        }

        // For InMemory storage, we clear checksums when metadata is stale
        // They will be null, indicating they need to be revalidated if required
        var hadChecksums = !string.IsNullOrEmpty(s3Object.ChecksumCRC32) ||
                          !string.IsNullOrEmpty(s3Object.ChecksumCRC32C) ||
                          !string.IsNullOrEmpty(s3Object.ChecksumCRC64NVME) ||
                          !string.IsNullOrEmpty(s3Object.ChecksumSHA1) ||
                          !string.IsNullOrEmpty(s3Object.ChecksumSHA256);

        if (hadChecksums)
        {
            _logger.LogInformation("Clearing checksums for stale metadata of {Key} in bucket {BucketName}", key, bucketName);
        }

        // Return empty dictionary which will cause all checksums to be set to null
        return (etag, new Dictionary<string, string>());
    }
}