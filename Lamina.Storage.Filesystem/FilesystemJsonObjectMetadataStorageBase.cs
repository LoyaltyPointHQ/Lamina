using System.Runtime.CompilerServices;
using System.Text.Json;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace Lamina.Storage.Filesystem;

/// <summary>
/// Shared logic for filesystem-based JSON object metadata stores. Subclasses differ only in
/// where the metadata JSON physically lives (<see cref="GetMetadataPath"/>) and how existing
/// metadata is enumerated (<see cref="ListAllMetadataKeysAsync"/>). All operations that would
/// otherwise need to read the data backend go through <see cref="IObjectDataStorage"/>, so the
/// data backend is free to be filesystem, in-memory, or anything else.
/// </summary>
public abstract class FilesystemJsonObjectMetadataStorageBase : IObjectMetadataStorage
{
    protected readonly IBucketStorageFacade _bucketStorage;
    protected readonly IObjectDataStorage _dataStorage;
    protected readonly IFileSystemLockManager _lockManager;
    protected readonly NetworkFileSystemHelper _networkHelper;
    protected readonly IMemoryCache? _cache;
    protected readonly MetadataCacheSettings _cacheSettings;
    protected readonly ILogger _logger;

    protected FilesystemJsonObjectMetadataStorageBase(
        IBucketStorageFacade bucketStorage,
        IObjectDataStorage dataStorage,
        IFileSystemLockManager lockManager,
        NetworkFileSystemHelper networkHelper,
        MetadataCacheSettings cacheSettings,
        ILogger logger,
        IMemoryCache? cache)
    {
        _bucketStorage = bucketStorage;
        _dataStorage = dataStorage;
        _lockManager = lockManager;
        _networkHelper = networkHelper;
        _cacheSettings = cacheSettings;
        _cache = cacheSettings.Enabled ? cache : null;
        _logger = logger;
    }

    // ----- abstract members: each mode decides its own physical layout -----

    /// <summary>Absolute path to the metadata JSON file for (bucket, key).</summary>
    protected abstract string GetMetadataPath(string bucketName, string key);

    /// <summary>
    /// Root directory that anchors both key-listing and empty-parent cleanup after a delete.
    /// Subclasses return wherever the metadata JSON physically lives (SeparateDirectory: the
    /// metadata directory; Inline: the data directory, since metadata lives beside data).
    /// </summary>
    protected abstract string GetStorageRootDirectory();

    /// <summary>Directory that represents the bucket's metadata root (never deleted as part of key cleanup).</summary>
    protected abstract string GetBucketDirectory(string bucketName);

    /// <summary>
    /// Enumerates object keys that have metadata under the given bucket directory. Called once
    /// per bucket by <see cref="ListAllMetadataKeysAsync"/>; subclasses need only describe how
    /// their layout maps files to keys.
    /// </summary>
    protected abstract IAsyncEnumerable<string> EnumerateKeysForBucketAsync(
        string bucketDirectory,
        CancellationToken cancellationToken);

    public abstract bool IsValidObjectKey(string key);

    // ----- public API -----

    public async Task<S3Object?> StoreMetadataAsync(
        string bucketName,
        string key,
        string etag,
        long size,
        PutObjectRequest? request = null,
        Dictionary<string, string>? calculatedChecksums = null,
        DateTime? lastModified = null,
        CancellationToken cancellationToken = default)
    {
        InvalidateCache(bucketName, key);

        if (!await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var metadataPath = GetMetadataPath(bucketName, key);
        var metadataDir = Path.GetDirectoryName(metadataPath)!;
        await _networkHelper.EnsureDirectoryExistsAsync(metadataDir, $"StoreMetadata-{bucketName}/{key}");

        // Prefer an explicit LastModified (supplied by multipart Complete before the data is
        // committed) over a lookup on the data backend (which may not see the object yet).
        DateTime resolvedLastModified;
        if (lastModified.HasValue)
        {
            resolvedLastModified = lastModified.Value;
        }
        else
        {
            var dataInfo = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
            resolvedLastModified = dataInfo?.lastModified ?? DateTime.UtcNow;
        }

        var metadata = new S3ObjectMetadata
        {
            BucketName = bucketName,
            ETag = etag,
            LastModified = resolvedLastModified,
            ContentType = request?.ContentType ?? "application/octet-stream",
            Metadata = request?.Metadata ?? new Dictionary<string, string>(),
            Tags = request?.Tags ?? new Dictionary<string, string>(),
            OwnerId = request?.OwnerId,
            OwnerDisplayName = request?.OwnerDisplayName
        };

        if (calculatedChecksums != null)
        {
            if (calculatedChecksums.TryGetValue("CRC32", out var crc32))
                metadata.ChecksumCRC32 = crc32;
            if (calculatedChecksums.TryGetValue("CRC32C", out var crc32c))
                metadata.ChecksumCRC32C = crc32c;
            if (calculatedChecksums.TryGetValue("CRC64NVME", out var crc64nvme))
                metadata.ChecksumCRC64NVME = crc64nvme;
            if (calculatedChecksums.TryGetValue("SHA1", out var sha1))
                metadata.ChecksumSHA1 = sha1;
            if (calculatedChecksums.TryGetValue("SHA256", out var sha256))
                metadata.ChecksumSHA256 = sha256;
        }
        else
        {
            metadata.ChecksumCRC32 = request?.ChecksumCRC32;
            metadata.ChecksumCRC32C = request?.ChecksumCRC32C;
            metadata.ChecksumCRC64NVME = request?.ChecksumCRC64NVME;
            metadata.ChecksumSHA1 = request?.ChecksumSHA1;
            metadata.ChecksumSHA256 = request?.ChecksumSHA256;
        }

        var json = JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true });

        await _lockManager.WriteFileAsync(metadataPath, json, cancellationToken);

        var s3Object = new S3Object
        {
            Key = key,
            BucketName = bucketName,
            Size = size,
            LastModified = resolvedLastModified,
            ETag = etag,
            ContentType = metadata.ContentType,
            Metadata = metadata.Metadata,
            Tags = metadata.Tags,
            OwnerId = metadata.OwnerId,
            OwnerDisplayName = metadata.OwnerDisplayName,
            ChecksumCRC32 = metadata.ChecksumCRC32,
            ChecksumCRC32C = metadata.ChecksumCRC32C,
            ChecksumCRC64NVME = metadata.ChecksumCRC64NVME,
            ChecksumSHA1 = metadata.ChecksumSHA1,
            ChecksumSHA256 = metadata.ChecksumSHA256
        };

        var objectInfo = new S3ObjectInfo
        {
            Key = key,
            Size = size,
            LastModified = resolvedLastModified,
            ETag = etag,
            ContentType = metadata.ContentType,
            Metadata = metadata.Metadata,
            Tags = metadata.Tags,
            OwnerId = metadata.OwnerId,
            OwnerDisplayName = metadata.OwnerDisplayName,
            ChecksumCRC32 = metadata.ChecksumCRC32,
            ChecksumCRC32C = metadata.ChecksumCRC32C,
            ChecksumCRC64NVME = metadata.ChecksumCRC64NVME,
            ChecksumSHA1 = metadata.ChecksumSHA1,
            ChecksumSHA256 = metadata.ChecksumSHA256
        };
        await CacheObjectInfoAsync(bucketName, key, objectInfo, cancellationToken);

        return s3Object;
    }

    public async Task<S3ObjectInfo?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var cacheKey = GetCacheKey(bucketName, key);
        var metadataPath = GetMetadataPath(bucketName, key);

        // Try to get from cache
        if (_cache?.TryGetValue<CachedObjectInfo>(cacheKey, out var cachedEntry) == true && cachedEntry != null)
        {
            // Validate staleness of the metadata file itself (mtime on our own store is fine)
            if (File.Exists(metadataPath))
            {
                var currentMtime = File.GetLastWriteTimeUtc(metadataPath);
                if (currentMtime <= cachedEntry.MetadataFileLastModified)
                {
                    // Metadata JSON unchanged - still need to confirm the data hasn't drifted
                    var dataInfo = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
                    if (dataInfo.HasValue)
                    {
                        if (dataInfo.Value.lastModified <= cachedEntry.ObjectInfo.LastModified)
                        {
                            _logger.LogDebug("Cache hit for object {Key} in bucket {BucketName}", key, bucketName);
                            return cachedEntry.ObjectInfo;
                        }
                        _logger.LogDebug("Data file modified for cached object {Key} in bucket {BucketName}, recomputing", key, bucketName);
                        _cache.Remove(cacheKey);
                    }
                    else
                    {
                        _logger.LogDebug("Data no longer exists for cached object {Key} in bucket {BucketName}", key, bucketName);
                        _cache.Remove(cacheKey);
                        return null;
                    }
                }
                else
                {
                    _logger.LogDebug("Stale cache entry detected for object {Key} in bucket {BucketName} (file mtime: {FileTime}, cache mtime: {CacheTime})",
                        key, bucketName, currentMtime, cachedEntry.MetadataFileLastModified);
                    _cache.Remove(cacheKey);
                }
            }
            else
            {
                _logger.LogDebug("Metadata file no longer exists for cached object {Key} in bucket {BucketName}, removing from cache", key, bucketName);
                _cache.Remove(cacheKey);
            }
        }

        _logger.LogDebug("Cache miss for object {Key} in bucket {BucketName}", key, bucketName);

        if (!File.Exists(metadataPath))
        {
            return null;
        }

        var dataInfoAfterMiss = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
        if (!dataInfoAfterMiss.HasValue)
        {
            _logger.LogWarning("Found orphaned metadata without data for key {Key} in bucket {BucketName}, cleaning up", key, bucketName);
            await DeleteMetadataAsync(bucketName, key, cancellationToken);
            return null;
        }

        var metadata = await _lockManager.ReadFileAsync(metadataPath, content =>
            Task.FromResult(JsonSerializer.Deserialize<S3ObjectMetadata>(content)), cancellationToken);

        if (metadata == null)
        {
            return null;
        }

        // Size and last-modified are authoritative from the data backend (data-first).
        var dataLastModified = dataInfoAfterMiss.Value.lastModified;
        var dataSize = dataInfoAfterMiss.Value.size;

        if (dataLastModified > metadata.LastModified)
        {
            _logger.LogInformation("Detected stale metadata for {Key} in bucket {BucketName} (data mtime: {DataTime}, metadata mtime: {MetadataTime}), recomputing checksums",
                key, bucketName, dataLastModified, metadata.LastModified);

            var recomputed = await RecomputeStaleMetadataAsync(bucketName, key, metadata, cancellationToken);
            metadata.ETag = recomputed.etag;
            metadata.ChecksumCRC32 = recomputed.checksums.GetValueOrDefault("CRC32");
            metadata.ChecksumCRC32C = recomputed.checksums.GetValueOrDefault("CRC32C");
            metadata.ChecksumCRC64NVME = recomputed.checksums.GetValueOrDefault("CRC64NVME");
            metadata.ChecksumSHA1 = recomputed.checksums.GetValueOrDefault("SHA1");
            metadata.ChecksumSHA256 = recomputed.checksums.GetValueOrDefault("SHA256");
            metadata.LastModified = dataLastModified;

            var updatedJson = JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true });
            await _lockManager.WriteFileAsync(metadataPath, updatedJson, cancellationToken);
        }

        var objectInfo = new S3ObjectInfo
        {
            Key = key,
            LastModified = dataLastModified,
            ETag = metadata.ETag,
            Size = dataSize,
            ContentType = metadata.ContentType,
            Metadata = metadata.Metadata,
            Tags = metadata.Tags,
            OwnerId = metadata.OwnerId,
            OwnerDisplayName = metadata.OwnerDisplayName,
            ChecksumCRC32 = metadata.ChecksumCRC32,
            ChecksumCRC32C = metadata.ChecksumCRC32C,
            ChecksumCRC64NVME = metadata.ChecksumCRC64NVME,
            ChecksumSHA1 = metadata.ChecksumSHA1,
            ChecksumSHA256 = metadata.ChecksumSHA256
        };

        await CacheObjectInfoAsync(bucketName, key, objectInfo, cancellationToken);

        return objectInfo;
    }

    public async Task<bool> DeleteMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        InvalidateCache(bucketName, key);

        var metadataPath = GetMetadataPath(bucketName, key);
        var result = await _lockManager.DeleteFile(metadataPath);

        try
        {
            var directory = Path.GetDirectoryName(metadataPath);
            var rootDir = GetStorageRootDirectory();
            var bucketDirectory = GetBucketDirectory(bucketName);

            if (!string.IsNullOrEmpty(directory) &&
                directory.StartsWith(rootDir) &&
                directory != rootDir &&
                directory != bucketDirectory)
            {
                await _networkHelper.DeleteDirectoryIfEmptyAsync(directory, bucketDirectory);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to clean up empty directories for path: {MetadataPath}", metadataPath);
        }

        return result;
    }

    public Task<bool> MetadataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var metadataPath = GetMetadataPath(bucketName, key);
        return Task.FromResult(File.Exists(metadataPath));
    }

    public async IAsyncEnumerable<(string bucketName, string key)> ListAllMetadataKeysAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var rootDirectory = GetStorageRootDirectory();
        if (!Directory.Exists(rootDirectory))
        {
            yield break;
        }

        foreach (var bucketDir in Directory.GetDirectories(rootDirectory))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var bucketName = Path.GetFileName(bucketDir);

            await foreach (var key in EnumerateKeysForBucketAsync(bucketDir, cancellationToken))
            {
                yield return (bucketName, key);
            }
        }
    }

    public async Task<Dictionary<string, string>?> GetObjectTagsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var metadataPath = GetMetadataPath(bucketName, key);
        if (!File.Exists(metadataPath))
        {
            return null;
        }

        if (!await _dataStorage.DataExistsAsync(bucketName, key, cancellationToken))
        {
            return null;
        }

        var metadata = await _lockManager.ReadFileAsync(metadataPath, content =>
            Task.FromResult(JsonSerializer.Deserialize<S3ObjectMetadata>(content)), cancellationToken);

        if (metadata == null)
        {
            return null;
        }

        return new Dictionary<string, string>(metadata.Tags);
    }

    public async Task<bool> SetObjectTagsAsync(string bucketName, string key, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        var metadataPath = GetMetadataPath(bucketName, key);

        if (!await _dataStorage.DataExistsAsync(bucketName, key, cancellationToken))
        {
            return false;
        }

        var jsonOptions = new JsonSerializerOptions { WriteIndented = true };

        var updated = await _lockManager.UpdateFileAsync(metadataPath, current =>
        {
            if (string.IsNullOrEmpty(current))
            {
                return Task.FromResult<string?>(null);
            }

            var metadata = JsonSerializer.Deserialize<S3ObjectMetadata>(current);
            if (metadata == null)
            {
                return Task.FromResult<string?>(null);
            }

            metadata.Tags = new Dictionary<string, string>(tags);
            return Task.FromResult<string?>(JsonSerializer.Serialize(metadata, jsonOptions));
        }, cancellationToken);

        if (updated)
        {
            InvalidateCache(bucketName, key);
        }

        return updated;
    }

    public Task<bool> DeleteObjectTagsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        return SetObjectTagsAsync(bucketName, key, new Dictionary<string, string>(), cancellationToken);
    }

    // ----- helpers -----

    private async Task<(string etag, Dictionary<string, string> checksums)> RecomputeStaleMetadataAsync(
        string bucketName,
        string key,
        S3ObjectMetadata metadata,
        CancellationToken cancellationToken)
    {
        var algorithmsToCompute = new List<string>();
        if (!string.IsNullOrEmpty(metadata.ChecksumCRC32))
            algorithmsToCompute.Add("CRC32");
        if (!string.IsNullOrEmpty(metadata.ChecksumCRC32C))
            algorithmsToCompute.Add("CRC32C");
        if (!string.IsNullOrEmpty(metadata.ChecksumCRC64NVME))
            algorithmsToCompute.Add("CRC64NVME");
        if (!string.IsNullOrEmpty(metadata.ChecksumSHA1))
            algorithmsToCompute.Add("SHA1");
        if (!string.IsNullOrEmpty(metadata.ChecksumSHA256))
            algorithmsToCompute.Add("SHA256");

        var (computedETag, checksums) = await _dataStorage.ComputeETagAndChecksumsAsync(bucketName, key, algorithmsToCompute, cancellationToken);

        // Preserve multipart ETags: recomputing from the merged bytes would yield MD5-of-full-file.
        var etag = ETagHelper.IsMultipartETag(metadata.ETag)
            ? metadata.ETag
            : computedETag ?? metadata.ETag;

        return (etag, checksums);
    }

    private async Task CacheObjectInfoAsync(string bucketName, string key, S3ObjectInfo objectInfo, CancellationToken cancellationToken)
    {
        if (_cache == null)
        {
            return;
        }

        try
        {
            var metadataPath = GetMetadataPath(bucketName, key);
            if (!File.Exists(metadataPath))
            {
                return;
            }

            var metadataFileLastModified = File.GetLastWriteTimeUtc(metadataPath);

            var cachedEntry = new CachedObjectInfo
            {
                ObjectInfo = objectInfo,
                MetadataFileLastModified = metadataFileLastModified
            };

            var cacheKey = GetCacheKey(bucketName, key);
            var entrySize = cachedEntry.EstimateSize();

            var cacheEntryOptions = new MemoryCacheEntryOptions()
                .SetSize(entrySize);

            if (_cacheSettings.AbsoluteExpirationMinutes.HasValue)
            {
                cacheEntryOptions.SetAbsoluteExpiration(TimeSpan.FromMinutes(_cacheSettings.AbsoluteExpirationMinutes.Value));
            }

            if (_cacheSettings.SlidingExpirationMinutes.HasValue)
            {
                cacheEntryOptions.SetSlidingExpiration(TimeSpan.FromMinutes(_cacheSettings.SlidingExpirationMinutes.Value));
            }

            _cache.Set(cacheKey, cachedEntry, cacheEntryOptions);
            _logger.LogDebug("Cached object metadata for {Key} in bucket {BucketName} (size: {Size} bytes)", key, bucketName, entrySize);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to cache object metadata for {Key} in bucket {BucketName}", key, bucketName);
        }

        await Task.CompletedTask;
    }

    private void InvalidateCache(string bucketName, string key)
    {
        if (_cache == null)
        {
            return;
        }

        var cacheKey = GetCacheKey(bucketName, key);
        _cache.Remove(cacheKey);
    }

    private static string GetCacheKey(string bucketName, string key)
    {
        return $"object:{bucketName}:{key}";
    }

    protected class CachedObjectInfo
    {
        public required S3ObjectInfo ObjectInfo { get; init; }
        public required DateTime MetadataFileLastModified { get; init; }

        public long EstimateSize()
        {
            long size = 200;

            size += (ObjectInfo.Key?.Length ?? 0) * 2;
            size += (ObjectInfo.ETag?.Length ?? 0) * 2;
            size += (ObjectInfo.ContentType?.Length ?? 0) * 2;
            size += (ObjectInfo.OwnerId?.Length ?? 0) * 2;
            size += (ObjectInfo.OwnerDisplayName?.Length ?? 0) * 2;

            if (ObjectInfo.Metadata != null)
            {
                foreach (var kvp in ObjectInfo.Metadata)
                {
                    size += (kvp.Key.Length + kvp.Value.Length) * 2;
                    size += 32;
                }
            }

            size += (ObjectInfo.ChecksumCRC32?.Length ?? 0) * 2;
            size += (ObjectInfo.ChecksumCRC32C?.Length ?? 0) * 2;
            size += (ObjectInfo.ChecksumCRC64NVME?.Length ?? 0) * 2;
            size += (ObjectInfo.ChecksumSHA1?.Length ?? 0) * 2;
            size += (ObjectInfo.ChecksumSHA256?.Length ?? 0) * 2;

            return size;
        }
    }

    protected class S3ObjectMetadata
    {
        public required string BucketName { get; set; }
        public required string ETag { get; set; }
        public DateTime LastModified { get; set; }
        public string ContentType { get; set; } = "application/octet-stream";
        public Dictionary<string, string> Metadata { get; set; } = new();
        public Dictionary<string, string> Tags { get; set; } = new();
        public string? OwnerId { get; set; }
        public string? OwnerDisplayName { get; set; }
        public string? ChecksumCRC32 { get; set; }
        public string? ChecksumCRC32C { get; set; }
        public string? ChecksumCRC64NVME { get; set; }
        public string? ChecksumSHA1 { get; set; }
        public string? ChecksumSHA256 { get; set; }
    }
}
