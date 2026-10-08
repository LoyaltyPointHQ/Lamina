using System.Runtime.CompilerServices;
using System.Text.Json;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
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
            Size = size,
            ETag = etag,
            LastModified = resolvedLastModified,
            ContentType = string.IsNullOrEmpty(request?.ContentType) ? "application/octet-stream" : request.ContentType,
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

        // Do not warm the cache from request values after releasing the write lock:
        // a concurrent metadata update may already have committed a newer version.
        InvalidateCache(bucketName, key);

        return s3Object;
    }

    public async Task<ObjectMetadataSnapshot?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var path = GetMetadataPath(bucketName, key);
        if (!File.Exists(path)) { InvalidateCache(bucketName, key); return null; }
        if (_cache?.TryGetValue<CachedObjectInfo>(GetCacheKey(bucketName, key), out var cached) == true && cached != null
            && File.GetLastWriteTimeUtc(path) == cached.MetadataFileLastModified)
            return new ObjectMetadataSnapshot(ObjectMetadataSnapshot.CloneMetadata(cached.ObjectInfo),
                cached.ObjectInfo.LastModified == default ? null : cached.ObjectInfo.LastModified);
        DateTime metadataFileLastModified = default;
        var metadata = await _lockManager.ReadFileAsync(path, content =>
        {
            // The payload and its version must be observed under the same reader lock.
            metadataFileLastModified = File.GetLastWriteTimeUtc(path);
            return Task.FromResult(JsonSerializer.Deserialize<S3ObjectMetadata>(content));
        }, cancellationToken);
        if (metadata == null) return null;
        var info = new S3ObjectInfo
        {
            Key = key,
            Size = metadata.Size,
            LastModified = metadata.LastModified,
            ETag = metadata.ETag,
            ContentType = metadata.ContentType,
            Metadata = new(metadata.Metadata),
            Tags = new(metadata.Tags),
            OwnerId = metadata.OwnerId,
            OwnerDisplayName = metadata.OwnerDisplayName,
            ChecksumCRC32 = metadata.ChecksumCRC32,
            ChecksumCRC32C = metadata.ChecksumCRC32C,
            ChecksumCRC64NVME = metadata.ChecksumCRC64NVME,
            ChecksumSHA1 = metadata.ChecksumSHA1,
            ChecksumSHA256 = metadata.ChecksumSHA256
        };
        CacheObjectInfo(bucketName, key, info, metadataFileLastModified);
        return new ObjectMetadataSnapshot(info, metadata.LastModified == default ? null : metadata.LastModified);
    }

    public async Task<bool> UpdateIntegrityAsync(string bucketName, string key, string etag, long size, DateTime lastModified, Dictionary<string, string> checksums, CancellationToken cancellationToken = default)
    {
        var updated = await _lockManager.UpdateFileAsync(GetMetadataPath(bucketName, key), current =>
        {
            var metadata = string.IsNullOrEmpty(current) ? null : JsonSerializer.Deserialize<S3ObjectMetadata>(current);
            if (metadata == null) return Task.FromResult<string?>(null);
            metadata.ETag = etag;
            metadata.Size = size;
            metadata.LastModified = lastModified;
            metadata.ChecksumCRC32 = checksums.GetValueOrDefault("CRC32");
            metadata.ChecksumCRC32C = checksums.GetValueOrDefault("CRC32C");
            metadata.ChecksumCRC64NVME = checksums.GetValueOrDefault("CRC64NVME");
            metadata.ChecksumSHA1 = checksums.GetValueOrDefault("SHA1");
            metadata.ChecksumSHA256 = checksums.GetValueOrDefault("SHA256");
            return Task.FromResult<string?>(JsonSerializer.Serialize(metadata));
        }, cancellationToken);
        InvalidateCache(bucketName, key);
        return updated;
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

    protected virtual bool IsMetadataBucketDirectory(string name) =>
        !name.StartsWith('.') && !name.StartsWith('_');

    public async IAsyncEnumerable<(string bucketName, string key)> ListAllMetadataKeysAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var rootDirectory = GetStorageRootDirectory();
        foreach (var bucketDir in DirectoryEnumeration.Directories(rootDirectory))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var bucketName = Path.GetFileName(bucketDir);
            if (!IsMetadataBucketDirectory(bucketName)) continue;

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
            S3ObjectMetadata metadata;
            if (string.IsNullOrEmpty(current))
            {
                // No metadata file yet — create a minimal stub so tags can be persisted.
                // The facade will resolve missing integrity fields on the next object read;
                // the metadata backend only stores and returns this stub.
                metadata = new S3ObjectMetadata { BucketName = bucketName, ETag = string.Empty };
            }
            else
            {
                metadata = JsonSerializer.Deserialize<S3ObjectMetadata>(current)
                    ?? new S3ObjectMetadata { BucketName = bucketName, ETag = string.Empty };
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

    private void CacheObjectInfo(string bucketName, string key, S3ObjectInfo objectInfo, DateTime metadataFileLastModified)
    {
        if (_cache == null)
        {
            return;
        }

        try
        {
            var metadataPath = GetMetadataPath(bucketName, key);
            if (!File.Exists(metadataPath) || File.GetLastWriteTimeUtc(metadataPath) != metadataFileLastModified)
            {
                return;
            }

            var cachedEntry = new CachedObjectInfo
            {
                ObjectInfo = ObjectMetadataSnapshot.CloneMetadata(objectInfo),
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
        public long Size { get; set; }
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
