using System.Text.Json;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

public class FilesystemMultipartUploadMetadataStorage : IMultipartUploadMetadataStorage
{
    private readonly string _dataDirectory;
    private readonly string? _metadataDirectory;
    private readonly MetadataStorageMode _metadataMode;
    private readonly string _inlineMetadataDirectoryName;
    private readonly NetworkFileSystemHelper _networkHelper;
    private readonly IFileSystemLockManager _lockManager;
    private readonly IMemoryCache? _cache;
    private readonly MetadataCacheSettings _cacheSettings;
    private readonly ILogger<FilesystemMultipartUploadMetadataStorage> _logger;

    public FilesystemMultipartUploadMetadataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        IOptions<MetadataCacheSettings> cacheSettingsOptions,
        NetworkFileSystemHelper networkHelper,
        IFileSystemLockManager lockManager,
        ILogger<FilesystemMultipartUploadMetadataStorage> logger,
        IMemoryCache? cache = null)
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _metadataMode = settings.MetadataMode;
        _metadataDirectory = settings.MetadataDirectory;
        _inlineMetadataDirectoryName = settings.InlineMetadataDirectoryName;
        _networkHelper = networkHelper;
        _lockManager = lockManager;
        _cacheSettings = cacheSettingsOptions.Value;
        _cache = _cacheSettings.Enabled ? cache : null;
        _logger = logger;

        _networkHelper.EnsureDirectoryExists(_dataDirectory);

        if (_metadataMode == MetadataStorageMode.SeparateDirectory)
        {
            if (string.IsNullOrWhiteSpace(_metadataDirectory))
            {
                throw new InvalidOperationException("MetadataDirectory is required when using SeparateDirectory metadata mode");
            }
            _networkHelper.EnsureDirectoryExists(_metadataDirectory);
        }
    }

    public async Task<MultipartUpload> InitiateUploadAsync(string bucketName, string key, InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        var uploadId = Guid.NewGuid().ToString();
        var upload = new MultipartUpload
        {
            UploadId = uploadId,
            Key = key,
            BucketName = bucketName,
            Initiated = DateTime.UtcNow,
            ContentType = request.ContentType ?? "application/octet-stream",
            Metadata = request.Metadata ?? new Dictionary<string, string>()
        };

        var uploadMetadataPath = GetUploadMetadataPath(uploadId);
        var uploadDir = Path.GetDirectoryName(uploadMetadataPath)!;
        await _networkHelper.EnsureDirectoryExistsAsync(uploadDir, $"InitiateUpload-{uploadId}");

        var json = JsonSerializer.Serialize(upload, new JsonSerializerOptions { WriteIndented = true });

        await _lockManager.WriteFileAsync(uploadMetadataPath, json, cancellationToken);

        // Cache the result
        await CacheUploadAsync(upload, cancellationToken);

        return upload;
    }

    public async Task<MultipartUpload?> GetUploadMetadataAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var cacheKey = GetCacheKey(uploadId);

        // Try to get from cache
        if (_cache?.TryGetValue<CachedUpload>(cacheKey, out var cachedEntry) == true && cachedEntry != null)
        {
            // Validate staleness by checking if metadata JSON file has been modified
            var metadataPath = GetUploadMetadataPath(uploadId);
            if (File.Exists(metadataPath))
            {
                var currentMtime = File.GetLastWriteTimeUtc(metadataPath);
                if (currentMtime <= cachedEntry.MetadataFileLastModified)
                {
                    // Fresh cache entry
                    _logger.LogDebug("Cache hit for upload {UploadId}", uploadId);
                    return cachedEntry.Upload;
                }
                // Stale - file modified, fall through
                _logger.LogDebug("Stale cache entry detected for upload {UploadId} (file mtime: {FileTime}, cache mtime: {CacheTime})",
                    uploadId, currentMtime, cachedEntry.MetadataFileLastModified);
                _cache.Remove(cacheKey);
            }
            else
            {
                // Metadata file no longer exists, remove from cache
                _logger.LogDebug("Metadata file no longer exists for cached upload {UploadId}, removing from cache", uploadId);
                _cache.Remove(cacheKey);
            }
        }

        // Cache miss or stale entry
        _logger.LogDebug("Cache miss for upload {UploadId}", uploadId);

        var uploadMetadataPath = GetUploadMetadataPath(uploadId);

        if (!File.Exists(uploadMetadataPath))
        {
            return null;
        }

        var upload = await _lockManager.ReadFileAsync(uploadMetadataPath, async content =>
        {
            return await Task.FromResult(JsonSerializer.Deserialize<MultipartUpload>(content));
        }, cancellationToken);

        if (upload == null || upload.BucketName != bucketName || upload.Key != key)
        {
            return null;
        }

        // Cache the result
        await CacheUploadAsync(upload, cancellationToken);

        return upload;
    }

    public Task<bool> DeleteUploadMetadataAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        // Invalidate cache
        InvalidateCache(uploadId);

        var uploadMetadataPath = GetUploadMetadataPath(uploadId);
        return _lockManager.DeleteFile(uploadMetadataPath);
    }

    public async Task<List<MultipartUpload>> ListUploadsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var uploads = new List<MultipartUpload>();
        var multipartUploadsDir = _metadataMode == MetadataStorageMode.SeparateDirectory
            ? Path.Combine(_metadataDirectory!, "_multipart_uploads")
            : Path.Combine(_dataDirectory, _inlineMetadataDirectoryName, "_multipart_uploads");

        if (!Directory.Exists(multipartUploadsDir))
        {
            return uploads;
        }

        var uploadDirs = Directory.GetDirectories(multipartUploadsDir);

        foreach (var uploadDir in uploadDirs)
        {
            var uploadId = Path.GetFileName(uploadDir);
            var uploadMetadataPath = GetUploadMetadataPath(uploadId);

            if (File.Exists(uploadMetadataPath))
            {
                try
                {
                    var json = await File.ReadAllTextAsync(uploadMetadataPath, cancellationToken);
                    var upload = JsonSerializer.Deserialize<MultipartUpload>(json);

                    if (upload != null && upload.BucketName == bucketName)
                    {
                        uploads.Add(upload);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to read upload metadata: {UploadMetadataPath}", uploadMetadataPath);
                }
            }
        }

        return uploads.OrderBy(u => u.Initiated).ToList();
    }

    public async Task UpdateUploadMetadataAsync(string bucketName, string key, string uploadId, MultipartUpload upload, CancellationToken cancellationToken = default)
    {
        // Invalidate cache before updating
        InvalidateCache(uploadId);

        var uploadMetadataPath = GetUploadMetadataPath(uploadId);
        var json = JsonSerializer.Serialize(upload, new JsonSerializerOptions { WriteIndented = true });
        await _lockManager.WriteFileAsync(uploadMetadataPath, json, cancellationToken);

        // Cache the updated result
        await CacheUploadAsync(upload, cancellationToken);
    }

    private string GetUploadMetadataPath(string uploadId)
    {
        if (_metadataMode == MetadataStorageMode.SeparateDirectory)
        {
            return Path.Combine(_metadataDirectory!, "_multipart_uploads", uploadId, "upload.metadata.json");
        }
        else
        {
            // For inline mode, store multipart uploads in a special directory at the data root
            return Path.Combine(_dataDirectory, _inlineMetadataDirectoryName, "_multipart_uploads", uploadId, "upload.metadata.json");
        }
    }

    private async Task CacheUploadAsync(MultipartUpload upload, CancellationToken cancellationToken)
    {
        if (_cache == null)
        {
            return;
        }

        try
        {
            // Get current metadata file modification time for staleness tracking
            var metadataPath = GetUploadMetadataPath(upload.UploadId);
            if (!File.Exists(metadataPath))
            {
                // Don't cache if metadata file doesn't exist
                return;
            }

            var metadataFileLastModified = File.GetLastWriteTimeUtc(metadataPath);

            var cachedEntry = new CachedUpload
            {
                Upload = upload,
                MetadataFileLastModified = metadataFileLastModified
            };

            var cacheKey = GetCacheKey(upload.UploadId);
            var entrySize = cachedEntry.EstimateSize();

            var cacheEntryOptions = new MemoryCacheEntryOptions()
                .SetSize(entrySize);

            // Set absolute expiration if configured
            if (_cacheSettings.AbsoluteExpirationMinutes.HasValue)
            {
                cacheEntryOptions.SetAbsoluteExpiration(TimeSpan.FromMinutes(_cacheSettings.AbsoluteExpirationMinutes.Value));
            }

            // Set sliding expiration if configured
            if (_cacheSettings.SlidingExpirationMinutes.HasValue)
            {
                cacheEntryOptions.SetSlidingExpiration(TimeSpan.FromMinutes(_cacheSettings.SlidingExpirationMinutes.Value));
            }

            _cache.Set(cacheKey, cachedEntry, cacheEntryOptions);
            _logger.LogDebug("Cached upload metadata for {UploadId} (size: {Size} bytes)", upload.UploadId, entrySize);
        }
        catch (Exception ex)
        {
            // Don't fail the operation if caching fails
            _logger.LogWarning(ex, "Failed to cache upload metadata for {UploadId}", upload.UploadId);
        }

        await Task.CompletedTask;
    }

    private void InvalidateCache(string uploadId)
    {
        if (_cache == null)
        {
            return;
        }

        var cacheKey = GetCacheKey(uploadId);
        _cache.Remove(cacheKey);
    }

    private static string GetCacheKey(string uploadId)
    {
        return $"upload:{uploadId}";
    }

    private class CachedUpload
    {
        public required MultipartUpload Upload { get; init; }
        public required DateTime MetadataFileLastModified { get; init; }

        public long EstimateSize()
        {
            long size = 200; // Base overhead

            size += (Upload.UploadId?.Length ?? 0) * 2;
            size += (Upload.BucketName?.Length ?? 0) * 2;
            size += (Upload.Key?.Length ?? 0) * 2;
            size += (Upload.ContentType?.Length ?? 0) * 2;
            size += (Upload.ChecksumAlgorithm?.Length ?? 0) * 2;

            // Metadata dictionary
            if (Upload.Metadata != null)
            {
                foreach (var kvp in Upload.Metadata)
                {
                    size += (kvp.Key.Length + kvp.Value.Length) * 2 + 32;
                }
            }

            // Parts dictionary
            if (Upload.Parts != null)
            {
                foreach (var part in Upload.Parts)
                {
                    size += 100; // PartMetadata overhead
                    var metadata = part.Value;
                    size += (metadata.ChecksumCRC32?.Length ?? 0) * 2;
                    size += (metadata.ChecksumCRC32C?.Length ?? 0) * 2;
                    size += (metadata.ChecksumCRC64NVME?.Length ?? 0) * 2;
                    size += (metadata.ChecksumSHA1?.Length ?? 0) * 2;
                    size += (metadata.ChecksumSHA256?.Length ?? 0) * 2;
                }
            }

            return size;
        }
    }
}