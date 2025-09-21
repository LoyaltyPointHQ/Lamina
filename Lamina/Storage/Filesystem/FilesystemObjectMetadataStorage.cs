using System.Text.Json;
using Lamina.Models;
using Lamina.Storage.Abstract;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Locking;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

public class FilesystemObjectMetadataStorage : IObjectMetadataStorage
{
    private readonly string _dataDirectory;
    private readonly string? _metadataDirectory;
    private readonly MetadataStorageMode _metadataMode;
    private readonly string _inlineMetadataDirectoryName;
    private readonly IBucketStorageFacade _bucketStorage;
    private readonly IFileSystemLockManager _lockManager;
    private readonly ILogger<FilesystemObjectMetadataStorage> _logger;

    public FilesystemObjectMetadataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        IBucketStorageFacade bucketStorage,
        IFileSystemLockManager lockManager,
        ILogger<FilesystemObjectMetadataStorage> logger)
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _metadataMode = settings.MetadataMode;
        _metadataDirectory = settings.MetadataDirectory;
        _inlineMetadataDirectoryName = settings.InlineMetadataDirectoryName;
        _bucketStorage = bucketStorage;
        _lockManager = lockManager;
        _logger = logger;

        Directory.CreateDirectory(_dataDirectory);

        if (_metadataMode == MetadataStorageMode.SeparateDirectory)
        {
            if (string.IsNullOrWhiteSpace(_metadataDirectory))
            {
                throw new InvalidOperationException("MetadataDirectory is required when using SeparateDirectory metadata mode");
            }
            Directory.CreateDirectory(_metadataDirectory);
        }
    }

    public async Task<S3Object?> StoreMetadataAsync(string bucketName, string key, string etag, long size, PutObjectRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (!await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var metadataPath = GetMetadataPath(bucketName, key);
        var metadataDir = Path.GetDirectoryName(metadataPath)!;
        Directory.CreateDirectory(metadataDir);

        var metadata = new S3ObjectMetadata
        {
            Key = key,
            BucketName = bucketName,
            ETag = etag,
            ContentType = request?.ContentType ?? "application/octet-stream",
            Metadata = request?.Metadata ?? new Dictionary<string, string>()
        };

        var json = JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true });

        await _lockManager.WriteFileAsync(metadataPath, json, cancellationToken);

        // Get the actual last modified time from the filesystem
        var dataPath = GetDataPath(bucketName, key);
        var fileInfo = new FileInfo(dataPath);

        return new S3Object
        {
            Key = key,
            BucketName = bucketName,
            Size = size,
            LastModified = fileInfo.LastWriteTimeUtc,
            ETag = etag,
            ContentType = metadata.ContentType,
            Metadata = metadata.Metadata
        };
    }

    public async Task<S3ObjectInfo?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var metadataPath = GetMetadataPath(bucketName, key);
        var dataPath = GetDataPath(bucketName, key);

        // Only return metadata if the metadata file exists
        // The facade will handle generating metadata on-the-fly if data exists without metadata
        if (!File.Exists(metadataPath))
        {
            return null;
        }

        // But still verify data exists
        if (!File.Exists(dataPath))
        {
            // Metadata exists but data doesn't - clean up orphaned metadata
            _logger.LogWarning("Found orphaned metadata without data for key {Key} in bucket {BucketName}, cleaning up", key, bucketName);
            await DeleteMetadataAsync(bucketName, key, cancellationToken);
            return null;
        }

        var metadata = await _lockManager.ReadFileAsync(metadataPath, async content =>
        {
            return await Task.FromResult(JsonSerializer.Deserialize<S3ObjectMetadata>(content));
        }, cancellationToken);

        if (metadata == null)
        {
            return null;
        }

        // Always get size and last modified from filesystem
        var fileInfo = new FileInfo(dataPath);

        return new S3ObjectInfo
        {
            Key = metadata.Key,
            LastModified = fileInfo.LastWriteTimeUtc,
            ETag = metadata.ETag,
            Size = fileInfo.Length,
            ContentType = metadata.ContentType,
            Metadata = metadata.Metadata
        };
    }

    public async Task<bool> DeleteMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var metadataPath = GetMetadataPath(bucketName, key);
        var result = await _lockManager.DeleteFile(metadataPath);

        // Clean up empty directories, but preserve the bucket directory
        try
        {
            var directory = Path.GetDirectoryName(metadataPath);
            var rootDir = _metadataMode == MetadataStorageMode.SeparateDirectory
                ? _metadataDirectory
                : _dataDirectory;

            // Determine the bucket directory based on the storage mode
            var bucketDirectory = _metadataMode == MetadataStorageMode.SeparateDirectory
                ? Path.Combine(_metadataDirectory!, bucketName)
                : Path.Combine(_dataDirectory, bucketName);

            while (!string.IsNullOrEmpty(directory) &&
                   directory.StartsWith(rootDir!) &&
                   directory != rootDir &&
                   directory != bucketDirectory)  // Stop at bucket directory
            {
                if (Directory.Exists(directory) && !Directory.EnumerateFileSystemEntries(directory).Any())
                {
                    Directory.Delete(directory);
                    directory = Path.GetDirectoryName(directory);
                }
                else
                {
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to clean up empty directories for path: {MetadataPath}", metadataPath);
        }

        return result;
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

        var objects = new List<S3ObjectInfo>();

        if (_metadataMode == MetadataStorageMode.SeparateDirectory)
        {
            var bucketMetadataDir = Path.Combine(_metadataDirectory!, bucketName);
            if (!Directory.Exists(bucketMetadataDir))
            {
                return new ListObjectsResponse
                {
                    IsTruncated = false,
                    Contents = new List<S3ObjectInfo>()
                };
            }

            var metadataFiles = Directory.GetFiles(bucketMetadataDir, "*.json", SearchOption.AllDirectories);
            foreach (var metadataFile in metadataFiles)
            {
                try
                {
                    var relativePath = Path.GetRelativePath(bucketMetadataDir, metadataFile);
                    var key = relativePath.Replace(".json", "").Replace(Path.DirectorySeparatorChar, '/');

                    if (!string.IsNullOrEmpty(request?.Prefix) && !key.StartsWith(request.Prefix))
                    {
                        continue;
                    }

                    if (!string.IsNullOrEmpty(request?.ContinuationToken) && string.Compare(key, request.ContinuationToken, StringComparison.Ordinal) <= 0)
                    {
                        continue;
                    }

                    var metadata = await GetMetadataAsync(bucketName, key, cancellationToken);
                    if (metadata != null)
                    {
                        objects.Add(metadata);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to read metadata for file: {MetadataFile}", metadataFile);
                }
            }
        }
        else
        {
            // Inline mode: scan for metadata files in .lamina-meta directories
            var bucketDataDir = Path.Combine(_dataDirectory, bucketName);
            if (!Directory.Exists(bucketDataDir))
            {
                return new ListObjectsResponse
                {
                    IsTruncated = false,
                    Contents = new List<S3ObjectInfo>()
                };
            }

            var metadataFiles = Directory.GetFiles(bucketDataDir, "*.json", SearchOption.AllDirectories)
                .Where(f => f.Contains(Path.DirectorySeparatorChar + _inlineMetadataDirectoryName + Path.DirectorySeparatorChar))
                .ToList();

            foreach (var metadataFile in metadataFiles)
            {
                try
                {
                    // Extract the key from the metadata file path
                    // Example: /data/bucket/path/to/.lamina-meta/object.zip.json
                    var metadataDir = Path.GetDirectoryName(metadataFile)!;
                    var metaDirName = Path.GetFileName(metadataDir);

                    if (metaDirName != _inlineMetadataDirectoryName)
                    {
                        continue;
                    }

                    var parentDir = Path.GetDirectoryName(metadataDir)!;
                    var fileName = Path.GetFileName(metadataFile).Replace(".json", "");
                    var relativePath = Path.GetRelativePath(bucketDataDir, parentDir);
                    var key = relativePath == "."
                        ? fileName
                        : Path.Combine(relativePath, fileName).Replace(Path.DirectorySeparatorChar, '/');

                    if (!string.IsNullOrEmpty(request?.Prefix) && !key.StartsWith(request.Prefix))
                    {
                        continue;
                    }

                    if (!string.IsNullOrEmpty(request?.ContinuationToken) && string.Compare(key, request.ContinuationToken, StringComparison.Ordinal) <= 0)
                    {
                        continue;
                    }

                    var metadata = await GetMetadataAsync(bucketName, key, cancellationToken);
                    if (metadata != null)
                    {
                        objects.Add(metadata);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to read metadata for file: {MetadataFile}", metadataFile);
                }
            }
        }

        var ordered = objects.OrderBy(o => o.Key).ToList();
        var maxKeys = request?.MaxKeys ?? 1000;
        var objectsToReturn = ordered.Take(maxKeys + 1).ToList();

        var isTruncated = objectsToReturn.Count > maxKeys;
        if (isTruncated)
        {
            objectsToReturn = objectsToReturn.Take(maxKeys).ToList();
        }

        return new ListObjectsResponse
        {
            Prefix = request?.Prefix,
            MaxKeys = maxKeys,
            IsTruncated = isTruncated,
            NextContinuationToken = isTruncated ? objectsToReturn.LastOrDefault()?.Key : null,
            Contents = objectsToReturn
        };
    }

    public Task<bool> MetadataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var metadataPath = GetMetadataPath(bucketName, key);
        return Task.FromResult(File.Exists(metadataPath));
    }

    public bool IsValidObjectKey(string key)
    {
        // Check if key is null or empty
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        // In inline mode, check that the key doesn't contain metadata directory patterns
        if (_metadataMode == MetadataStorageMode.Inline)
        {
            var separator = '/';  // S3 keys use forward slashes
            var metaDirPattern = $"{separator}{_inlineMetadataDirectoryName}{separator}";
            var metaDirEnd = $"{separator}{_inlineMetadataDirectoryName}";

            // Check if the key contains or ends with the metadata directory
            if (key.Contains(metaDirPattern) || key.EndsWith(metaDirEnd))
            {
                return false;
            }

            // Also check each segment of the path
            var segments = key.Split(separator);
            foreach (var segment in segments)
            {
                if (segment == _inlineMetadataDirectoryName)
                {
                    return false;
                }
            }
        }

        return true;
    }

    private string GetMetadataPath(string bucketName, string key)
    {
        if (_metadataMode == MetadataStorageMode.SeparateDirectory)
        {
            return Path.Combine(_metadataDirectory!, bucketName, $"{key}.json");
        }
        else
        {
            // For inline mode: /bucket/path/to/object.zip -> /bucket/path/to/.lamina-meta/object.zip.json
            var dataPath = Path.Combine(_dataDirectory, bucketName, key);
            var directory = Path.GetDirectoryName(dataPath) ?? Path.Combine(_dataDirectory, bucketName);
            var fileName = Path.GetFileName(dataPath);
            return Path.Combine(directory, _inlineMetadataDirectoryName, $"{fileName}.json");
        }
    }

    private string GetDataPath(string bucketName, string key)
    {
        return Path.Combine(_dataDirectory, bucketName, key);
    }

    private class S3ObjectMetadata
    {
        public required string Key { get; set; }
        public required string BucketName { get; set; }
        public required string ETag { get; set; }
        public string ContentType { get; set; } = "application/octet-stream";
        public Dictionary<string, string> Metadata { get; set; } = new();
    }
}