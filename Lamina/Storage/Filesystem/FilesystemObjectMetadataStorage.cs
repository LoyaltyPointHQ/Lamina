using System.Runtime.CompilerServices;
using System.Text.Json;
using Lamina.Models;
using Lamina.Storage.Abstract;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
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
    private readonly NetworkFileSystemHelper _networkHelper;
    private readonly ILogger<FilesystemObjectMetadataStorage> _logger;

    public FilesystemObjectMetadataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        IBucketStorageFacade bucketStorage,
        IFileSystemLockManager lockManager,
        NetworkFileSystemHelper networkHelper,
        ILogger<FilesystemObjectMetadataStorage> logger)
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _metadataMode = settings.MetadataMode;
        _metadataDirectory = settings.MetadataDirectory;
        _inlineMetadataDirectoryName = settings.InlineMetadataDirectoryName;
        _bucketStorage = bucketStorage;
        _lockManager = lockManager;
        _networkHelper = networkHelper;
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
            Key = key,
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

            if (!string.IsNullOrEmpty(directory) &&
                directory.StartsWith(rootDir!) &&
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

    public async IAsyncEnumerable<(string bucketName, string key)> ListAllMetadataKeysAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_metadataMode == MetadataStorageMode.SeparateDirectory)
        {
            await foreach (var entry in ListMetadataFromSeparateDirectoryAsync(cancellationToken))
            {
                yield return entry;
            }
        }
        else
        {
            await foreach (var entry in ListMetadataFromInlineDirectoriesAsync(cancellationToken))
            {
                yield return entry;
            }
        }
    }

    private async IAsyncEnumerable<(string bucketName, string key)> ListMetadataFromSeparateDirectoryAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(_metadataDirectory))
        {
            yield break;
        }

        var bucketDirectories = Directory.GetDirectories(_metadataDirectory);

        foreach (var bucketDir in bucketDirectories)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var bucketName = Path.GetFileName(bucketDir);

            await foreach (var key in EnumerateMetadataFilesInDirectoryAsync(bucketDir, cancellationToken))
            {
                yield return (bucketName, key);
            }
        }
    }

    private async IAsyncEnumerable<(string bucketName, string key)> ListMetadataFromInlineDirectoriesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(_dataDirectory))
        {
            yield break;
        }

        var bucketDirectories = Directory.GetDirectories(_dataDirectory);

        foreach (var bucketDir in bucketDirectories)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var bucketName = Path.GetFileName(bucketDir);

            await foreach (var key in EnumerateInlineMetadataFilesAsync(bucketDir, "", cancellationToken))
            {
                yield return (bucketName, key);
            }
        }
    }

    private async IAsyncEnumerable<string> EnumerateMetadataFilesInDirectoryAsync(string directory, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var files = Directory.GetFiles(directory, "*.json", SearchOption.AllDirectories);

        foreach (var file in files)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Convert back from metadata path to object key
            var relativePath = Path.GetRelativePath(directory, file);
            // Remove .json extension
            var key = relativePath.EndsWith(".json") ? relativePath[..^5] : relativePath;
            // Normalize path separators for S3 (always forward slashes)
            key = key.Replace(Path.DirectorySeparatorChar, '/');

            yield return key;
        }

        await Task.CompletedTask; // Satisfy async requirement
    }

    private async IAsyncEnumerable<string> EnumerateInlineMetadataFilesAsync(string currentDirectory, string keyPrefix, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Look for metadata directory in current directory
        var metadataDir = Path.Combine(currentDirectory, _inlineMetadataDirectoryName);
        if (Directory.Exists(metadataDir))
        {
            var files = Directory.GetFiles(metadataDir, "*.json");
            foreach (var file in files)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var fileName = Path.GetFileName(file);
                // Remove .json extension
                var objectName = fileName.EndsWith(".json") ? fileName[..^5] : fileName;
                var key = string.IsNullOrEmpty(keyPrefix) ? objectName : $"{keyPrefix}/{objectName}";

                yield return key;
            }
        }

        // Recursively check subdirectories (but skip metadata directories)
        var subdirectories = Directory.GetDirectories(currentDirectory);
        foreach (var subdir in subdirectories)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var dirName = Path.GetFileName(subdir);
            if (dirName == _inlineMetadataDirectoryName)
            {
                continue; // Skip metadata directories
            }

            var newPrefix = string.IsNullOrEmpty(keyPrefix) ? dirName : $"{keyPrefix}/{dirName}";
            await foreach (var key in EnumerateInlineMetadataFilesAsync(subdir, newPrefix, cancellationToken))
            {
                yield return key;
            }
        }

        await Task.CompletedTask; // Satisfy async requirement
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
        public required string BucketName { get; set; }
        public required string ETag { get; set; }
        public string ContentType { get; set; } = "application/octet-stream";
        public Dictionary<string, string> Metadata { get; set; } = new();
    }
}