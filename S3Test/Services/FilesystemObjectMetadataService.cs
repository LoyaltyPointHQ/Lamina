using System.Text.Json;
using S3Test.Models;

namespace S3Test.Services;

public class FilesystemObjectMetadataService : IObjectMetadataService
{
    private readonly string _metadataDirectory;
    private readonly IBucketServiceFacade _bucketService;
    private readonly IFileSystemLockManager _lockManager;
    private readonly ILogger<FilesystemObjectMetadataService> _logger;

    public FilesystemObjectMetadataService(
        IConfiguration configuration,
        IBucketServiceFacade bucketService,
        IFileSystemLockManager lockManager,
        ILogger<FilesystemObjectMetadataService> logger)
    {
        _metadataDirectory = configuration["FilesystemStorage:MetadataDirectory"] ?? "/var/s3test/metadata";
        _bucketService = bucketService;
        _lockManager = lockManager;
        _logger = logger;

        Directory.CreateDirectory(_metadataDirectory);
    }

    public async Task<S3Object?> StoreMetadataAsync(string bucketName, string key, string etag, long size, PutObjectRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var metadataPath = GetMetadataPath(bucketName, key);
        var metadataDir = Path.GetDirectoryName(metadataPath)!;
        Directory.CreateDirectory(metadataDir);

        var s3Object = new S3Object
        {
            Key = key,
            BucketName = bucketName,
            Size = size,
            LastModified = DateTime.UtcNow,
            ETag = etag,
            ContentType = request?.ContentType ?? "application/octet-stream",
            Metadata = request?.Metadata ?? new Dictionary<string, string>()
        };

        var metadata = new S3ObjectMetadata
        {
            Key = key,
            BucketName = bucketName,
            LastModified = s3Object.LastModified,
            ETag = etag,
            ContentType = s3Object.ContentType,
            Metadata = s3Object.Metadata
        };

        var json = JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true });

        await _lockManager.WriteFileAsync(metadataPath, async _ =>
        {
            await File.WriteAllTextAsync(metadataPath, json, cancellationToken);
            return json;
        }, cancellationToken);

        return s3Object;
    }

    public async Task<S3ObjectInfo?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var metadataPath = GetMetadataPath(bucketName, key);
        var dataPath = GetDataPath(bucketName, key);

        if (!File.Exists(metadataPath) || !File.Exists(dataPath))
        {
            return null;
        }

        var metadata = await _lockManager.ReadFileAsync(metadataPath, async path =>
        {
            var json = await File.ReadAllTextAsync(path, cancellationToken);
            return JsonSerializer.Deserialize<S3ObjectMetadata>(json);
        }, cancellationToken);

        if (metadata == null)
        {
            return null;
        }

        // Always get size from filesystem
        var fileInfo = new FileInfo(dataPath);

        return new S3ObjectInfo
        {
            Key = metadata.Key,
            LastModified = metadata.LastModified,
            ETag = metadata.ETag,
            Size = fileInfo.Length,
            ContentType = metadata.ContentType,
            Metadata = metadata.Metadata
        };
    }

    public async Task<bool> DeleteMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var metadataPath = GetMetadataPath(bucketName, key);
        var result = await _lockManager.DeleteFileAsync(metadataPath, cancellationToken);

        // Clean up empty directories
        try
        {
            var directory = Path.GetDirectoryName(metadataPath);
            while (!string.IsNullOrEmpty(directory) &&
                   directory.StartsWith(_metadataDirectory) &&
                   directory != _metadataDirectory)
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
        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return new ListObjectsResponse
            {
                IsTruncated = false,
                Contents = new List<S3ObjectInfo>()
            };
        }

        var bucketMetadataDir = Path.Combine(_metadataDirectory, bucketName);
        var bucketDataDir = Path.Combine(Directory.GetParent(_metadataDirectory)!.FullName, "data", bucketName);

        if (!Directory.Exists(bucketMetadataDir))
        {
            return new ListObjectsResponse
            {
                IsTruncated = false,
                Contents = new List<S3ObjectInfo>()
            };
        }

        var objects = new List<S3ObjectInfo>();
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

    private string GetMetadataPath(string bucketName, string key)
    {
        return Path.Combine(_metadataDirectory, bucketName, $"{key}.json");
    }

    private string GetDataPath(string bucketName, string key)
    {
        var dataDirectory = Directory.GetParent(_metadataDirectory)!.FullName;
        return Path.Combine(dataDirectory, "data", bucketName, key);
    }

    private class S3ObjectMetadata
    {
        public required string Key { get; set; }
        public required string BucketName { get; set; }
        public DateTime LastModified { get; set; }
        public required string ETag { get; set; }
        public string ContentType { get; set; } = "application/octet-stream";
        public Dictionary<string, string> Metadata { get; set; } = new();
    }
}