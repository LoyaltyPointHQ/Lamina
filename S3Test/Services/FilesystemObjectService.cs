using System.IO.Pipelines;
using System.Security.Cryptography;
using System.Text.Json;
using S3Test.Models;

namespace S3Test.Services;

public class FilesystemObjectService : IObjectService
{
    private readonly string _dataDirectory;
    private readonly string _metadataDirectory;
    private readonly ILogger<FilesystemObjectService> _logger;
    private readonly IFileSystemLockManager _lockManager;

    public FilesystemObjectService(
        IConfiguration configuration,
        ILogger<FilesystemObjectService> logger,
        IFileSystemLockManager lockManager)
    {
        _dataDirectory = configuration["FilesystemStorage:DataDirectory"] ?? "/var/s3test/data";
        _metadataDirectory = configuration["FilesystemStorage:MetadataDirectory"] ?? "/var/s3test/metadata";
        _logger = logger;
        _lockManager = lockManager;

        Directory.CreateDirectory(_dataDirectory);
        Directory.CreateDirectory(_metadataDirectory);
    }

    public async Task<S3Object?> PutObjectAsync(string bucketName, string key, PipeReader dataReader, PutObjectRequest? request = null, CancellationToken cancellationToken = default)
    {
        try
        {
            var dataPath = GetDataPath(bucketName, key);
            var metadataPath = GetMetadataPath(bucketName, key);

            var dataDir = Path.GetDirectoryName(dataPath)!;
            var metadataDir = Path.GetDirectoryName(metadataPath)!;
            Directory.CreateDirectory(dataDir);
            Directory.CreateDirectory(metadataDir);

            using var memoryStream = new MemoryStream();
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await dataReader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (buffer.IsEmpty && result.IsCompleted)
                {
                    break;
                }

                foreach (var memory in buffer)
                {
                    await memoryStream.WriteAsync(memory, cancellationToken);
                }

                dataReader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }

            var data = memoryStream.ToArray();
            // Write data file directly (no lock needed for new files)
            await File.WriteAllBytesAsync(dataPath, data, cancellationToken);

            var etag = ComputeETag(data);
            var s3Object = new S3Object
            {
                Key = key,
                BucketName = bucketName,
                Size = data.Length,
                LastModified = DateTime.UtcNow,
                ETag = etag,
                ContentType = request?.ContentType ?? "application/octet-stream",
                Metadata = request?.Metadata ?? new Dictionary<string, string>()
            };

            var metadata = new ObjectMetadata
            {
                Key = key,
                BucketName = bucketName,
                LastModified = s3Object.LastModified,
                ETag = s3Object.ETag,
                ContentType = s3Object.ContentType,
                UserMetadata = s3Object.Metadata
            };

            // Write metadata with lock
            await _lockManager.WriteFileAsync(metadataPath, _ =>
            {
                return Task.FromResult(JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true }));
            }, cancellationToken);

            _logger.LogInformation("Stored object {Key} in bucket {BucketName} to filesystem", key, bucketName);
            return s3Object;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error putting object {Key} in bucket {BucketName}", key, bucketName);
            return null;
        }
    }

    public async Task<GetObjectResponse?> GetObjectAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);

        if (!File.Exists(dataPath))
        {
            return null;
        }

        // Read data directly
        var data = await File.ReadAllBytesAsync(dataPath, cancellationToken);
        var fileInfo = new FileInfo(dataPath);

        // Load or create metadata without locks (it's thread-safe on its own)
        var metadata = await LoadOrCreateMetadataAsync(bucketName, key, data, fileInfo, cancellationToken);

        return new GetObjectResponse
        {
            Data = data,
            ContentType = metadata.ContentType,
            ContentLength = fileInfo.Length,
            ETag = metadata.ETag,
            LastModified = metadata.LastModified,
            Metadata = metadata.UserMetadata ?? new Dictionary<string, string>()
        };
    }

    public async Task<bool> WriteObjectToPipeAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);

        if (!File.Exists(dataPath))
        {
            return false;
        }

        try
        {
            using var fileStream = File.OpenRead(dataPath);
            const int bufferSize = 4096;

            while (!cancellationToken.IsCancellationRequested)
            {
                var memory = writer.GetMemory(bufferSize);
                var bytesRead = await fileStream.ReadAsync(memory, cancellationToken);

                if (bytesRead == 0)
                {
                    break;
                }

                writer.Advance(bytesRead);
                var result = await writer.FlushAsync(cancellationToken);

                if (result.IsCompleted)
                {
                    break;
                }
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error writing object {Key} from bucket {BucketName} to pipe", key, bucketName);
            return false;
        }
    }

    public async Task<bool> DeleteObjectAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);
        var metadataPath = GetMetadataPath(bucketName, key);

        var exists = File.Exists(dataPath);

        if (exists)
        {
            // Delete data file
            File.Delete(dataPath);
            CleanupEmptyDirectories(Path.GetDirectoryName(dataPath)!, Path.Combine(_dataDirectory, bucketName));
        }

        if (File.Exists(metadataPath))
        {
            // Delete metadata file with lock
            await _lockManager.DeleteFileAsync(metadataPath, cancellationToken);
            CleanupEmptyDirectories(Path.GetDirectoryName(metadataPath)!, Path.Combine(_metadataDirectory, bucketName));
        }

        if (exists)
        {
            _logger.LogInformation("Deleted object {Key} from bucket {BucketName}", key, bucketName);
        }

        return exists;
    }

    public async Task<ListObjectsResponse> ListObjectsAsync(string bucketName, ListObjectsRequest? request = null, CancellationToken cancellationToken = default)
    {
        var response = new ListObjectsResponse
        {
            Prefix = request?.Prefix,
            Delimiter = request?.Delimiter,
            MaxKeys = request?.MaxKeys ?? 1000
        };

        var bucketDataPath = Path.Combine(_dataDirectory, bucketName);
        if (!Directory.Exists(bucketDataPath))
        {
            return response;
        }

        // List all files in the data directory as the source of truth
        var allDataFiles = Directory.GetFiles(bucketDataPath, "*", SearchOption.AllDirectories);
        var objects = new List<S3ObjectInfo>();

        foreach (var dataFile in allDataFiles)
        {
            try
            {
                // Convert file path back to object key
                var relativePath = Path.GetRelativePath(bucketDataPath, dataFile);
                var key = relativePath.Replace(Path.DirectorySeparatorChar, '/');

                if (string.IsNullOrEmpty(request?.Prefix) || key.StartsWith(request.Prefix))
                {
                    var fileInfo = new FileInfo(dataFile);
                    // Load or create metadata without locks
                    var metadata = await LoadOrCreateMetadataAsync(bucketName, key, null, fileInfo, cancellationToken);

                    objects.Add(new S3ObjectInfo
                    {
                        Key = key,
                        Size = fileInfo.Length,
                        LastModified = metadata.LastModified,
                        ETag = metadata.ETag
                    });
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing data file {File}", dataFile);
            }
        }

        objects = objects.OrderBy(o => o.Key).ToList();

        if (request?.ContinuationToken != null)
        {
            objects = objects.Where(o => string.Compare(o.Key, request.ContinuationToken, StringComparison.Ordinal) > 0).ToList();
        }

        if (objects.Count > response.MaxKeys)
        {
            response.IsTruncated = true;
            response.NextContinuationToken = objects[response.MaxKeys - 1].Key;
            objects = objects.Take(response.MaxKeys).ToList();
        }

        response.Contents = objects;
        return response;
    }

    public Task<bool> ObjectExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);
        return Task.FromResult(File.Exists(dataPath));
    }

    public void CleanupBucketDirectories(string bucketName)
    {
        try
        {
            var bucketDataPath = Path.Combine(_dataDirectory, bucketName);
            var bucketMetadataPath = Path.Combine(_metadataDirectory, bucketName);

            if (Directory.Exists(bucketDataPath))
            {
                try
                {
                    Directory.Delete(bucketDataPath, recursive: true);
                    _logger.LogInformation("Deleted bucket data directory: {Directory}", bucketDataPath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to delete bucket data directory: {Directory}", bucketDataPath);
                }
            }

            if (Directory.Exists(bucketMetadataPath))
            {
                try
                {
                    Directory.Delete(bucketMetadataPath, recursive: true);
                    _logger.LogInformation("Deleted bucket metadata directory: {Directory}", bucketMetadataPath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to delete bucket metadata directory: {Directory}", bucketMetadataPath);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error cleaning up bucket directories for {BucketName}", bucketName);
        }
    }

    public async Task<S3ObjectInfo?> GetObjectInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);

        if (!File.Exists(dataPath))
        {
            return null;
        }

        var fileInfo = new FileInfo(dataPath);

        // Load or create metadata without locks
        var metadata = await LoadOrCreateMetadataAsync(bucketName, key, null, fileInfo, cancellationToken);

        return new S3ObjectInfo
        {
            Key = metadata.Key,
            Size = fileInfo.Length,
            LastModified = metadata.LastModified,
            ETag = metadata.ETag,
            ContentType = metadata.ContentType,
            Metadata = metadata.UserMetadata ?? new Dictionary<string, string>()
        };
    }

    private string GetDataPath(string bucketName, string key)
    {
        var safePath = Path.Combine(bucketName, key.Replace('/', Path.DirectorySeparatorChar));
        return Path.Combine(_dataDirectory, safePath);
    }

    private string GetMetadataPath(string bucketName, string key)
    {
        var safePath = Path.Combine(bucketName, key.Replace('/', Path.DirectorySeparatorChar) + ".json");
        return Path.Combine(_metadataDirectory, safePath);
    }

    private static string ComputeETag(byte[] data)
    {
        using var md5 = MD5.Create();
        var hash = md5.ComputeHash(data);
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }

    private async Task<ObjectMetadata> LoadOrCreateMetadataAsync(string bucketName, string key, byte[]? data, FileInfo fileInfo, CancellationToken cancellationToken)
    {
        var metadataPath = GetMetadataPath(bucketName, key);

        // Try to load existing metadata WITHOUT lock (caller should handle locking if needed)
        if (File.Exists(metadataPath))
        {
            try
            {
                var json = await File.ReadAllTextAsync(metadataPath, cancellationToken);
                var existingMetadata = JsonSerializer.Deserialize<ObjectMetadata>(json);
                if (existingMetadata != null)
                {
                    return existingMetadata;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to load metadata for {Key} in bucket {BucketName}, will recreate", key, bucketName);
            }
        }

        // Create metadata lazily
        string etag;
        if (data != null)
        {
            etag = ComputeETag(data);
        }
        else
        {
            // Compute ETag from file if data wasn't provided
            using var stream = fileInfo.OpenRead();
            using var md5 = MD5.Create();
            var hash = await md5.ComputeHashAsync(stream, cancellationToken);
            etag = BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
        }

        var metadata = new ObjectMetadata
        {
            Key = key,
            BucketName = bucketName,
            LastModified = fileInfo.LastWriteTimeUtc,
            ETag = etag,
            ContentType = GuessContentType(key),
            UserMetadata = new Dictionary<string, string>()
        };

        // Try to save metadata WITHOUT lock (best effort - caller handles locking)
        try
        {
            var metadataDir = Path.GetDirectoryName(metadataPath)!;
            Directory.CreateDirectory(metadataDir);

            var metadataJson = JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(metadataPath, metadataJson, cancellationToken);
            _logger.LogInformation("Created metadata for {Key} in bucket {BucketName}", key, bucketName);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to save metadata for {Key} in bucket {BucketName}, continuing with generated metadata", key, bucketName);
        }

        return metadata;
    }

    private void CleanupEmptyDirectories(string startPath, string stopPath)
    {
        try
        {
            // Normalize paths to avoid issues with path comparison
            startPath = Path.GetFullPath(startPath);
            stopPath = Path.GetFullPath(stopPath);

            var currentPath = startPath;

            // Walk up the directory tree until we reach the stopPath or can't go further
            while (!string.IsNullOrEmpty(currentPath) &&
                   !string.Equals(currentPath, stopPath, StringComparison.OrdinalIgnoreCase) &&
                   currentPath.StartsWith(stopPath, StringComparison.OrdinalIgnoreCase))
            {
                if (Directory.Exists(currentPath))
                {
                    // Check if directory is empty (no files and no directories)
                    var files = Directory.GetFiles(currentPath);
                    var directories = Directory.GetDirectories(currentPath);

                    if (files.Length == 0 && directories.Length == 0)
                    {
                        try
                        {
                            Directory.Delete(currentPath);
                            _logger.LogDebug("Cleaned up empty directory: {Directory}", currentPath);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Failed to delete empty directory: {Directory}", currentPath);
                            break; // Stop trying to delete parent directories
                        }
                    }
                    else
                    {
                        // Directory is not empty, stop cleanup
                        break;
                    }
                }

                // Move to parent directory
                var parentPath = Path.GetDirectoryName(currentPath);
                if (string.Equals(currentPath, parentPath, StringComparison.OrdinalIgnoreCase))
                {
                    // Reached root or no more parent directories
                    break;
                }
                currentPath = parentPath;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during directory cleanup starting from {StartPath}", startPath);
        }
    }

    private static string GuessContentType(string key)
    {
        var extension = Path.GetExtension(key).ToLowerInvariant();
        return extension switch
        {
            ".txt" => "text/plain",
            ".html" or ".htm" => "text/html",
            ".css" => "text/css",
            ".js" => "application/javascript",
            ".json" => "application/json",
            ".xml" => "application/xml",
            ".pdf" => "application/pdf",
            ".jpg" or ".jpeg" => "image/jpeg",
            ".png" => "image/png",
            ".gif" => "image/gif",
            ".svg" => "image/svg+xml",
            ".mp3" => "audio/mpeg",
            ".mp4" => "video/mp4",
            ".zip" => "application/zip",
            _ => "application/octet-stream"
        };
    }

    private class ObjectMetadata
    {
        public string Key { get; set; } = string.Empty;
        public string BucketName { get; set; } = string.Empty;
        public DateTime LastModified { get; set; }
        public string ETag { get; set; } = string.Empty;
        public string ContentType { get; set; } = string.Empty;
        public Dictionary<string, string>? UserMetadata { get; set; }
    }
}