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

    public FilesystemObjectService(IConfiguration configuration, ILogger<FilesystemObjectService> logger)
    {
        _dataDirectory = configuration["FilesystemStorage:DataDirectory"] ?? "/var/s3test/data";
        _metadataDirectory = configuration["FilesystemStorage:MetadataDirectory"] ?? "/var/s3test/metadata";
        _logger = logger;

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

            var metadataJson = JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(metadataPath, metadataJson, cancellationToken);

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

        var data = await File.ReadAllBytesAsync(dataPath, cancellationToken);
        var fileInfo = new FileInfo(dataPath);

        // Try to load metadata, or create it lazily
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

    public Task<bool> DeleteObjectAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);
        var metadataPath = GetMetadataPath(bucketName, key);

        var exists = File.Exists(dataPath);

        if (File.Exists(dataPath))
        {
            File.Delete(dataPath);
        }

        if (File.Exists(metadataPath))
        {
            File.Delete(metadataPath);
        }

        if (exists)
        {
            _logger.LogInformation("Deleted object {Key} from bucket {BucketName}", key, bucketName);
        }

        return Task.FromResult(exists);
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

                    // Try to load metadata, or create it lazily
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

    public async Task<S3ObjectInfo?> GetObjectInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);

        if (!File.Exists(dataPath))
        {
            return null;
        }

        var fileInfo = new FileInfo(dataPath);

        // Try to load metadata, or create it lazily (without reading full file)
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

        // Try to load existing metadata
        if (File.Exists(metadataPath))
        {
            try
            {
                var metadataJson = await File.ReadAllTextAsync(metadataPath, cancellationToken);
                var existingMetadata = JsonSerializer.Deserialize<ObjectMetadata>(metadataJson);
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

        // Try to save metadata (best effort)
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