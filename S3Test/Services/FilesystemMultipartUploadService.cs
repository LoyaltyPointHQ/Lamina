using System.IO.Pipelines;
using System.Security.Cryptography;
using System.Text.Json;
using S3Test.Models;

namespace S3Test.Services;

public class FilesystemMultipartUploadService : IMultipartUploadService
{
    private readonly string _metadataDirectory;
    private readonly string _tempDirectory;
    private readonly IObjectService _objectService;
    private readonly ILogger<FilesystemMultipartUploadService> _logger;
    private readonly IFileSystemLockManager _lockManager;

    public FilesystemMultipartUploadService(
        IConfiguration configuration,
        IObjectService objectService,
        ILogger<FilesystemMultipartUploadService> logger,
        IFileSystemLockManager lockManager)
    {
        _metadataDirectory = configuration["FilesystemStorage:MetadataDirectory"] ?? "/var/s3test/metadata";
        _tempDirectory = Path.Combine(_metadataDirectory, "_multipart_uploads");
        _objectService = objectService;
        _logger = logger;
        _lockManager = lockManager;

        Directory.CreateDirectory(_tempDirectory);
    }

    public async Task<InitiateMultipartUploadResponse> InitiateMultipartUploadAsync(
        string bucketName,
        InitiateMultipartUploadRequest request,
        CancellationToken cancellationToken = default)
    {
        var uploadId = Guid.NewGuid().ToString();
        var uploadPath = GetUploadPath(uploadId);

        var upload = new MultipartUploadState
        {
            UploadId = uploadId,
            BucketName = bucketName,
            Key = request.Key,
            ContentType = request.ContentType ?? "application/octet-stream",
            Metadata = request.Metadata ?? new Dictionary<string, string>(),
            InitiatedAt = DateTime.UtcNow,
            Parts = new List<MultipartUploadPart>()
        };

        Directory.CreateDirectory(uploadPath);
        var metadataPath = Path.Combine(uploadPath, "metadata.json");

        // Write upload metadata with lock
        await _lockManager.WriteFileAsync(metadataPath, _ =>
        {
            return Task.FromResult(JsonSerializer.Serialize(upload, new JsonSerializerOptions { WriteIndented = true }));
        }, cancellationToken);

        _logger.LogInformation("Initiated multipart upload {UploadId} for {Key} in bucket {BucketName}", uploadId, request.Key, bucketName);

        return new InitiateMultipartUploadResponse
        {
            BucketName = bucketName,
            Key = request.Key,
            UploadId = uploadId
        };
    }

    public async Task<UploadPartResponse> UploadPartAsync(
        string bucketName,
        string key,
        string uploadId,
        int partNumber,
        PipeReader dataReader,
        CancellationToken cancellationToken = default)
    {
        var uploadPath = GetUploadPath(uploadId);
        var metadataPath = Path.Combine(uploadPath, "metadata.json");

        if (!File.Exists(metadataPath))
        {
            throw new InvalidOperationException($"Upload {uploadId} not found");
        }

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
        var partPath = Path.Combine(uploadPath, $"part_{partNumber}");

        // Write part data directly
        await File.WriteAllBytesAsync(partPath, data, cancellationToken);

        var etag = ComputeETag(data);

        // Use optimistic concurrency with retries to handle concurrent part uploads
        const int maxRetries = 10;
        for (int retry = 0; retry < maxRetries; retry++)
        {
            try
            {
                // Try to update metadata with a shorter timeout to detect contention quickly
                var success = await TryUpdatePartMetadataAsync(metadataPath, uploadId, partNumber, etag, data.Length, cancellationToken);
                if (success)
                {
                    break;
                }

                // If we're on the last retry, throw
                if (retry == maxRetries - 1)
                {
                    throw new InvalidOperationException($"Failed to update metadata for upload {uploadId} after {maxRetries} retries");
                }

                // Exponential backoff with jitter
                var delay = TimeSpan.FromMilliseconds(Math.Min(100 * Math.Pow(2, retry), 1000) + Random.Shared.Next(0, 100));
                await Task.Delay(delay, cancellationToken);
            }
            catch (TimeoutException) when (retry < maxRetries - 1)
            {
                // On timeout, retry with backoff
                var delay = TimeSpan.FromMilliseconds(Math.Min(100 * Math.Pow(2, retry), 1000) + Random.Shared.Next(0, 100));
                await Task.Delay(delay, cancellationToken);
            }
        }

        _logger.LogInformation("Uploaded part {PartNumber} of upload {UploadId}", partNumber, uploadId);

        return new UploadPartResponse
        {
            PartNumber = partNumber,
            ETag = etag
        };
    }

    public async Task<CompleteMultipartUploadResponse?> CompleteMultipartUploadAsync(
        string bucketName,
        string key,
        CompleteMultipartUploadRequest request,
        CancellationToken cancellationToken = default)
    {
        var uploadPath = GetUploadPath(request.UploadId);
        var metadataPath = Path.Combine(uploadPath, "metadata.json");

        if (!File.Exists(metadataPath))
        {
            return null;
        }

        // Read upload metadata with lock
        var upload = await _lockManager.ReadFileAsync<MultipartUploadState>(metadataPath, json =>
        {
            return Task.FromResult(JsonSerializer.Deserialize<MultipartUploadState>(json) ?? throw new InvalidOperationException("Failed to deserialize upload metadata"));
        }, cancellationToken);

        using var combinedStream = new MemoryStream();
        var sortedParts = request.Parts.OrderBy(p => p.PartNumber).ToList();

        foreach (var partRequest in sortedParts)
        {
            var partPath = Path.Combine(uploadPath, $"part_{partRequest.PartNumber}");
            if (!File.Exists(partPath))
            {
                _logger.LogError("Part {PartNumber} not found for upload {UploadId}", partRequest.PartNumber, request.UploadId);
                return null;
            }

            // Read part data directly
            var partData = await File.ReadAllBytesAsync(partPath, cancellationToken);
            await combinedStream.WriteAsync(partData, cancellationToken);
        }

        combinedStream.Position = 0;
        var pipeReader = PipeReader.Create(combinedStream);

        var putRequest = new PutObjectRequest
        {
            Key = key,
            ContentType = upload.ContentType,
            Metadata = upload.Metadata
        };

        var s3Object = await _objectService.PutObjectAsync(bucketName, key, pipeReader, putRequest, cancellationToken);

        if (s3Object != null)
        {
            // Delete upload directory
            try
            {
                if (Directory.Exists(uploadPath))
                {
                    Directory.Delete(uploadPath, recursive: true);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error deleting upload directory {UploadPath}", uploadPath);
            }

            _logger.LogInformation("Completed multipart upload {UploadId} for {Key}", request.UploadId, key);

            return new CompleteMultipartUploadResponse
            {
                Location = $"/{bucketName}/{key}",
                BucketName = bucketName,
                Key = key,
                ETag = s3Object.ETag
            };
        }

        return null;
    }

    public async Task<bool> AbortMultipartUploadAsync(
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default)
    {
        var uploadPath = GetUploadPath(uploadId);

        if (!Directory.Exists(uploadPath))
        {
            return false;
        }

        // Delete upload directory
        try
        {
            if (Directory.Exists(uploadPath))
            {
                Directory.Delete(uploadPath, recursive: true);
                _logger.LogInformation("Aborted multipart upload {UploadId}", uploadId);
                return true;
            }
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error aborting upload {UploadId}", uploadId);
            return false;
        }
    }

    public async Task<List<UploadPart>> ListPartsAsync(
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default)
    {
        var uploadPath = GetUploadPath(uploadId);
        var metadataPath = Path.Combine(uploadPath, "metadata.json");

        if (!File.Exists(metadataPath))
        {
            return new List<UploadPart>();
        }

        // Read upload metadata with lock
        var upload = await _lockManager.ReadFileAsync<MultipartUploadState>(metadataPath, json =>
        {
            return Task.FromResult(JsonSerializer.Deserialize<MultipartUploadState>(json) ?? throw new InvalidOperationException("Failed to deserialize upload metadata"));
        }, cancellationToken);

        return upload.Parts
            .OrderBy(p => p.PartNumber)
            .Select(p => new UploadPart
            {
                PartNumber = p.PartNumber,
                ETag = p.ETag,
                Size = p.Size,
                LastModified = p.LastModified
            })
            .ToList();
    }

    public async Task<List<MultipartUpload>> ListMultipartUploadsAsync(
        string bucketName,
        CancellationToken cancellationToken = default)
    {
        var uploads = new List<MultipartUpload>();

        if (!Directory.Exists(_tempDirectory))
        {
            return uploads;
        }

        var uploadDirs = Directory.GetDirectories(_tempDirectory);

        foreach (var uploadDir in uploadDirs)
        {
            var metadataPath = Path.Combine(uploadDir, "metadata.json");
            if (File.Exists(metadataPath))
            {
                try
                {
                    // Read upload metadata with lock
                    var upload = await _lockManager.ReadFileAsync<MultipartUploadState>(metadataPath, json =>
                    {
                        return Task.FromResult(JsonSerializer.Deserialize<MultipartUploadState>(json));
                    }, cancellationToken);

                    if (upload != null && upload.BucketName == bucketName)
                    {
                        uploads.Add(new MultipartUpload
                        {
                            UploadId = upload.UploadId,
                            Key = upload.Key,
                            Initiated = upload.InitiatedAt
                        });
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error reading upload metadata from {Path}", metadataPath);
                }
            }
        }

        return uploads.OrderBy(u => u.Initiated).ToList();
    }

    private string GetUploadPath(string uploadId)
    {
        return Path.Combine(_tempDirectory, uploadId);
    }

    private static string ComputeETag(byte[] data)
    {
        using var md5 = MD5.Create();
        var hash = md5.ComputeHash(data);
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }

    private class MultipartUploadState
    {
        public string UploadId { get; set; } = string.Empty;
        public string BucketName { get; set; } = string.Empty;
        public string Key { get; set; } = string.Empty;
        public string ContentType { get; set; } = string.Empty;
        public Dictionary<string, string> Metadata { get; set; } = new();
        public DateTime InitiatedAt { get; set; }
        public List<MultipartUploadPart> Parts { get; set; } = new();
    }

    private class MultipartUploadPart
    {
        public int PartNumber { get; set; }
        public string ETag { get; set; } = string.Empty;
        public long Size { get; set; }
        public DateTime LastModified { get; set; }
    }

    private async Task<bool> TryUpdatePartMetadataAsync(
        string metadataPath,
        string uploadId,
        int partNumber,
        string etag,
        long size,
        CancellationToken cancellationToken)
    {
        try
        {
            // Use ExecuteWithLockAsync with a shorter timeout for better concurrency detection
            var shortTimeout = TimeSpan.FromSeconds(2); // Short timeout to detect contention quickly
            return await _lockManager.ExecuteWithLockAsync(metadataPath, async () =>
            {
                // Read current metadata
                if (!File.Exists(metadataPath))
                {
                    throw new InvalidOperationException($"Upload {uploadId} metadata not found");
                }

                var json = await File.ReadAllTextAsync(metadataPath, cancellationToken);
                var upload = JsonSerializer.Deserialize<MultipartUploadState>(json);
                if (upload == null)
                {
                    throw new InvalidOperationException($"Failed to deserialize upload {uploadId} metadata");
                }

                // Update parts list
                upload.Parts.RemoveAll(p => p.PartNumber == partNumber);
                upload.Parts.Add(new MultipartUploadPart
                {
                    PartNumber = partNumber,
                    ETag = etag,
                    Size = size,
                    LastModified = DateTime.UtcNow
                });

                // Write back atomically
                var updatedJson = JsonSerializer.Serialize(upload, new JsonSerializerOptions { WriteIndented = true });
                var tempFile = $"{metadataPath}.tmp.{Guid.NewGuid():N}";
                try
                {
                    await File.WriteAllTextAsync(tempFile, updatedJson, cancellationToken);
                    File.Move(tempFile, metadataPath, overwrite: true);
                }
                finally
                {
                    // Cleanup temp file if it exists
                    if (File.Exists(tempFile))
                    {
                        try { File.Delete(tempFile); } catch { }
                    }
                }

                return true;
            }, isWrite: true, cancellationToken, timeout: shortTimeout);
        }
        catch (TimeoutException)
        {
            // Let the caller handle timeout with retry
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to update part metadata for upload {UploadId}, part {PartNumber}", uploadId, partNumber);
            return false;
        }
    }
}