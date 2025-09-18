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

    public FilesystemMultipartUploadService(
        IConfiguration configuration,
        IObjectService objectService,
        ILogger<FilesystemMultipartUploadService> logger)
    {
        _metadataDirectory = configuration["FilesystemStorage:MetadataDirectory"] ?? "/var/s3test/metadata";
        _tempDirectory = Path.Combine(_metadataDirectory, "_multipart_uploads");
        _objectService = objectService;
        _logger = logger;

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
        var json = JsonSerializer.Serialize(upload, new JsonSerializerOptions { WriteIndented = true });
        await File.WriteAllTextAsync(metadataPath, json, cancellationToken);

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
        await File.WriteAllBytesAsync(partPath, data, cancellationToken);

        var etag = ComputeETag(data);

        var json = await File.ReadAllTextAsync(metadataPath, cancellationToken);
        var upload = JsonSerializer.Deserialize<MultipartUploadState>(json)!;

        upload.Parts.RemoveAll(p => p.PartNumber == partNumber);
        upload.Parts.Add(new MultipartUploadPart
        {
            PartNumber = partNumber,
            ETag = etag,
            Size = data.Length,
            LastModified = DateTime.UtcNow
        });

        json = JsonSerializer.Serialize(upload, new JsonSerializerOptions { WriteIndented = true });
        await File.WriteAllTextAsync(metadataPath, json, cancellationToken);

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

        var json = await File.ReadAllTextAsync(metadataPath, cancellationToken);
        var upload = JsonSerializer.Deserialize<MultipartUploadState>(json)!;

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
            try
            {
                Directory.Delete(uploadPath, recursive: true);
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

    public Task<bool> AbortMultipartUploadAsync(
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default)
    {
        var uploadPath = GetUploadPath(uploadId);

        if (!Directory.Exists(uploadPath))
        {
            return Task.FromResult(false);
        }

        try
        {
            Directory.Delete(uploadPath, recursive: true);
            _logger.LogInformation("Aborted multipart upload {UploadId}", uploadId);
            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error aborting upload {UploadId}", uploadId);
            return Task.FromResult(false);
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

        var json = await File.ReadAllTextAsync(metadataPath, cancellationToken);
        var upload = JsonSerializer.Deserialize<MultipartUploadState>(json)!;

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
                    var json = await File.ReadAllTextAsync(metadataPath, cancellationToken);
                    var upload = JsonSerializer.Deserialize<MultipartUploadState>(json)!;

                    if (upload.BucketName == bucketName)
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
}