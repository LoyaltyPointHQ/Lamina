using System.Text.Json;
using S3Test.Models;

namespace S3Test.Services;

public class FilesystemMultipartUploadMetadataService : IMultipartUploadMetadataService
{
    private readonly string _metadataDirectory;
    private readonly IFileSystemLockManager _lockManager;
    private readonly ILogger<FilesystemMultipartUploadMetadataService> _logger;

    public FilesystemMultipartUploadMetadataService(
        IConfiguration configuration,
        IFileSystemLockManager lockManager,
        ILogger<FilesystemMultipartUploadMetadataService> logger)
    {
        _metadataDirectory = configuration["FilesystemStorage:MetadataDirectory"] ?? "/var/s3test/metadata";
        _lockManager = lockManager;
        _logger = logger;

        Directory.CreateDirectory(_metadataDirectory);
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
        Directory.CreateDirectory(uploadDir);

        var json = JsonSerializer.Serialize(upload, new JsonSerializerOptions { WriteIndented = true });

        await _lockManager.WriteFileAsync(uploadMetadataPath, async () =>
        {
            await File.WriteAllTextAsync(uploadMetadataPath, json, cancellationToken);
            return json;
        }, cancellationToken);

        return upload;
    }

    public async Task<MultipartUpload?> GetUploadMetadataAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
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

        return upload;
    }

    public async Task<bool> DeleteUploadMetadataAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadMetadataPath = GetUploadMetadataPath(uploadId);
        return await _lockManager.DeleteFileAsync(uploadMetadataPath, cancellationToken);
    }

    public async Task<List<MultipartUpload>> ListUploadsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var uploads = new List<MultipartUpload>();
        var multipartUploadsDir = Path.Combine(_metadataDirectory, "_multipart_uploads");

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

    public async Task<bool> UploadExistsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var upload = await GetUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);
        return upload != null;
    }

    private string GetUploadMetadataPath(string uploadId)
    {
        return Path.Combine(_metadataDirectory, "_multipart_uploads", uploadId, "upload.metadata.json");
    }
}