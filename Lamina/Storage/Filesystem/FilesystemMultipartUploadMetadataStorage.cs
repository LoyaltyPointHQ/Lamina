using System.Text.Json;
using Lamina.Models;
using Lamina.Storage.Abstract;
using Lamina.Storage.Filesystem.Configuration;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

public class FilesystemMultipartUploadMetadataStorage : IMultipartUploadMetadataStorage
{
    private readonly string _dataDirectory;
    private readonly string? _metadataDirectory;
    private readonly MetadataStorageMode _metadataMode;
    private readonly string _inlineMetadataDirectoryName;
    private readonly IFileSystemLockManager _lockManager;
    private readonly ILogger<FilesystemMultipartUploadMetadataStorage> _logger;

    public FilesystemMultipartUploadMetadataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        IFileSystemLockManager lockManager,
        ILogger<FilesystemMultipartUploadMetadataStorage> logger)
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _metadataMode = settings.MetadataMode;
        _metadataDirectory = settings.MetadataDirectory;
        _inlineMetadataDirectoryName = settings.InlineMetadataDirectoryName;
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

        await _lockManager.WriteFileAsync(uploadMetadataPath, json, cancellationToken);

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

    public Task<bool> DeleteUploadMetadataAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
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

    public async Task<bool> UploadExistsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var upload = await GetUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);
        return upload != null;
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
}