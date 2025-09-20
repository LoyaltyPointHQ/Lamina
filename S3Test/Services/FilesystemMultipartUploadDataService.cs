using System.IO.Pipelines;
using System.Security.Cryptography;
using S3Test.Models;

namespace S3Test.Services;

public class FilesystemMultipartUploadDataService : IMultipartUploadDataService
{
    private readonly string _metadataDirectory;
    private readonly ILogger<FilesystemMultipartUploadDataService> _logger;

    public FilesystemMultipartUploadDataService(
        IConfiguration configuration,
        IFileSystemLockManager lockManager,
        ILogger<FilesystemMultipartUploadDataService> logger)
    {
        _metadataDirectory = configuration["FilesystemStorage:MetadataDirectory"] ?? "/var/s3test/metadata";
        _logger = logger;

        Directory.CreateDirectory(_metadataDirectory);
    }

    public async Task<UploadPart> StorePartDataAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        var partPath = GetPartPath(uploadId, partNumber);
        var partDir = Path.GetDirectoryName(partPath)!;
        Directory.CreateDirectory(partDir);

        using var memoryStream = new MemoryStream();
        while (!cancellationToken.IsCancellationRequested)
        {
            var result = await dataReader.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            if (buffer.IsEmpty && result.IsCompleted)
            {
                break;
            }

            foreach (var segment in buffer)
            {
                await memoryStream.WriteAsync(segment, cancellationToken);
            }

            dataReader.AdvanceTo(buffer.End);

            if (result.IsCompleted)
            {
                break;
            }
        }

        await dataReader.CompleteAsync();

        var data = memoryStream.ToArray();
        var etag = ComputeETag(data);
        
        await File.WriteAllBytesAsync(partPath, data, cancellationToken);

        var part = new UploadPart
        {
            PartNumber = partNumber,
            ETag = etag,
            Size = data.Length,
            LastModified = DateTime.UtcNow
        };

        return part;
    }

    public async Task<byte[]?> GetPartDataAsync(string bucketName, string key, string uploadId, int partNumber, CancellationToken cancellationToken = default)
    {
        var partPath = GetPartPath(uploadId, partNumber);

        if (!File.Exists(partPath))
        {
            return null;
        }

        return await File.ReadAllBytesAsync(partPath, cancellationToken);
    }

    public async Task<byte[]?> AssemblePartsAsync(string bucketName, string key, string uploadId, List<CompletedPart> parts, CancellationToken cancellationToken = default)
    {
        var orderedParts = parts.OrderBy(p => p.PartNumber).ToList();
        var combinedData = new List<byte>();

        foreach (var part in orderedParts)
        {
            var partData = await GetPartDataAsync(bucketName, key, uploadId, part.PartNumber, cancellationToken);
            if (partData == null)
            {
                return null;
            }

            // Verify ETag
            var actualETag = ComputeETag(partData);
            var expectedETag = part.ETag.Trim('"');
            if (actualETag != expectedETag)
            {
                _logger.LogWarning("Part ETag mismatch for upload {UploadId}, part {PartNumber}", uploadId, part.PartNumber);
                return null;
            }

            combinedData.AddRange(partData);
        }

        return combinedData.ToArray();
    }

    public Task<bool> DeletePartDataAsync(string bucketName, string key, string uploadId, int partNumber, CancellationToken cancellationToken = default)
    {
        var partPath = GetPartPath(uploadId, partNumber);
        if (!File.Exists(partPath)) 
            return Task.FromResult(false);
        File.Delete(partPath);
        return Task.FromResult(true);
    }

    public Task<bool> DeleteAllPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadDir = Path.Combine(_metadataDirectory, "_multipart_uploads", uploadId);

        if (!Directory.Exists(uploadDir))
        {
            return Task.FromResult(true);
        }

        try
        {
            Directory.Delete(uploadDir, recursive: true);
            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete upload directory: {UploadDir}", uploadDir);
            return Task.FromResult(false);
        }
    }

    public async Task<List<UploadPart>> GetStoredPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadDir = Path.Combine(_metadataDirectory, "_multipart_uploads", uploadId);
        var parts = new List<UploadPart>();

        if (!Directory.Exists(uploadDir))
        {
            return parts;
        }

        // Get all part data files (without .metadata.json extension)
        var partFiles = Directory.GetFiles(uploadDir, "part_*")
            .Where(f => !f.EndsWith(".metadata.json"))
            .ToArray();

        foreach (var partFile in partFiles)
        {
            try
            {
                // Extract part number from filename
                var fileName = Path.GetFileName(partFile);
                if (fileName.StartsWith("part_") && int.TryParse(fileName.Substring(5), out int partNumber))
                {
                    var fileInfo = new FileInfo(partFile);
                    var data = await File.ReadAllBytesAsync(partFile, cancellationToken);
                    var etag = ComputeETag(data);

                    var part = new UploadPart
                    {
                        PartNumber = partNumber,
                        ETag = etag,
                        Size = fileInfo.Length,
                        LastModified = fileInfo.LastWriteTimeUtc
                    };
                    parts.Add(part);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to read part data: {PartFile}", partFile);
            }
        }

        return parts.OrderBy(p => p.PartNumber).ToList();
    }

    private string GetPartPath(string uploadId, int partNumber)
    {
        return Path.Combine(_metadataDirectory, "_multipart_uploads", uploadId, $"part_{partNumber}");
    }


    private static string ComputeETag(byte[] data)
    {
        using var md5 = MD5.Create();
        var hash = md5.ComputeHash(data);
        return Convert.ToHexString(hash).ToLower();
    }
}