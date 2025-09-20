using System.IO.Pipelines;
using S3Test.Helpers;
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

        // Stream directly to file without buffering in memory
        await using var fileStream = new FileStream(partPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);
        long bytesWritten = 0;

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
                await fileStream.WriteAsync(segment, cancellationToken);
                bytesWritten += segment.Length;
            }

            dataReader.AdvanceTo(buffer.End);

            if (result.IsCompleted)
            {
                break;
            }
        }

        await dataReader.CompleteAsync();
        await fileStream.FlushAsync(cancellationToken);

        // Compute ETag from the file on disk
        var etag = await ETagHelper.ComputeETagFromFileAsync(partPath);

        var part = new UploadPart
        {
            PartNumber = partNumber,
            ETag = etag,
            Size = bytesWritten,
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

        // First, verify all parts exist and have correct ETags
        long totalSize = 0;
        foreach (var part in orderedParts)
        {
            var partPath = GetPartPath(uploadId, part.PartNumber);
            if (!File.Exists(partPath))
            {
                _logger.LogWarning("Part file not found for upload {UploadId}, part {PartNumber}", uploadId, part.PartNumber);
                return null;
            }

            // Verify ETag from the file on disk
            var actualETag = await ETagHelper.ComputeETagFromFileAsync(partPath);
            var expectedETag = part.ETag.Trim('"');
            if (actualETag != expectedETag)
            {
                _logger.LogWarning("Part ETag mismatch for upload {UploadId}, part {PartNumber}", uploadId, part.PartNumber);
                return null;
            }

            var fileInfo = new FileInfo(partPath);
            totalSize += fileInfo.Length;
        }

        // Now stream all parts together into a combined array
        var combinedData = new byte[totalSize];
        var offset = 0;

        foreach (var part in orderedParts)
        {
            var partPath = GetPartPath(uploadId, part.PartNumber);
            await using var partStream = File.OpenRead(partPath);
            var bytesRead = await partStream.ReadAsync(combinedData.AsMemory(offset, (int)partStream.Length), cancellationToken);
            offset += bytesRead;
        }

        return combinedData;
    }

    public async Task<IEnumerable<PipeReader>> GetPartReadersAsync(string bucketName, string key, string uploadId, List<CompletedPart> parts, CancellationToken cancellationToken = default)
    {
        var orderedParts = parts.OrderBy(p => p.PartNumber).ToList();
        var readers = new List<PipeReader>();

        foreach (var part in orderedParts)
        {
            var partPath = GetPartPath(uploadId, part.PartNumber);
            if (!File.Exists(partPath))
            {
                // Clean up any readers we've already created
                foreach (var reader in readers)
                {
                    await reader.CompleteAsync();
                }
                _logger.LogWarning("Part file not found for upload {UploadId}, part {PartNumber}", uploadId, part.PartNumber);
                return Enumerable.Empty<PipeReader>();
            }

            // Verify ETag
            var actualETag = await ETagHelper.ComputeETagFromFileAsync(partPath);
            var expectedETag = part.ETag.Trim('"');
            if (actualETag != expectedETag)
            {
                // Clean up any readers we've already created
                foreach (var reader in readers)
                {
                    await reader.CompleteAsync();
                }
                _logger.LogWarning("Part ETag mismatch for upload {UploadId}, part {PartNumber}", uploadId, part.PartNumber);
                return Enumerable.Empty<PipeReader>();
            }

            // Create a pipe for this part
            var pipe = new Pipe();
            var writer = pipe.Writer;

            // Start streaming the part file to the pipe in the background
            _ = Task.Run(async () =>
            {
                try
                {
                    await using var fileStream = File.OpenRead(partPath);
                    const int bufferSize = 4096;

                    int bytesRead;
                    while ((bytesRead = await fileStream.ReadAsync(writer.GetMemory(bufferSize), cancellationToken)) > 0)
                    {
                        writer.Advance(bytesRead);
                        await writer.FlushAsync(cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    await writer.CompleteAsync(ex);
                    return;
                }

                await writer.CompleteAsync();
            }, cancellationToken);

            readers.Add(pipe.Reader);
        }

        return readers;
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
                    // Compute ETag directly from file without loading into memory
                    var etag = await ETagHelper.ComputeETagFromFileAsync(partFile);

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
                throw;
            }
        }

        return parts.OrderBy(p => p.PartNumber).ToList();
    }

    private string GetPartPath(string uploadId, int partNumber)
    {
        return Path.Combine(_metadataDirectory, "_multipart_uploads", uploadId, $"part_{partNumber}");
    }
}