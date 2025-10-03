using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.Filesystem.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

public class FilesystemMultipartUploadDataStorage : IMultipartUploadDataStorage
{
    private readonly string _dataDirectory;
    private readonly string? _metadataDirectory;
    private readonly MetadataStorageMode _metadataMode;
    private readonly string _inlineMetadataDirectoryName;
    private readonly ILogger<FilesystemMultipartUploadDataStorage> _logger;

    public FilesystemMultipartUploadDataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        ILogger<FilesystemMultipartUploadDataStorage> logger)
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _metadataMode = settings.MetadataMode;
        _metadataDirectory = settings.MetadataDirectory;
        _inlineMetadataDirectoryName = settings.InlineMetadataDirectoryName;
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

    public async Task<UploadPart> StorePartDataAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        var partPath = GetPartPath(uploadId, partNumber);
        var partDir = Path.GetDirectoryName(partPath)!;
        Directory.CreateDirectory(partDir);

        long bytesWritten = 0;

        // Write the data to file, ensuring proper disposal before computing ETag
        {
            await using var fileStream = new FileStream(partPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

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

            await fileStream.FlushAsync(cancellationToken);
        } // FileStream is fully disposed here

        // Now compute ETag from the file on disk with a new file handle
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
                    // Use FileShare.Read to allow concurrent reads for ETag verification
                    await using var fileStream = new FileStream(partPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: true);
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

    public Task<bool> DeleteAllPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadDir = _metadataMode == MetadataStorageMode.SeparateDirectory
            ? Path.Combine(_metadataDirectory!, "_multipart_uploads", uploadId)
            : Path.Combine(_dataDirectory, _inlineMetadataDirectoryName, "_multipart_uploads", uploadId);

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
        var uploadDir = _metadataMode == MetadataStorageMode.SeparateDirectory
            ? Path.Combine(_metadataDirectory!, "_multipart_uploads", uploadId)
            : Path.Combine(_dataDirectory, _inlineMetadataDirectoryName, "_multipart_uploads", uploadId);
        var parts = new List<UploadPart>();

        if (!Directory.Exists(uploadDir))
        {
            return parts;
        }

        // Get all part data files (without .metadata.json extension)
        var partFiles = Directory.EnumerateFiles(uploadDir, "part_*")
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
        if (_metadataMode == MetadataStorageMode.SeparateDirectory)
        {
            return Path.Combine(_metadataDirectory!, "_multipart_uploads", uploadId, $"part_{partNumber}");
        }
        else
        {
            return Path.Combine(_dataDirectory, _inlineMetadataDirectoryName, "_multipart_uploads", uploadId, $"part_{partNumber}");
        }
    }
}