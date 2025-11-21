using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

public class FilesystemMultipartUploadDataStorage : IMultipartUploadDataStorage
{
    private readonly string _dataDirectory;
    private readonly string? _metadataDirectory;
    private readonly MetadataStorageMode _metadataMode;
    private readonly string _inlineMetadataDirectoryName;
    private readonly NetworkFileSystemHelper _networkHelper;
    private readonly ILogger<FilesystemMultipartUploadDataStorage> _logger;

    public FilesystemMultipartUploadDataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        NetworkFileSystemHelper networkHelper,
        ILogger<FilesystemMultipartUploadDataStorage> logger)
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _metadataMode = settings.MetadataMode;
        _metadataDirectory = settings.MetadataDirectory;
        _inlineMetadataDirectoryName = settings.InlineMetadataDirectoryName;
        _networkHelper = networkHelper;
        _logger = logger;

        _networkHelper.EnsureDirectoryExists(_dataDirectory);

        if (_metadataMode == MetadataStorageMode.SeparateDirectory)
        {
            if (string.IsNullOrWhiteSpace(_metadataDirectory))
            {
                throw new InvalidOperationException("MetadataDirectory is required when using SeparateDirectory metadata mode");
            }
            _networkHelper.EnsureDirectoryExists(_metadataDirectory);
        }
    }

    public async Task<StorageResult<UploadPart>> StorePartDataAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, ChecksumRequest? checksumRequest, CancellationToken cancellationToken = default)
    {
        var partPath = GetPartPath(uploadId, partNumber);
        var partDir = Path.GetDirectoryName(partPath)!;
        await _networkHelper.EnsureDirectoryExistsAsync(partDir, $"StorePartData-{uploadId}/{partNumber}");

        long bytesWritten = 0;

        // Initialize checksum calculator if needed
        StreamingChecksumCalculator? checksumCalculator = null;
        if (checksumRequest != null)
        {
            checksumCalculator = new StreamingChecksumCalculator(checksumRequest.Algorithm, checksumRequest.ProvidedChecksums);
        }

        try
        {
            // Write the data to file, ensuring proper disposal before computing ETag
            {
                await using var fileStream = new FileStream(partPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

                // Use helper to write and calculate checksums in one pass
                bytesWritten = await ChecksumStreamHelper.WriteDataWithChecksumsAsync(dataReader, fileStream, checksumCalculator, cancellationToken);

                await fileStream.FlushAsync(cancellationToken);
            } // FileStream is fully disposed here

            // Validate and finalize checksums if requested
            if (checksumCalculator?.HasChecksums == true)
            {
                var result = checksumCalculator.Finish();

                if (!result.IsValid)
                {
                    // Checksum validation failed - clean up and return null
                    if (File.Exists(partPath))
                    {
                        File.Delete(partPath);
                    }
                    return StorageResult<UploadPart>.Error("InvalidChecksum", result.ErrorMessage ?? "Checksum validation failed");
                }

                // Populate checksum fields
                var part = new UploadPart
                {
                    PartNumber = partNumber,
                    ETag = await ETagHelper.ComputeETagFromFileAsync(partPath),
                    Size = bytesWritten,
                    LastModified = DateTime.UtcNow
                };

                if (result.CalculatedChecksums.TryGetValue("CRC32", out var crc32))
                    part.ChecksumCRC32 = crc32;
                if (result.CalculatedChecksums.TryGetValue("CRC32C", out var crc32c))
                    part.ChecksumCRC32C = crc32c;
                if (result.CalculatedChecksums.TryGetValue("CRC64NVME", out var crc64))
                    part.ChecksumCRC64NVME = crc64;
                if (result.CalculatedChecksums.TryGetValue("SHA1", out var sha1))
                    part.ChecksumSHA1 = sha1;
                if (result.CalculatedChecksums.TryGetValue("SHA256", out var sha256))
                    part.ChecksumSHA256 = sha256;

                return StorageResult<UploadPart>.Success(part);
            }
            else
            {
                // No checksums requested
                var etag = await ETagHelper.ComputeETagFromFileAsync(partPath);

                var part = new UploadPart
                {
                    PartNumber = partNumber,
                    ETag = etag,
                    Size = bytesWritten,
                    LastModified = DateTime.UtcNow
                };

                return StorageResult<UploadPart>.Success(part);
            }
        }
        finally
        {
            checksumCalculator?.Dispose();
        }
    }

    public async Task<StorageResult<UploadPart>> StorePartDataAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkedDataParser chunkedDataParser, IChunkSignatureValidator chunkValidator, ChecksumRequest? checksumRequest, CancellationToken cancellationToken = default)
    {
        var partPath = GetPartPath(uploadId, partNumber);
        var partDir = Path.GetDirectoryName(partPath)!;
        await _networkHelper.EnsureDirectoryExistsAsync(partDir, $"StorePartDataChunked-{uploadId}/{partNumber}");

        // Create a temporary file for validated data
        var tempPath = Path.Combine(partDir, $".lamina-tmp-{Guid.NewGuid():N}");

        // Initialize checksum calculator if needed
        StreamingChecksumCalculator? checksumCalculator = null;
        if (checksumRequest != null)
        {
            checksumCalculator = new StreamingChecksumCalculator(checksumRequest.Algorithm, checksumRequest.ProvidedChecksums);
        }

        try
        {
            ChunkedDataResult parseResult;

            // Write validated chunks directly to temp file
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);
                parseResult = await chunkedDataParser.ParseChunkedDataToStreamAsync(
                    dataReader, 
                    fileStream, 
                    chunkValidator,
                    checksumCalculator?.HasChecksums == true ? data => checksumCalculator.Append(data) : null,
                    cancellationToken);
                await fileStream.FlushAsync(cancellationToken);
            } // FileStream is fully disposed here

            // Check if validation succeeded
            if (!parseResult.Success)
            {
                // Invalid chunk signature - clean up temp file and return null
                _logger.LogWarning("Chunk signature validation failed for part {PartNumber} of upload {UploadId}: {Error}", partNumber, uploadId, parseResult.ErrorMessage);

                if (File.Exists(tempPath))
                {
                    try
                    {
                        File.Delete(tempPath);
                    }
                    catch (Exception cleanupEx)
                    {
                        _logger.LogWarning(cleanupEx, "Failed to clean up temporary file: {TempPath}", tempPath);
                    }
                }

                return StorageResult<UploadPart>.Error("SignatureDoesNotMatch", "Chunk signature validation failed");
            }

            // Finalize and validate checksums if they were calculated
            if (checksumCalculator?.HasChecksums == true)
            {
                var result = checksumCalculator.Finish();

                if (!result.IsValid)
                {
                    // Checksum validation failed - clean up temp file and return null
                    _logger.LogWarning("Checksum validation failed for part {PartNumber} of upload {UploadId}: {Error}", partNumber, uploadId, result.ErrorMessage);

                    if (File.Exists(tempPath))
                    {
                        try
                        {
                            File.Delete(tempPath);
                        }
                        catch (Exception cleanupEx)
                        {
                            _logger.LogWarning(cleanupEx, "Failed to clean up temporary file: {TempPath}", tempPath);
                        }
                    }

                    return StorageResult<UploadPart>.Error("InvalidChecksum", result.ErrorMessage ?? "Checksum validation failed");
                }

                // Compute ETag from the temp file
                var etag = await ETagHelper.ComputeETagFromFileAsync(tempPath);

                // Atomically move temp file to final location
                File.Move(tempPath, partPath, overwrite: true);

                var part = new UploadPart
                {
                    PartNumber = partNumber,
                    ETag = etag,
                    Size = parseResult.TotalBytesWritten,
                    LastModified = DateTime.UtcNow
                };

                // Populate checksum fields
                if (result.CalculatedChecksums.TryGetValue("CRC32", out var crc32))
                    part.ChecksumCRC32 = crc32;
                if (result.CalculatedChecksums.TryGetValue("CRC32C", out var crc32c))
                    part.ChecksumCRC32C = crc32c;
                if (result.CalculatedChecksums.TryGetValue("CRC64NVME", out var crc64))
                    part.ChecksumCRC64NVME = crc64;
                if (result.CalculatedChecksums.TryGetValue("SHA1", out var sha1))
                    part.ChecksumSHA1 = sha1;
                if (result.CalculatedChecksums.TryGetValue("SHA256", out var sha256))
                    part.ChecksumSHA256 = sha256;

                return StorageResult<UploadPart>.Success(part);
            }
            else
            {
                // No checksums requested
                var etag = await ETagHelper.ComputeETagFromFileAsync(tempPath);

                // Atomically move temp file to final location
                File.Move(tempPath, partPath, overwrite: true);

                var part = new UploadPart
                {
                    PartNumber = partNumber,
                    ETag = etag,
                    Size = parseResult.TotalBytesWritten,
                    LastModified = DateTime.UtcNow
                };

                return StorageResult<UploadPart>.Success(part);
            }
        }
        catch
        {
            // Clean up temp file on any other error
            if (File.Exists(tempPath))
            {
                try
                {
                    File.Delete(tempPath);
                }
                catch (Exception cleanupEx)
                {
                    _logger.LogWarning(cleanupEx, "Failed to clean up temporary file: {TempPath}", tempPath);
                }
            }

            throw;
        }
        finally
        {
            checksumCalculator?.Dispose();
        }
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

    private string GetUploadDirectory(string uploadId)
    {
        if (_metadataMode == MetadataStorageMode.SeparateDirectory)
        {
            return Path.Combine(_metadataDirectory!, "_multipart_uploads", uploadId);
        }
        else
        {
            return Path.Combine(_dataDirectory, _inlineMetadataDirectoryName, "_multipart_uploads", uploadId);
        }
    }

}