using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

public class FilesystemMultipartUploadDataStorage : IMultipartUploadDataStorage, IFileBackedMultipartPartSource
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

    public async Task<StagedDataWrite> BeginPartWriteAsync(string bucketName, string key, string uploadId, int partNumber, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var directory = GetUploadDirectory(uploadId);
        await _networkHelper.EnsureDirectoryExistsAsync(directory, $"StorePart-{uploadId}/{partNumber}");
        var path = Path.Combine(directory, $".lamina-tmp-{Guid.NewGuid():N}");
        var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, true);
        void Cleanup() => File.Delete(path);
        return new StagedDataWrite(stream, size =>
        {
            var prepared = new PreparedData { BucketName = bucketName, Key = key, Size = size, Tag = path };
            prepared.SetDisposeAction(Cleanup);
            return prepared;
        }, Cleanup);
    }

    public async Task CommitPreparedPartAsync(string bucketName, string key, string uploadId, int partNumber, PreparedData preparedData, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await _networkHelper.AtomicMoveAsync(preparedData.Tag!, GetPartPath(uploadId, partNumber), overwrite: true);
    }

    public Task AbortPreparedPartAsync(PreparedData preparedData, CancellationToken cancellationToken = default)
    {
        preparedData.Dispose();
        return Task.CompletedTask;
    }

    public async Task<IEnumerable<PipeReader>> GetPartReadersAsync(string bucketName, string key, string uploadId, List<CompletedPart> parts, CancellationToken cancellationToken = default)
    {
        var readers = new List<PipeReader>();
        try
        {
            foreach (var part in parts.OrderBy(p => p.PartNumber))
            {
                cancellationToken.ThrowIfCancellationRequested();
                var stream = new FileStream(GetPartPath(uploadId, part.PartNumber), FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, 81920, true);
                readers.Add(PipeReader.Create(stream));
            }
            return readers;
        }
        catch (Exception error)
        {
            foreach (var reader in readers) await reader.CompleteAsync();
            if (error is FileNotFoundException or DirectoryNotFoundException) return Array.Empty<PipeReader>();
            throw;
        }
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

    public bool TryGetPartFilePaths(string bucketName, string key, string uploadId, IReadOnlyList<CompletedPart> parts, out IReadOnlyList<string> paths)
    {
        var ordered = parts.OrderBy(p => p.PartNumber).ToList();
        var result = new List<string>(ordered.Count);
        foreach (var part in ordered)
        {
            var partPath = GetPartPath(uploadId, part.PartNumber);
            if (!File.Exists(partPath))
            {
                _logger.LogWarning("File-backed fast path cannot start: part {PartNumber} missing for upload {UploadId}", part.PartNumber, uploadId);
                paths = Array.Empty<string>();
                return false;
            }
            result.Add(partPath);
        }
        paths = result;
        return true;
    }

    public Task<bool> HasAnyPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadDir = _metadataMode == MetadataStorageMode.SeparateDirectory
            ? Path.Combine(_metadataDirectory!, "_multipart_uploads", uploadId)
            : Path.Combine(_dataDirectory, _inlineMetadataDirectoryName, "_multipart_uploads", uploadId);

        if (!Directory.Exists(uploadDir))
        {
            return Task.FromResult(false);
        }

        var exists = Directory.EnumerateFiles(uploadDir, "part_*")
            .Any(f => !f.EndsWith(".metadata.json"));
        return Task.FromResult(exists);
    }

    public Task<List<UploadPart>> GetStoredPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadDir = _metadataMode == MetadataStorageMode.SeparateDirectory
            ? Path.Combine(_metadataDirectory!, "_multipart_uploads", uploadId)
            : Path.Combine(_dataDirectory, _inlineMetadataDirectoryName, "_multipart_uploads", uploadId);
        var parts = new List<UploadPart>();

        if (!Directory.Exists(uploadDir))
        {
            return Task.FromResult(parts);
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

                    var part = new UploadPart
                    {
                        PartNumber = partNumber,
                        ETag = string.Empty,
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

        return Task.FromResult(parts.OrderBy(p => p.PartNumber).ToList());
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