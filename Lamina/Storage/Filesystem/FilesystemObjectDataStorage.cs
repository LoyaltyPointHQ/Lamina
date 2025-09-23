using System.IO.Pipelines;
using Lamina.Helpers;
using Lamina.Services;
using Lamina.Storage.Abstract;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

public class FilesystemObjectDataStorage : IObjectDataStorage
{
    private readonly string _dataDirectory;
    private readonly MetadataStorageMode _metadataMode;
    private readonly string _inlineMetadataDirectoryName;
    private readonly NetworkFileSystemHelper _networkHelper;
    private readonly ILogger<FilesystemObjectDataStorage> _logger;

    public FilesystemObjectDataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        NetworkFileSystemHelper networkHelper,
        ILogger<FilesystemObjectDataStorage> logger)
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _metadataMode = settings.MetadataMode;
        _inlineMetadataDirectoryName = settings.InlineMetadataDirectoryName;
        _networkHelper = networkHelper;
        _logger = logger;

        Directory.CreateDirectory(_dataDirectory);
    }

    public async Task<(long size, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        // Validate that the key doesn't contain metadata directory patterns
        if (FilesystemStorageHelper.IsMetadataPath(key, _metadataMode, _inlineMetadataDirectoryName))
        {
            throw new InvalidOperationException($"Cannot store data with key containing metadata directory '{_inlineMetadataDirectoryName}'");
        }

        var dataPath = GetDataPath(bucketName, key);
        var dataDir = Path.GetDirectoryName(dataPath)!;
        Directory.CreateDirectory(dataDir);

        // Create a temporary file in the same directory to ensure atomic move
        var tempPath = Path.Combine(dataDir, $".tmp_{Guid.NewGuid():N}");
        long bytesWritten = 0;

        try
        {
            // Write the data to temp file, ensuring proper disposal before computing ETag
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

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
            } // FileStream is fully disposed here

            // Now compute ETag from the temp file on disk with a new file handle
            var etag = await ETagHelper.ComputeETagFromFileAsync(tempPath);

            // Atomically move the temp file to the final location
            await _networkHelper.AtomicMoveAsync(tempPath, dataPath, overwrite: true);

            return (bytesWritten, etag);
        }
        catch
        {
            // Clean up temp file if something went wrong
            try
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to clean up temporary file: {TempPath}", tempPath);
            }
            throw;
        }
    }

    public async Task<(long size, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, CancellationToken cancellationToken = default)
    {
        // If no chunk validator is provided, use the standard method
        if (chunkValidator == null)
        {
            return await StoreDataAsync(bucketName, key, dataReader, cancellationToken);
        }

        // Validate that the key doesn't contain metadata directory patterns
        if (FilesystemStorageHelper.IsMetadataPath(key, _metadataMode, _inlineMetadataDirectoryName))
        {
            throw new InvalidOperationException($"Cannot store data with key containing metadata directory '{_inlineMetadataDirectoryName}'");
        }

        var dataPath = GetDataPath(bucketName, key);
        var dataDir = Path.GetDirectoryName(dataPath)!;
        Directory.CreateDirectory(dataDir);

        // Create a temporary file in the same directory to ensure atomic move
        var tempPath = Path.Combine(dataDir, $".tmp_{Guid.NewGuid():N}");
        long bytesWritten = 0;

        try
        {
            // Write the decoded data to temp file
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

                // Parse AWS chunked encoding and write decoded data directly to file with signature validation
                try
                {
                    bytesWritten = await AwsChunkedEncodingHelper.ParseChunkedDataToStreamAsync(dataReader, fileStream, chunkValidator, _logger, cancellationToken);
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("Invalid") && ex.Message.Contains("signature"))
                {
                    // Invalid chunk signature - clean up and return failure
                    await fileStream.FlushAsync(cancellationToken);
                    fileStream.Close();
                    File.Delete(tempPath);
                    return (0, string.Empty);
                }

                await fileStream.FlushAsync(cancellationToken);
            } // FileStream is fully disposed here

            // Now compute ETag from the temp file on disk with a new file handle
            var etag = await ETagHelper.ComputeETagFromFileAsync(tempPath);

            // Atomically move the temp file to the final location
            await _networkHelper.AtomicMoveAsync(tempPath, dataPath, overwrite: true);

            return (bytesWritten, etag);
        }
        catch
        {
            // Clean up temp file if something went wrong
            try
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to clean up temporary file: {TempPath}", tempPath);
            }
            throw;
        }
    }

    public async Task<(long size, string etag)> StoreMultipartDataAsync(string bucketName, string key, IEnumerable<PipeReader> partReaders, CancellationToken cancellationToken = default)
    {
        // Validate that the key doesn't contain metadata directory patterns
        if (FilesystemStorageHelper.IsMetadataPath(key, _metadataMode, _inlineMetadataDirectoryName))
        {
            throw new InvalidOperationException($"Cannot store data with key containing metadata directory '{_inlineMetadataDirectoryName}'");
        }

        var dataPath = GetDataPath(bucketName, key);
        var dataDir = Path.GetDirectoryName(dataPath)!;
        Directory.CreateDirectory(dataDir);

        // Create a temporary file in the same directory to ensure atomic move
        var tempPath = Path.Combine(dataDir, $".tmp_{Guid.NewGuid():N}");
        long totalBytesWritten = 0;

        try
        {
            // Write all parts to the temp file, ensuring proper disposal before computing ETag
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

                foreach (var reader in partReaders)
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var result = await reader.ReadAsync(cancellationToken);
                        var buffer = result.Buffer;

                        if (buffer.IsEmpty && result.IsCompleted)
                        {
                            break;
                        }

                        foreach (var segment in buffer)
                        {
                            await fileStream.WriteAsync(segment, cancellationToken);
                            totalBytesWritten += segment.Length;
                        }

                        reader.AdvanceTo(buffer.End);

                        if (result.IsCompleted)
                        {
                            break;
                        }
                    }
                    await reader.CompleteAsync();
                }

                await fileStream.FlushAsync(cancellationToken);
            } // FileStream is fully disposed here

            // Now compute ETag from the completed temp file with a new file handle
            var etag = await ETagHelper.ComputeETagFromFileAsync(tempPath);

            // Atomically move the temp file to the final location
            await _networkHelper.AtomicMoveAsync(tempPath, dataPath, overwrite: true);

            return (totalBytesWritten, etag);
        }
        catch
        {
            // Clean up temp file if something went wrong
            try
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to clean up temporary file: {TempPath}", tempPath);
            }
            throw;
        }
    }

    public async Task<bool> WriteDataToPipeAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default)
    {
        // Validate that the key doesn't contain metadata directory patterns
        if (FilesystemStorageHelper.IsMetadataPath(key, _metadataMode, _inlineMetadataDirectoryName))
        {
            return false;  // Return false to indicate object not found
        }

        var dataPath = GetDataPath(bucketName, key);

        if (!File.Exists(dataPath))
        {
            return false;
        }

        await using var fileStream = File.OpenRead(dataPath);
        const int bufferSize = 4096;
        var buffer = new byte[bufferSize];
        int bytesRead;

        while ((bytesRead = await fileStream.ReadAsync(buffer, cancellationToken)) > 0)
        {
            var memory = writer.GetMemory(bytesRead);
            buffer.AsMemory(0, bytesRead).CopyTo(memory);
            writer.Advance(bytesRead);
            await writer.FlushAsync(cancellationToken);
        }

        await writer.CompleteAsync();
        return true;
    }

    public async Task<bool> DeleteDataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        // Validate that the key doesn't contain metadata directory patterns
        if (FilesystemStorageHelper.IsMetadataPath(key, _metadataMode, _inlineMetadataDirectoryName))
        {
            return false;  // Cannot delete metadata paths
        }

        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return false;
        }

        await _networkHelper.ExecuteWithRetryAsync(() =>
        {
            File.Delete(dataPath);
            return Task.FromResult(true);
        }, "DeleteFile");

        // Clean up empty directories, but preserve the bucket directory
        try
        {
            var bucketDirectory = Path.Combine(_dataDirectory, bucketName);
            var directory = Path.GetDirectoryName(dataPath);

            if (!string.IsNullOrEmpty(directory) &&
                directory.StartsWith(_dataDirectory) &&
                directory != _dataDirectory &&
                directory != bucketDirectory)
            {
                // Use helper for directory cleanup with network filesystem support
                await _networkHelper.DeleteDirectoryIfEmptyAsync(directory, bucketDirectory);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to clean up empty directories for path: {DataPath}", dataPath);
        }

        return true;
    }

    public Task<bool> DataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        // Validate that the key doesn't contain metadata directory patterns
        if (FilesystemStorageHelper.IsMetadataPath(key, _metadataMode, _inlineMetadataDirectoryName))
        {
            return Task.FromResult(false);
        }

        var dataPath = GetDataPath(bucketName, key);
        return Task.FromResult(File.Exists(dataPath));
    }

    public Task<(long size, DateTime lastModified)?> GetDataInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        // Validate that the key doesn't contain metadata directory patterns
        if (FilesystemStorageHelper.IsMetadataPath(key, _metadataMode, _inlineMetadataDirectoryName))
        {
            return Task.FromResult<(long size, DateTime lastModified)?>(null);
        }

        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return Task.FromResult<(long size, DateTime lastModified)?>(null);
        }

        var fileInfo = new FileInfo(dataPath);
        return Task.FromResult<(long size, DateTime lastModified)?>((fileInfo.Length, fileInfo.LastWriteTimeUtc));
    }

    public Task<IEnumerable<string>> ListDataKeysAsync(string bucketName, string? prefix = null, CancellationToken cancellationToken = default)
    {
        var bucketPath = Path.Combine(_dataDirectory, bucketName);
        if (!Directory.Exists(bucketPath))
        {
            return Task.FromResult(Enumerable.Empty<string>());
        }

        var keys = new List<string>();
        var searchPath = string.IsNullOrEmpty(prefix) ? bucketPath : Path.Combine(bucketPath, prefix);

        if (Directory.Exists(searchPath))
        {
            EnumerateDataFiles(searchPath, bucketPath, keys);
        }
        else if (!string.IsNullOrEmpty(prefix))
        {
            // If prefix doesn't match a directory, search parent directory
            var parentDir = Path.GetDirectoryName(searchPath);
            if (Directory.Exists(parentDir))
            {
                EnumerateDataFiles(parentDir, bucketPath, keys, prefix);
            }
        }

        return Task.FromResult<IEnumerable<string>>(keys);
    }

    private void EnumerateDataFiles(string directory, string bucketPath, List<string> keys, string? prefixFilter = null)
    {
        foreach (var file in Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories))
        {
            var relativePath = Path.GetRelativePath(bucketPath, file).Replace(Path.DirectorySeparatorChar, '/');

            // Skip if it's a metadata path
            if (FilesystemStorageHelper.IsMetadataPath(relativePath, _metadataMode, _inlineMetadataDirectoryName))
            {
                continue;
            }

            // Apply prefix filter if provided
            if (!string.IsNullOrEmpty(prefixFilter) && !relativePath.StartsWith(prefixFilter))
            {
                continue;
            }

            keys.Add(relativePath);
        }
    }

    public async Task<string?> ComputeETagAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        // Validate that the key doesn't contain metadata directory patterns
        if (FilesystemStorageHelper.IsMetadataPath(key, _metadataMode, _inlineMetadataDirectoryName))
        {
            return null;
        }

        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return null;
        }

        // Use ETagHelper which efficiently computes hash without loading entire file into memory
        return await ETagHelper.ComputeETagFromFileAsync(dataPath);
    }

    private string GetDataPath(string bucketName, string key)
    {
        return Path.Combine(_dataDirectory, bucketName, key);
    }

}