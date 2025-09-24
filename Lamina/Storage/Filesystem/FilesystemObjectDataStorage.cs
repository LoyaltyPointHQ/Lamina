using System.IO.Pipelines;
using Lamina.Helpers;
using Lamina.Streaming.Chunked;
using Lamina.Streaming.Validation;
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
    private readonly string _tempFilePrefix;
    private readonly NetworkFileSystemHelper _networkHelper;
    private readonly ILogger<FilesystemObjectDataStorage> _logger;
    private readonly IChunkedDataParser _chunkedDataParser;

    public FilesystemObjectDataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        NetworkFileSystemHelper networkHelper,
        ILogger<FilesystemObjectDataStorage> logger,
        IChunkedDataParser chunkedDataParser)
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _metadataMode = settings.MetadataMode;
        _inlineMetadataDirectoryName = settings.InlineMetadataDirectoryName;
        _tempFilePrefix = settings.TempFilePrefix;
        _networkHelper = networkHelper;
        _logger = logger;
        _chunkedDataParser = chunkedDataParser;

        Directory.CreateDirectory(_dataDirectory);
    }

    public async Task<(long size, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        // Validate that the key doesn't conflict with temporary files or metadata directories
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            throw new InvalidOperationException($"Cannot store data with key '{key}' as it conflicts with temporary file pattern '{_tempFilePrefix}' or metadata directory '{_inlineMetadataDirectoryName}'");
        }

        var dataPath = GetDataPath(bucketName, key);
        var dataDir = Path.GetDirectoryName(dataPath)!;
        Directory.CreateDirectory(dataDir);

        // Create a temporary file in the same directory to ensure atomic move
        var tempPath = Path.Combine(dataDir, $"{_tempFilePrefix}{Guid.NewGuid():N}");
        long bytesWritten = 0;

        try
        {
            // Write the data to temp file, ensuring proper disposal before computing ETag
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);
                bytesWritten = await PipeReaderHelper.CopyToAsync(dataReader, fileStream, false, cancellationToken);
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

        // Validate that the key doesn't conflict with temporary files or metadata directories
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            throw new InvalidOperationException($"Cannot store data with key '{key}' as it conflicts with temporary file pattern '{_tempFilePrefix}' or metadata directory '{_inlineMetadataDirectoryName}'");
        }

        var dataPath = GetDataPath(bucketName, key);
        var dataDir = Path.GetDirectoryName(dataPath)!;
        Directory.CreateDirectory(dataDir);

        // Create a temporary file in the same directory to ensure atomic move
        var tempPath = Path.Combine(dataDir, $"{_tempFilePrefix}{Guid.NewGuid():N}");
        long bytesWritten = 0;

        try
        {
            // Write the decoded data to temp file
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

                // Parse AWS chunked encoding and write decoded data directly to file with signature validation
                try
                {
                    bytesWritten = await _chunkedDataParser.ParseChunkedDataToStreamAsync(dataReader, fileStream, chunkValidator, cancellationToken);
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
        // Validate that the key doesn't conflict with temporary files or metadata directories
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            throw new InvalidOperationException($"Cannot store data with key '{key}' as it conflicts with temporary file pattern '{_tempFilePrefix}' or metadata directory '{_inlineMetadataDirectoryName}'");
        }

        var dataPath = GetDataPath(bucketName, key);
        var dataDir = Path.GetDirectoryName(dataPath)!;
        Directory.CreateDirectory(dataDir);

        // Create a temporary file in the same directory to ensure atomic move
        var tempPath = Path.Combine(dataDir, $"{_tempFilePrefix}{Guid.NewGuid():N}");
        long totalBytesWritten = 0;

        try
        {
            // Write all parts to the temp file, ensuring proper disposal before computing ETag
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

                foreach (var reader in partReaders)
                {
                    var bytesWritten = await PipeReaderHelper.CopyToAsync(reader, fileStream, true, cancellationToken);
                    totalBytesWritten += bytesWritten;
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
        // Validate that the key doesn't conflict with temporary files or metadata directories
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return false;  // Return false to indicate object not found
        }

        var dataPath = GetDataPath(bucketName, key);

        if (!File.Exists(dataPath))
        {
            return false;
        }

        // Check if this is a temporary file
        var fileName = Path.GetFileName(dataPath);
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
        {
            return false;  // Temporary files should be invisible
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
        // Validate that the key doesn't conflict with temporary files or metadata directories
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return false;  // Cannot delete forbidden paths
        }

        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return false;
        }

        // Check if this is a temporary file
        var fileName = Path.GetFileName(dataPath);
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
        {
            return false;  // Temporary files should be invisible
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
        // Validate that the key doesn't conflict with temporary files or metadata directories
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return Task.FromResult(false);
        }

        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return Task.FromResult(false);
        }

        // Check if this is a temporary file
        var fileName = Path.GetFileName(dataPath);
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
        {
            return Task.FromResult(false);  // Temporary files should be invisible
        }

        return Task.FromResult(true);
    }

    public Task<(long size, DateTime lastModified)?> GetDataInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        // Validate that the key doesn't conflict with temporary files or metadata directories
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return Task.FromResult<(long size, DateTime lastModified)?>(null);
        }

        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return Task.FromResult<(long size, DateTime lastModified)?>(null);
        }

        // Check if this is a temporary file
        var fileName = Path.GetFileName(dataPath);
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
        {
            return Task.FromResult<(long size, DateTime lastModified)?>(null);  // Temporary files should be invisible
        }

        var fileInfo = new FileInfo(dataPath);
        return Task.FromResult<(long size, DateTime lastModified)?>((fileInfo.Length, fileInfo.LastWriteTimeUtc));
    }


    public Task<ListDataResult> ListDataKeysAsync(string bucketName, string? prefix = null, string? delimiter = null, string? startAfter = null, int? maxKeys = null, CancellationToken cancellationToken = default)
    {
        var result = new ListDataResult();

        // Validate bucket name
        if (FilesystemStorageHelper.IsKeyForbidden(bucketName, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return Task.FromResult(result);
        }

        var bucketPath = Path.Combine(_dataDirectory, bucketName);
        if (!Directory.Exists(bucketPath))
        {
            return Task.FromResult(result);
        }

        // If no delimiter, use iterator-based collection with early termination
        if (string.IsNullOrEmpty(delimiter))
        {
            // Calculate the most specific starting directory based on prefix
            var keysStartPath = CalculateOptimalStartingPath(bucketPath, prefix);

            // Use iterator to collect keys with early termination
            var keys = CollectKeysIteratively(keysStartPath, bucketName, prefix, startAfter, maxKeys);
            result.Keys.AddRange(keys);
            return Task.FromResult(result);
        }

        // Process with delimiter logic and pagination
        var prefixLength = prefix?.Length ?? 0;
        var allItems = new SortedSet<(string item, bool isPrefix)>(
            Comparer<(string item, bool isPrefix)>.Create((x, y) =>
                string.Compare(x.item, y.item, StringComparison.Ordinal)));

        // Calculate the most specific starting directory based on prefix
        var delimiterStartPath = CalculateOptimalStartingPath(bucketPath, prefix);
        CollectKeysWithDelimiter(delimiterStartPath, bucketName, prefix, delimiter, prefixLength, allItems, startAfter, maxKeys);

        // Apply maxKeys limit to final sorted results and materialize to avoid multiple enumeration
        var finalItems = allItems.AsEnumerable();
        if (maxKeys.HasValue)
        {
            finalItems = finalItems.Take(maxKeys.Value);
        }
        var materializedItems = finalItems.ToList();

        // Separate into keys and common prefixes
        result.Keys.AddRange(materializedItems.Where(item => !item.isPrefix).Select(item => item.item));
        result.CommonPrefixes.AddRange(materializedItems.Where(item => item.isPrefix).Select(item => item.item));

        return Task.FromResult(result);
    }

    private bool CollectKeysWithDelimiter(string currentPath, string bucketName, string? prefix, string delimiter, int prefixLength, SortedSet<(string item, bool isPrefix)> allItems, string? startAfter, int? maxKeys)
    {
        if (!Directory.Exists(currentPath))
        {
            return false;
        }

        // Check if we've already collected enough items
        if (maxKeys.HasValue && allItems.Count >= maxKeys.Value)
        {
            return true; // Signal that we should stop collecting
        }

        // Process files in current directory
        foreach (var file in Directory.GetFiles(currentPath))
        {
            var fileName = Path.GetFileName(file);

            // Skip temporary files
            if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
            {
                continue;
            }

            // Skip inline metadata directories
            if (_metadataMode == MetadataStorageMode.Inline && fileName == _inlineMetadataDirectoryName)
            {
                continue;
            }

            var relativePath = Path.GetRelativePath(Path.Combine(_dataDirectory, bucketName), file)
                .Replace(Path.DirectorySeparatorChar, '/');

            // Apply prefix filter
            if (!string.IsNullOrEmpty(prefix) && !relativePath.StartsWith(prefix))
            {
                continue;
            }

            // Check for delimiter after prefix
            var remainingKey = relativePath.Substring(prefixLength);
            var delimiterIndex = remainingKey.IndexOf(delimiter, StringComparison.Ordinal);

            string itemToAdd;
            bool isPrefix;

            if (delimiterIndex >= 0)
            {
                // Found delimiter - add as common prefix
                itemToAdd = relativePath.Substring(0, prefixLength + delimiterIndex + delimiter.Length);
                isPrefix = true;
            }
            else
            {
                // No delimiter - direct file
                itemToAdd = relativePath;
                isPrefix = false;
            }

            // Apply startAfter filter
            if (!string.IsNullOrEmpty(startAfter) && string.Compare(itemToAdd, startAfter, StringComparison.Ordinal) <= 0)
            {
                continue;
            }

            allItems.Add((itemToAdd, isPrefix));

            // Check if we've reached the limit after adding this item
            if (maxKeys.HasValue && allItems.Count >= maxKeys.Value)
            {
                return true; // Stop collecting
            }
        }

        // Process subdirectories - sort them first to ensure lexicographical order
        var directories = Directory.GetDirectories(currentPath);
        Array.Sort(directories, StringComparer.Ordinal);

        foreach (var dir in directories)
        {
            var dirName = Path.GetFileName(dir);

            // Skip inline metadata directories
            if (_metadataMode == MetadataStorageMode.Inline && dirName == _inlineMetadataDirectoryName)
            {
                continue;
            }

            var relativePath = Path.GetRelativePath(Path.Combine(_dataDirectory, bucketName), dir)
                .Replace(Path.DirectorySeparatorChar, '/') + "/";

            // Apply prefix filter - only process directories that could contain matching keys
            if (!string.IsNullOrEmpty(prefix) && !relativePath.StartsWith(prefix) && !prefix.StartsWith(relativePath))
            {
                continue;
            }

            if (!string.IsNullOrEmpty(prefix) && relativePath.StartsWith(prefix))
            {
                // Check for delimiter after prefix in directory path
                var remainingKey = relativePath.Substring(prefixLength);
                var delimiterIndex = remainingKey.IndexOf(delimiter, StringComparison.Ordinal);

                if (delimiterIndex >= 0)
                {
                    // Found delimiter - add as common prefix
                    var commonPrefix = relativePath.Substring(0, prefixLength + delimiterIndex + delimiter.Length);

                    // Apply startAfter filter
                    if (string.IsNullOrEmpty(startAfter) || string.Compare(commonPrefix, startAfter, StringComparison.Ordinal) > 0)
                    {
                        allItems.Add((commonPrefix, true));

                        // Check if we've reached the limit after adding this item
                        if (maxKeys.HasValue && allItems.Count >= maxKeys.Value)
                        {
                            return true; // Stop collecting
                        }
                    }

                    continue; // Don't recurse into this directory
                }
            }

            // Recurse into subdirectory - stop if we've collected enough items
            if (CollectKeysWithDelimiter(dir, bucketName, prefix, delimiter, prefixLength, allItems, startAfter, maxKeys))
            {
                return true; // Stop collecting if subdirectory signaled to stop
            }
        }

        return false; // Continue collecting
    }

    private void CollectKeys(string currentPath, string bucketName, string? prefixFilter, SortedSet<string> keys, string? startAfter)
    {
        if (!Directory.Exists(currentPath))
        {
            return;
        }

        // Process files in current directory
        foreach (var file in Directory.GetFiles(currentPath))
        {
            var fileName = Path.GetFileName(file);

            // Skip temporary files
            if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
            {
                continue;
            }

            // Skip inline metadata directories
            if (_metadataMode == MetadataStorageMode.Inline && fileName == _inlineMetadataDirectoryName)
            {
                continue;
            }

            var relativePath = Path.GetRelativePath(Path.Combine(_dataDirectory, bucketName), file)
                .Replace(Path.DirectorySeparatorChar, '/');

            // Apply prefix filter
            if (!string.IsNullOrEmpty(prefixFilter) && !relativePath.StartsWith(prefixFilter))
            {
                continue;
            }

            // Apply startAfter filter
            if (!string.IsNullOrEmpty(startAfter) && string.Compare(relativePath, startAfter, StringComparison.Ordinal) <= 0)
            {
                continue;
            }

            keys.Add(relativePath);

        }

        // Process subdirectories - only recurse if they could contain matching keys and we need more keys
        foreach (var dir in Directory.GetDirectories(currentPath))
        {
            var dirName = Path.GetFileName(dir);

            // Skip inline metadata directories
            if (_metadataMode == MetadataStorageMode.Inline && dirName == _inlineMetadataDirectoryName)
            {
                continue;
            }

            // Calculate the directory path relative to bucket
            var dirRelativePath = Path.GetRelativePath(Path.Combine(_dataDirectory, bucketName), dir)
                .Replace(Path.DirectorySeparatorChar, '/') + "/";

            // Only recurse if this directory could contain keys matching our prefix
            if (string.IsNullOrEmpty(prefixFilter) ||
                prefixFilter.StartsWith(dirRelativePath) ||
                dirRelativePath.StartsWith(prefixFilter))
            {
                // Recurse into subdirectory
                CollectKeys(dir, bucketName, prefixFilter, keys, startAfter);
            }
        }
    }

    private IEnumerable<string> CollectKeysIteratively(string currentPath, string bucketName, string? prefixFilter, string? startAfter, int? maxKeys)
    {
        var sortedKeys = new SortedSet<string>(StringComparer.Ordinal);
        CollectKeysOptimized(currentPath, bucketName, prefixFilter, sortedKeys, startAfter, maxKeys);
        return sortedKeys;
    }

    private bool CollectKeysOptimized(string currentPath, string bucketName, string? prefixFilter, SortedSet<string> keys, string? startAfter, int? maxKeys)
    {
        if (!Directory.Exists(currentPath))
        {
            return false;
        }

        // Check if we've already collected enough items
        if (maxKeys.HasValue && keys.Count >= maxKeys.Value)
        {
            return true; // Signal that we should stop collecting
        }

        // Process files in current directory first
        var files = Directory.GetFiles(currentPath);
        Array.Sort(files, StringComparer.Ordinal);

        foreach (var file in files)
        {
            var fileName = Path.GetFileName(file);

            // Skip temporary files
            if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
            {
                continue;
            }

            // Skip inline metadata directories
            if (_metadataMode == MetadataStorageMode.Inline && fileName == _inlineMetadataDirectoryName)
            {
                continue;
            }

            var relativePath = Path.GetRelativePath(Path.Combine(_dataDirectory, bucketName), file)
                .Replace(Path.DirectorySeparatorChar, '/');

            // Apply prefix filter
            if (!string.IsNullOrEmpty(prefixFilter) && !relativePath.StartsWith(prefixFilter))
            {
                continue;
            }

            // Apply startAfter filter
            if (!string.IsNullOrEmpty(startAfter) && string.Compare(relativePath, startAfter, StringComparison.Ordinal) <= 0)
            {
                continue;
            }

            keys.Add(relativePath);

            // Check if we've reached the limit after adding this item
            if (maxKeys.HasValue && keys.Count >= maxKeys.Value)
            {
                return true; // Stop collecting
            }
        }

        // Process subdirectories - sort them first to ensure lexicographical order
        var directories = Directory.GetDirectories(currentPath);
        Array.Sort(directories, StringComparer.Ordinal);

        foreach (var dir in directories)
        {
            var dirName = Path.GetFileName(dir);

            // Skip inline metadata directories
            if (_metadataMode == MetadataStorageMode.Inline && dirName == _inlineMetadataDirectoryName)
            {
                continue;
            }

            // Calculate the directory path relative to bucket
            var dirRelativePath = Path.GetRelativePath(Path.Combine(_dataDirectory, bucketName), dir)
                .Replace(Path.DirectorySeparatorChar, '/') + "/";

            // Only recurse if this directory could contain keys matching our prefix
            if (string.IsNullOrEmpty(prefixFilter) ||
                prefixFilter.StartsWith(dirRelativePath) ||
                dirRelativePath.StartsWith(prefixFilter))
            {
                // Recurse into subdirectory - stop if we've collected enough items
                if (CollectKeysOptimized(dir, bucketName, prefixFilter, keys, startAfter, maxKeys))
                {
                    return true; // Stop collecting if subdirectory signaled to stop
                }
            }
        }

        return false; // Continue collecting
    }

    public async Task<string?> ComputeETagAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        // Validate that the key doesn't conflict with temporary files or metadata directories
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return null;
        }

        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return null;
        }

        // Check if this is a temporary file
        var fileName = Path.GetFileName(dataPath);
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
        {
            return null;  // Temporary files should be invisible
        }

        // Use ETagHelper which efficiently computes hash without loading entire file into memory
        return await ETagHelper.ComputeETagFromFileAsync(dataPath);
    }

    private string GetDataPath(string bucketName, string key)
    {
        return Path.Combine(_dataDirectory, bucketName, key);
    }

    private string CalculateOptimalStartingPath(string bucketPath, string? prefix)
    {
        if (string.IsNullOrEmpty(prefix))
        {
            return bucketPath;
        }

        // Convert prefix to a filesystem path
        var prefixPath = prefix.Replace('/', Path.DirectorySeparatorChar);
        var fullPrefixPath = Path.Combine(bucketPath, prefixPath);

        // Find the deepest existing directory that contains potential matches
        var directoryPath = fullPrefixPath;
        
        // If the full prefix path is a directory, start from there
        if (Directory.Exists(directoryPath))
        {
            return directoryPath;
        }

        // Otherwise, find the parent directory that exists
        while (!string.IsNullOrEmpty(directoryPath) && !Directory.Exists(directoryPath))
        {
            directoryPath = Path.GetDirectoryName(directoryPath);
        }

        // Return the deepest existing directory, or bucket path if nothing exists
        return string.IsNullOrEmpty(directoryPath) || !directoryPath.StartsWith(bucketPath) ? bucketPath : directoryPath;
    }

}