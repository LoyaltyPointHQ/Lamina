using System.IO.Pipelines;
using Lamina.Helpers;
using Lamina.Models;
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
        try
        {
            // Write the data to temp file, ensuring proper disposal before computing ETag
            long bytesWritten;
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
        try
        {
            // Write the decoded data to temp file
            long bytesWritten;
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


    public Task<ListDataResult> ListDataKeysAsync(string bucketName, BucketType bucketType, string? prefix = null, string? delimiter = null, string? startAfter = null, int? maxKeys = null, CancellationToken cancellationToken = default)
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

        // Choose implementation based on bucket type
        if (bucketType == BucketType.Directory)
        {
            // Directory buckets: Use filesystem enumeration order (no sorting)
            return Task.FromResult(ListDataKeysForDirectoryBucket(bucketPath, bucketName, prefix, delimiter, startAfter, maxKeys));
        }
        else
        {
            // General purpose buckets: Maintain lexicographical order
            return Task.FromResult(ListDataKeysForGeneralPurposeBucket(bucketPath, bucketName, prefix, delimiter, startAfter, maxKeys));
        }
    }

    private ListDataResult ListDataKeysForDirectoryBucket(string bucketPath, string bucketName, string? prefix, string? delimiter, string? startAfter, int? maxKeys)
    {
        var result = new ListDataResult();

        if (string.IsNullOrEmpty(delimiter))
        {
            // No delimiter - enumerate all files in filesystem order
            var keys = EnumerateKeysForDirectoryBucketNoDelimiter(bucketPath, bucketName, prefix, startAfter, maxKeys);
            result.Keys.AddRange(keys);
        }
        else
        {
            // With delimiter - enumerate with delimiter handling in filesystem order
            var (keys, commonPrefixes) = EnumerateKeysForDirectoryBucketWithDelimiter(bucketPath, bucketName, prefix, delimiter, startAfter, maxKeys);
            result.Keys.AddRange(keys);
            result.CommonPrefixes.AddRange(commonPrefixes);
        }

        return result;
    }

    private ListDataResult ListDataKeysForGeneralPurposeBucket(string bucketPath, string bucketName, string? prefix, string? delimiter, string? startAfter, int? maxKeys)
    {
        var result = new ListDataResult();

        if (string.IsNullOrEmpty(delimiter))
        {
            // No delimiter - use sorted enumeration for lexicographical order
            var keys = EnumerateKeysForGeneralPurposeBucketNoDelimiter(bucketPath, bucketName, prefix, startAfter, maxKeys);
            result.Keys.AddRange(keys);
        }
        else
        {
            // With delimiter - use sorted enumeration with delimiter handling
            var (keys, commonPrefixes) = EnumerateKeysForGeneralPurposeBucketWithDelimiter(bucketPath, bucketName, prefix, delimiter, startAfter, maxKeys);
            result.Keys.AddRange(keys);
            result.CommonPrefixes.AddRange(commonPrefixes);
        }

        return result;
    }

    private IEnumerable<string> EnumerateKeysForDirectoryBucketNoDelimiter(string bucketPath, string bucketName, string? prefix, string? startAfter, int? maxKeys)
    {
        var count = 0;
        var startPath = CalculateOptimalStartingPath(bucketPath, prefix);

        // Use lazy enumeration for Directory buckets - no sorting needed
        foreach (var key in EnumerateAllKeysLazily(startPath, bucketName, prefix))
        {
            // Apply startAfter filter
            if (!string.IsNullOrEmpty(startAfter) && string.Compare(key, startAfter, StringComparison.Ordinal) <= 0)
            {
                continue;
            }

            yield return key;
            count++;

            // Early termination for Directory buckets
            if (maxKeys.HasValue && count >= maxKeys.Value)
            {
                yield break;
            }
        }
    }

    private (List<string> keys, List<string> commonPrefixes) EnumerateKeysForDirectoryBucketWithDelimiter(string bucketPath, string bucketName, string? prefix, string delimiter, string? startAfter, int? maxKeys)
    {
        // Use optimized single-directory scan for forward slash delimiter
        if (delimiter == "/")
        {
            return ListDataKeysWithDelimiterOptimized(bucketPath, prefix, delimiter, startAfter, maxKeys, sortResults: false);
        }

        // Fall back to full enumeration for other delimiters (rare case)
        var keys = new List<string>();
        var commonPrefixes = new List<string>();
        var count = 0;
        var prefixLength = prefix?.Length ?? 0;
        var startPath = CalculateOptimalStartingPath(bucketPath, prefix);
        var seenPrefixes = new HashSet<string>();

        // Use lazy enumeration for Directory buckets - filesystem order
        foreach (var key in EnumerateAllKeysLazily(startPath, bucketName, prefix))
        {
            // Apply startAfter filter early
            if (!string.IsNullOrEmpty(startAfter) && string.Compare(key, startAfter, StringComparison.Ordinal) <= 0)
            {
                continue;
            }

            // Check for delimiter after prefix
            var remainingKey = key.Substring(prefixLength);
            var delimiterIndex = remainingKey.IndexOf(delimiter, StringComparison.Ordinal);

            if (delimiterIndex >= 0)
            {
                // Found delimiter - this is a common prefix
                var commonPrefix = key.Substring(0, prefixLength + delimiterIndex + delimiter.Length);

                if (seenPrefixes.Add(commonPrefix)) // Only add if not seen before
                {
                    commonPrefixes.Add(commonPrefix);
                    count++;
                }
            }
            else
            {
                // No delimiter - direct key
                keys.Add(key);
                count++;
            }

            // Early termination
            if (maxKeys.HasValue && count >= maxKeys.Value)
            {
                break;
            }
        }

        return (keys, commonPrefixes);
    }

    private IEnumerable<string> EnumerateKeysForGeneralPurposeBucketNoDelimiter(string bucketPath, string bucketName, string? prefix, string? startAfter, int? maxKeys)
    {
        var sortedKeys = new SortedSet<string>(StringComparer.Ordinal);
        var startPath = CalculateOptimalStartingPath(bucketPath, prefix);

        // Collect into sorted set for lexicographical order
        foreach (var key in EnumerateAllKeysLazily(startPath, bucketName, prefix))
        {
            // Apply startAfter filter during enumeration for efficiency
            if (!string.IsNullOrEmpty(startAfter) && string.Compare(key, startAfter, StringComparison.Ordinal) <= 0)
            {
                continue;
            }

            sortedKeys.Add(key);

            // Early termination if we have enough
            if (maxKeys.HasValue && sortedKeys.Count >= maxKeys.Value)
            {
                break;
            }
        }

        return sortedKeys;
    }

    private (List<string> keys, List<string> commonPrefixes) EnumerateKeysForGeneralPurposeBucketWithDelimiter(string bucketPath, string bucketName, string? prefix, string delimiter, string? startAfter, int? maxKeys)
    {
        // Use optimized single-directory scan for forward slash delimiter
        if (delimiter == "/")
        {
            return ListDataKeysWithDelimiterOptimized(bucketPath, prefix, delimiter, startAfter, maxKeys, sortResults: true);
        }

        // Fall back to full enumeration for other delimiters (rare case)
        var allItems = new SortedSet<(string item, bool isPrefix)>(
            Comparer<(string item, bool isPrefix)>.Create((x, y) =>
                string.Compare(x.item, y.item, StringComparison.Ordinal)));

        var prefixLength = prefix?.Length ?? 0;
        var startPath = CalculateOptimalStartingPath(bucketPath, prefix);

        // Collect into sorted set for lexicographical order
        foreach (var key in EnumerateAllKeysLazily(startPath, bucketName, prefix))
        {
            // Apply startAfter filter early
            if (!string.IsNullOrEmpty(startAfter) && string.Compare(key, startAfter, StringComparison.Ordinal) <= 0)
            {
                continue;
            }

            // Check for delimiter after prefix
            var remainingKey = key.Substring(prefixLength);
            var delimiterIndex = remainingKey.IndexOf(delimiter, StringComparison.Ordinal);

            string itemToAdd;
            bool isPrefix;

            if (delimiterIndex >= 0)
            {
                // Found delimiter - add as common prefix
                itemToAdd = key.Substring(0, prefixLength + delimiterIndex + delimiter.Length);
                isPrefix = true;
            }
            else
            {
                // No delimiter - direct key
                itemToAdd = key;
                isPrefix = false;
            }

            allItems.Add((itemToAdd, isPrefix));

            // Early termination
            if (maxKeys.HasValue && allItems.Count >= maxKeys.Value)
            {
                break;
            }
        }

        // Separate into keys and common prefixes
        var keys = allItems.Where(item => !item.isPrefix).Select(item => item.item).ToList();
        var commonPrefixes = allItems.Where(item => item.isPrefix).Select(item => item.item).ToList();

        return (keys, commonPrefixes);
    }

    private IEnumerable<string> EnumerateAllKeysLazily(string startPath, string bucketName, string? prefix)
    {
        if (!Directory.Exists(startPath))
        {
            yield break;
        }

        // Use Directory.EnumerateFiles for lazy enumeration - much more memory efficient
        foreach (var file in Directory.EnumerateFiles(startPath, "*", SearchOption.AllDirectories))
        {
            var fileName = Path.GetFileName(file);

            // Skip temporary files
            if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
            {
                continue;
            }

            // Skip inline metadata directories
            var relativePath = Path.GetRelativePath(Path.Combine(_dataDirectory, bucketName), file);
            if (_metadataMode == MetadataStorageMode.Inline &&
                relativePath.Contains(Path.DirectorySeparatorChar + _inlineMetadataDirectoryName + Path.DirectorySeparatorChar))
            {
                continue;
            }

            // Convert to S3 key format
            var key = relativePath.Replace(Path.DirectorySeparatorChar, '/');

            // Apply prefix filter
            if (!string.IsNullOrEmpty(prefix) && !key.StartsWith(prefix))
            {
                continue;
            }

            yield return key;
        }
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

    /// <summary>
    /// Gets the parent directory path from a prefix for optimized delimiter-based scanning.
    /// For prefix "a/b/c", returns ("a/b/", "c") to scan directory "a/b/" for entries starting with "c".
    /// </summary>
    /// <param name="bucketPath">The base bucket directory path</param>
    /// <param name="prefix">The S3 prefix (e.g., "a/b/c")</param>
    /// <param name="delimiter">The delimiter character (typically "/")</param>
    /// <returns>Tuple of (parentDirectoryPath, filterPrefix)</returns>
    private (string parentDirectoryPath, string filterPrefix) GetParentDirectoryFromPrefix(string bucketPath, string? prefix, string delimiter)
    {
        if (string.IsNullOrEmpty(prefix))
        {
            // No prefix - scan bucket root, no filter
            return (bucketPath, string.Empty);
        }

        var lastDelimiterIndex = prefix.LastIndexOf(delimiter, StringComparison.Ordinal);

        if (lastDelimiterIndex == -1)
        {
            // No delimiter in prefix - scan bucket root, filter by entire prefix
            return (bucketPath, prefix);
        }

        // Split at last delimiter
        var parentPrefix = prefix.Substring(0, lastDelimiterIndex + delimiter.Length); // "a/b/"
        var filterPrefix = prefix.Substring(lastDelimiterIndex + delimiter.Length);    // "c"

        // Convert to filesystem path
        var parentPath = Path.Combine(bucketPath, parentPrefix.Replace('/', Path.DirectorySeparatorChar).TrimEnd(Path.DirectorySeparatorChar));

        return (parentPath, filterPrefix);
    }

    /// <summary>
    /// Optimized listing method that performs single-directory scan when delimiter is "/".
    /// This provides massive performance improvements for hierarchical data structures.
    /// </summary>
    /// <param name="bucketPath">The bucket directory path</param>
    /// <param name="prefix">The S3 prefix to filter by</param>
    /// <param name="delimiter">The delimiter (must be "/")</param>
    /// <param name="startAfter">The continuation token</param>
    /// <param name="maxKeys">Maximum number of keys to return</param>
    /// <param name="sortResults">Whether to sort results (true for general-purpose, false for directory buckets)</param>
    /// <returns>Lists of keys and common prefixes</returns>
    private (List<string> keys, List<string> commonPrefixes) ListDataKeysWithDelimiterOptimized(
        string bucketPath, string? prefix, string delimiter,
        string? startAfter, int? maxKeys, bool sortResults)
    {
        var (parentDirectoryPath, filterPrefix) = GetParentDirectoryFromPrefix(bucketPath, prefix, delimiter);

        var keys = new List<string>();
        var commonPrefixSet = new HashSet<string>();
        var totalItems = 0;
        var effectiveMaxKeys = maxKeys ?? int.MaxValue;

        // Check if parent directory exists
        if (!Directory.Exists(parentDirectoryPath))
        {
            return (keys, []);
        }

        // Get all entries in the parent directory (single-level scan only)
        var allEntries = new List<(string name, bool isDirectory, string fullS3Key)>();

        try
        {
            // Enumerate files and directories in the parent directory
            foreach (var entry in Directory.EnumerateFileSystemEntries(parentDirectoryPath))
            {
                var entryName = Path.GetFileName(entry);

                // Skip temporary files
                if (FilesystemStorageHelper.IsTemporaryFile(entryName, _tempFilePrefix))
                {
                    continue;
                }

                // Skip inline metadata directories
                if (_metadataMode == MetadataStorageMode.Inline && entryName == _inlineMetadataDirectoryName)
                {
                    continue;
                }

                // Apply prefix filter at directory level
                if (!string.IsNullOrEmpty(filterPrefix) && !entryName.StartsWith(filterPrefix))
                {
                    continue;
                }

                var isDirectory = Directory.Exists(entry);

                // Construct full S3 key
                string fullS3Key;
                if (string.IsNullOrEmpty(prefix))
                {
                    fullS3Key = entryName;
                }
                else
                {
                    var parentS3Prefix = prefix.Substring(0, Math.Max(0, prefix.Length - filterPrefix.Length));
                    fullS3Key = parentS3Prefix + entryName;
                }

                allEntries.Add((entryName, isDirectory, fullS3Key));
            }
        }
        catch (DirectoryNotFoundException)
        {
            // Directory doesn't exist - return empty results
            return (keys, new List<string>());
        }
        catch (UnauthorizedAccessException)
        {
            // Permission denied - return empty results
            return (keys, new List<string>());
        }

        // Sort entries if required (general-purpose buckets need lexicographical order)
        if (sortResults)
        {
            allEntries.Sort((a, b) => string.Compare(a.fullS3Key, b.fullS3Key, StringComparison.Ordinal));
        }

        // Process entries and apply pagination
        foreach (var (_, isDirectory, fullS3Key) in allEntries)
        {
            // Apply startAfter filter
            if (!string.IsNullOrEmpty(startAfter) && string.Compare(fullS3Key, startAfter, StringComparison.Ordinal) <= 0)
            {
                continue;
            }

            if (totalItems >= effectiveMaxKeys)
            {
                break;
            }

            if (isDirectory)
            {
                // Directory becomes a common prefix
                var commonPrefix = fullS3Key + delimiter;
                if (commonPrefixSet.Add(commonPrefix))
                {
                    totalItems++;
                }
            }
            else
            {
                // File becomes a direct key
                keys.Add(fullS3Key);
                totalItems++;
            }
        }

        var commonPrefixes = commonPrefixSet.ToList();
        if (sortResults)
        {
            commonPrefixes.Sort(StringComparer.Ordinal);
        }

        return (keys, commonPrefixes);
    }

}