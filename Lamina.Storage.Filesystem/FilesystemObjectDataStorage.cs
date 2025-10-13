using System.Buffers;
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
        IChunkedDataParser chunkedDataParser
    )
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



    public async Task<StorageResult<(long size, string etag, Dictionary<string, string> checksums)>> StoreDataAsync(
        string bucketName,
        string key,
        PipeReader dataReader,
        IChunkSignatureValidator? chunkValidator,
        ChecksumRequest? checksumRequest,
        CancellationToken cancellationToken = default
    )
    {
        // Validate that the key doesn't conflict with temporary files or metadata directories
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            throw new InvalidOperationException(
                $"Cannot store data with key '{key}' as it conflicts with temporary file pattern '{_tempFilePrefix}' or metadata directory '{_inlineMetadataDirectoryName}'");
        }

        var dataPath = GetDataPath(bucketName, key);
        var dataDir = Path.GetDirectoryName(dataPath)!;
        Directory.CreateDirectory(dataDir);

        // Create a temporary file in the same directory to ensure atomic move
        var tempPath = Path.Combine(dataDir, $"{_tempFilePrefix}{Guid.NewGuid():N}");

        // Initialize checksum calculator if needed
        StreamingChecksumCalculator? checksumCalculator = null;
        if (checksumRequest != null)
        {
            checksumCalculator = new StreamingChecksumCalculator(checksumRequest.Algorithm, checksumRequest.ProvidedChecksums);
        }

        try
        {
            long bytesWritten;

            // Handle chunked data with validation if validator is provided
            if (chunkValidator != null)
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

                // Parse AWS chunked encoding and write decoded data directly to file with signature validation
                var parseResult = await _chunkedDataParser.ParseChunkedDataToStreamAsync(dataReader, fileStream, chunkValidator, cancellationToken);

                // Check if validation succeeded
                if (!parseResult.Success)
                {
                    // Invalid chunk signature - clean up and return failure
                    await fileStream.FlushAsync(cancellationToken);
                    fileStream.Close();
                    File.Delete(tempPath);
                    return StorageResult<(long size, string etag, Dictionary<string, string> checksums)>.Error("SignatureDoesNotMatch", "Chunk signature validation failed");
                }

                bytesWritten = parseResult.TotalBytesWritten;
                await fileStream.FlushAsync(cancellationToken);
            }
            else
            {
                // Standard write without chunked encoding
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

                // If we need checksums, we need to calculate while writing
                if (checksumCalculator?.HasChecksums == true)
                {
                    bytesWritten = 0;
                    ReadResult readResult;
                    do
                    {
                        readResult = await dataReader.ReadAsync(cancellationToken);
                        var buffer = readResult.Buffer;

                        foreach (var segment in buffer)
                        {
                            // Update checksum calculator
                            checksumCalculator.Append(segment.Span);

                            // Write to file
                            await fileStream.WriteAsync(segment, cancellationToken);
                            bytesWritten += segment.Length;
                        }

                        dataReader.AdvanceTo(buffer.End);
                    } while (!readResult.IsCompleted);

                    await dataReader.CompleteAsync();
                }
                else
                {
                    bytesWritten = await PipeReaderHelper.CopyToAsync(dataReader, fileStream, false, cancellationToken);
                }

                await fileStream.FlushAsync(cancellationToken);
            }

            // Calculate checksums from the file if needed but not yet calculated
            var checksums = new Dictionary<string, string>();
            if (checksumCalculator?.HasChecksums == true)
            {
                // If we didn't calculate during write (chunked path), read file now
                if (chunkValidator != null)
                {
                    await using var fileStream = File.OpenRead(tempPath);
                    var buffer = ArrayPool<byte>.Shared.Rent(8192);
                    try
                    {
                        int read;
                        while ((read = await fileStream.ReadAsync(buffer, cancellationToken)) > 0)
                        {
                            checksumCalculator.Append(buffer.AsSpan(0, read));
                        }
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                    }
                }

                var result = checksumCalculator.Finish();

                if (!result.IsValid)
                {
                    // Checksum validation failed - clean up and return error
                    File.Delete(tempPath);
                    return StorageResult<(long size, string etag, Dictionary<string, string> checksums)>.Error("InvalidChecksum", result.ErrorMessage ?? "Checksum validation failed");
                }

                checksums = result.CalculatedChecksums;
            }

            // Now compute ETag from the temp file on disk with a new file handle
            var etag = await ETagHelper.ComputeETagFromFileAsync(tempPath);

            // Atomically move the temp file to the final location
            await _networkHelper.AtomicMoveAsync(tempPath, dataPath, overwrite: true);

            return StorageResult<(long size, string etag, Dictionary<string, string> checksums)>.Success((bytesWritten, etag, checksums));
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
        finally
        {
            checksumCalculator?.Dispose();
        }
    }

    public async Task<(long size, string etag)> StoreMultipartDataAsync(string bucketName, string key, IEnumerable<PipeReader> partReaders, CancellationToken cancellationToken = default)
    {
        // Validate that the key doesn't conflict with temporary files or metadata directories
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            throw new InvalidOperationException(
                $"Cannot store data with key '{key}' as it conflicts with temporary file pattern '{_tempFilePrefix}' or metadata directory '{_inlineMetadataDirectoryName}'");
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
            return false; // Return false to indicate object not found
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
            return false; // Temporary files should be invisible
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
            return false; // Cannot delete forbidden paths
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
            return false; // Temporary files should be invisible
        }

        await _networkHelper.ExecuteWithRetryAsync(() =>
            {
                File.Delete(dataPath);
                return Task.FromResult(true);
            },
            "DeleteFile");

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
            return Task.FromResult(false); // Temporary files should be invisible
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
            return Task.FromResult<(long size, DateTime lastModified)?>(null); // Temporary files should be invisible
        }

        var fileInfo = new FileInfo(dataPath);
        return Task.FromResult<(long size, DateTime lastModified)?>((fileInfo.Length, fileInfo.LastWriteTimeUtc));
    }


    public Task<ListDataResult> ListDataKeysAsync(
        string bucketName,
        BucketType bucketType,
        string? prefix = null,
        string? delimiter = null,
        string? startAfter = null,
        int maxKeys = 1000,
        CancellationToken cancellationToken = default
    )
    {
        var validationResult = ValidateAndPreparePath(bucketName, prefix);
        if (!validationResult.IsValid)
        {
            return Task.FromResult(new ListDataResult());
        }

        var config = CreateTraversalConfiguration(bucketType, prefix, delimiter);
        
        var result = DoDirectoryTraversal(
            validationResult.Path,
            bucketName,
            prefix,
            delimiter,
            config,
            startAfter,
            maxKeys);

        return Task.FromResult(result);
    }

    private (bool IsValid, string Path) ValidateAndPreparePath(string bucketName, string? prefix)
    {
        // Validate bucket name
        if (FilesystemStorageHelper.IsKeyForbidden(bucketName, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return (false, string.Empty);
        }

        var bucketPath = Path.Combine(_dataDirectory, bucketName);
        if (!Directory.Exists(bucketPath))
        {
            return (false, string.Empty);
        }

        var path = Path.Combine(_dataDirectory, bucketPath, (prefix ?? "").Replace('/', Path.DirectorySeparatorChar));
        if (!string.IsNullOrEmpty(prefix))
            path = Path.GetDirectoryName(path)!;

        if (!path.StartsWith(Path.Combine(_dataDirectory, bucketPath)))
            throw new InvalidOperationException("Invalid prefix to bucket");

        return (true, path);
    }

    private TraversalConfiguration CreateTraversalConfiguration(BucketType bucketType, string? prefix, string? delimiter)
    {
        return new TraversalConfiguration
        {
            AllRecursive = delimiter != "/",
            NeedsFilter = delimiter is not null && delimiter != "/" || prefix?.EndsWith('/') == false,
            OrderedLexically = bucketType switch
            {
                BucketType.GeneralPurpose => true,
                BucketType.Directory => false,
                _ => throw new ArgumentOutOfRangeException(nameof(bucketType), bucketType, null)
            }
        };
    }

    private class TraversalConfiguration
    {
        public bool AllRecursive { get; set; }
        public bool NeedsFilter { get; set; }
        public bool OrderedLexically { get; set; }
    }

    private ListDataResult DoDirectoryTraversal(
        string path,
        string bucketName,
        string? prefix,
        string? delimiter,
        TraversalConfiguration config,
        string? startAfter,
        int maxKeys
    )
    {
        var result = new ListDataResult();
        if (!Directory.Exists(path))
            return result;

        var commonPrefixSet = new HashSet<string>();
        var traversalState = new TraversalState(result, commonPrefixSet, maxKeys);

        foreach (var entryName in GetFilesystemEnumerator(path, config.OrderedLexically, config.AllRecursive)
                     .SkipWhile(x => !string.IsNullOrEmpty(startAfter) && EntryNameToKey(path, x) != startAfter))
        {
            prefix ??= "";
            
            if (ShouldSkipEntry(entryName, bucketName, prefix, config.NeedsFilter, out var key))
                continue;

            if (HasReachedLimit(traversalState))
            {
                result.IsTruncated = true;
                result.StartAfter = key;
                break;
            }

            ProcessFileSystemEntry(entryName, key, prefix, delimiter, config.AllRecursive, traversalState);
        }

        FinalizeCommonPrefixes(traversalState, config.OrderedLexically);
        return result;
    }

    private class TraversalState
    {
        public ListDataResult Result { get; }
        public HashSet<string> CommonPrefixSet { get; }
        public int MaxKeys { get; }

        public TraversalState(ListDataResult result, HashSet<string> commonPrefixSet, int maxKeys)
        {
            Result = result;
            CommonPrefixSet = commonPrefixSet;
            MaxKeys = maxKeys;
        }
    }

    private bool ShouldSkipEntry(string entryName, string bucketName, string prefix, bool needsFilter, out string key)
    {
        key = string.Empty;
        
        if (IsEntryNameForbidden(entryName))
            return true;

        key = EntryNameToKey(bucketName, entryName);

        if (needsFilter && !string.IsNullOrEmpty(prefix) && !key.StartsWith(prefix))
            return true;

        return false;
    }

    private bool HasReachedLimit(TraversalState state)
    {
        return state.Result.Keys.Count + state.Result.CommonPrefixes.Count + state.CommonPrefixSet.Count >= state.MaxKeys;
    }

    private void ProcessFileSystemEntry(
        string entryName, 
        string key, 
        string prefix, 
        string? delimiter, 
        bool allRecursive,
        TraversalState state)
    {
        if (Directory.Exists(entryName))
        {
            ProcessDirectory(key, delimiter, allRecursive, state);
        }
        else
        {
            ProcessFile(key, prefix, delimiter, state);
        }
    }

    private void ProcessDirectory(string key, string? delimiter, bool allRecursive, TraversalState state)
    {
        if (!allRecursive && delimiter == "/")
        {
            state.Result.CommonPrefixes.Add($"{key}/");
        }
    }

    private void ProcessFile(string key, string prefix, string? delimiter, TraversalState state)
    {
        if (delimiter != null && delimiter != "/")
        {
            HandleDelimiterGrouping(key, prefix, delimiter, state);
        }
        else
        {
            state.Result.Keys.Add(key);
        }
    }

    private void HandleDelimiterGrouping(string key, string prefix, string delimiter, TraversalState state)
    {
        var localKey = key[prefix.Length..];
        if (localKey.Contains(delimiter))
        {
            var commonPrefix = $"{prefix}{localKey[..(localKey.IndexOf(delimiter, StringComparison.Ordinal) + 1)]}";
            state.CommonPrefixSet.Add(commonPrefix);
        }
        else
        {
            state.Result.Keys.Add(key);
        }
    }

    private void FinalizeCommonPrefixes(TraversalState state, bool orderedLexically)
    {
        if (state.CommonPrefixSet.Count > 0)
        {
            state.Result.CommonPrefixes.AddRange(state.CommonPrefixSet);
            if (orderedLexically)
            {
                state.Result.CommonPrefixes = state.Result.CommonPrefixes
                    .OrderBy(x => x, StringComparer.Ordinal)
                    .ToList();
            }
        }
    }

    private static IEnumerable<string> GetFilesystemEnumerator(string path, bool orderedLexically, bool allRecursive)
    {
        if (allRecursive)
        {
            if (orderedLexically)
                return Directory.EnumerateFileSystemEntries(path, "*", SearchOption.AllDirectories).OrderBy(x => x, StringComparer.Ordinal);

            return Directory.EnumerateFileSystemEntries(path, "*", SearchOption.AllDirectories);
        }

        if (orderedLexically)
            return Directory.EnumerateFileSystemEntries(path).OrderBy(x => x, StringComparer.Ordinal);

        return Directory.EnumerateFileSystemEntries(path);
    }

    private string EntryNameToKey(string bucketName, string entryName) => Path.GetRelativePath(Path.Combine(_dataDirectory, bucketName), entryName);

    private bool IsEntryNameForbidden(string entryName)
    {
        var fileName = Path.GetFileName(entryName);
        // Skip temporary files
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
        {
            return true;
        }

        // Skip inline metadata directories
        if (FilesystemStorageHelper.IsMetadataPath(entryName, _metadataMode, _inlineMetadataDirectoryName))
        {
            return true;
        }

        return false;
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
            return null; // Temporary files should be invisible
        }

        // Use ETagHelper which efficiently computes hash without loading entire file into memory
        return await ETagHelper.ComputeETagFromFileAsync(dataPath);
    }

    public async Task<(long size, string etag)?> CopyDataAsync(string sourceBucketName, string sourceKey, string destBucketName, string destKey, CancellationToken cancellationToken = default)
    {
        // Validate keys
        if (FilesystemStorageHelper.IsKeyForbidden(sourceKey, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName) ||
            FilesystemStorageHelper.IsKeyForbidden(destKey, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return null;
        }

        var sourcePath = GetDataPath(sourceBucketName, sourceKey);
        if (!File.Exists(sourcePath))
        {
            return null;
        }

        // Check if source is a temporary file
        var sourceFileName = Path.GetFileName(sourcePath);
        if (FilesystemStorageHelper.IsTemporaryFile(sourceFileName, _tempFilePrefix))
        {
            return null; // Temporary files should be invisible
        }

        var destPath = GetDataPath(destBucketName, destKey);
        var destDir = Path.GetDirectoryName(destPath)!;
        Directory.CreateDirectory(destDir);

        // Use a temporary file for atomic copy
        var tempPath = Path.Combine(destDir, $"{_tempFilePrefix}{Guid.NewGuid():N}");
        try
        {
            // Copy the file
            await _networkHelper.ExecuteWithRetryAsync(() =>
                {
                    File.Copy(sourcePath, tempPath, overwrite: false);
                    return Task.FromResult(true);
                },
                "CopyFile");

            // Move temp file to final destination atomically
            await _networkHelper.ExecuteWithRetryAsync(() =>
                {
                    File.Move(tempPath, destPath, overwrite: true);
                    return Task.FromResult(true);
                },
                "MoveFile");

            // Get the size and compute ETag of the destination file
            var fileInfo = new FileInfo(destPath);
            var etag = await ETagHelper.ComputeETagFromFileAsync(destPath);

            return (fileInfo.Length, etag);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error copying data from {SourceBucket}/{SourceKey} to {DestBucket}/{DestKey}",
                sourceBucketName, sourceKey, destBucketName, destKey);

            // Clean up temp file if it exists
            if (File.Exists(tempPath))
            {
                try
                {
                    File.Delete(tempPath);
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }

            return null;
        }
    }

    private string GetDataPath(string bucketName, string key)
    {
        return Path.Combine(_dataDirectory, bucketName, key);
    }
}