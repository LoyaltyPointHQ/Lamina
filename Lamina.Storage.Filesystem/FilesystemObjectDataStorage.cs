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
using Microsoft.Win32.SafeHandles;

namespace Lamina.Storage.Filesystem;

public class FilesystemObjectDataStorage : IObjectDataStorage, IFileBackedObjectDataStorage
{
    private readonly string _dataDirectory;
    private readonly MetadataStorageMode _metadataMode;
    private readonly string _inlineMetadataDirectoryName;
    private readonly string _tempFilePrefix;
    private readonly bool _zeroCopyEnabled;
    private readonly NetworkFileSystemHelper _networkHelper;
    private readonly LinuxZeroCopyHelper _zeroCopyHelper;
    private readonly ILogger<FilesystemObjectDataStorage> _logger;
    private readonly IChunkedDataParser _chunkedDataParser;

    public FilesystemObjectDataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        NetworkFileSystemHelper networkHelper,
        LinuxZeroCopyHelper zeroCopyHelper,
        ILogger<FilesystemObjectDataStorage> logger,
        IChunkedDataParser chunkedDataParser
    )
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _metadataMode = settings.MetadataMode;
        _inlineMetadataDirectoryName = settings.InlineMetadataDirectoryName;
        _tempFilePrefix = settings.TempFilePrefix;
        _zeroCopyEnabled = settings.UseZeroCopyCompleteMultipart;
        _networkHelper = networkHelper;
        _zeroCopyHelper = zeroCopyHelper;
        _logger = logger;
        _chunkedDataParser = chunkedDataParser;

        _networkHelper.EnsureDirectoryExists(_dataDirectory);
    }

    public async Task<StorageResult<PreparedData>> PrepareDataAsync(
        string bucketName,
        string key,
        PipeReader dataReader,
        IChunkSignatureValidator? chunkValidator,
        ChecksumRequest? checksumRequest,
        CancellationToken cancellationToken = default
    )
    {
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            throw new InvalidOperationException(
                $"Cannot store data with key '{key}' as it conflicts with temporary file pattern '{_tempFilePrefix}' or metadata directory '{_inlineMetadataDirectoryName}'");
        }

        var dataPath = GetDataPath(bucketName, key);
        var dataDir = Path.GetDirectoryName(dataPath)!;
        await _networkHelper.EnsureDirectoryExistsAsync(dataDir, $"StoreObject-{bucketName}/{key}");

        var tempPath = Path.Combine(dataDir, $"{_tempFilePrefix}{Guid.NewGuid():N}");

        StreamingChecksumCalculator? checksumCalculator = null;
        if (checksumRequest != null)
        {
            checksumCalculator = new StreamingChecksumCalculator(checksumRequest.Algorithm, checksumRequest.ProvidedChecksums);
        }

        try
        {
            long bytesWritten;

            if (chunkValidator != null)
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

                // AWS CLI v2 default checksum mode ships the integrity value (CRC64NVME etc.) in
                // a trailer. Pick the trailer-aware parser when the validator signals trailers, so
                // we actually capture parseResult.Trailers instead of silently ignoring them.
                var onDataWritten = checksumCalculator?.HasChecksums == true
                    ? (Action<ReadOnlySpan<byte>>)(data => checksumCalculator.Append(data))
                    : null;
                var parseResult = chunkValidator.ExpectsTrailers
                    ? await _chunkedDataParser.ParseChunkedDataWithTrailersToStreamAsync(dataReader, fileStream, chunkValidator, onDataWritten, cancellationToken)
                    : await _chunkedDataParser.ParseChunkedDataToStreamAsync(dataReader, fileStream, chunkValidator, onDataWritten, cancellationToken);

                if (!parseResult.Success)
                {
                    await fileStream.FlushAsync(cancellationToken);
                    fileStream.Close();
                    File.Delete(tempPath);
                    return StorageResult<PreparedData>.Error("SignatureDoesNotMatch", "Chunk signature validation failed");
                }

                // Feed trailer-delivered checksum values into the calculator so Finish() compares
                // client-provided vs server-computed below.
                if (checksumCalculator != null && parseResult.Trailers.Count > 0)
                {
                    TrailerChecksumMerger.MergeIntoCalculator(parseResult.Trailers, checksumCalculator);
                }

                bytesWritten = parseResult.TotalBytesWritten;
                await fileStream.FlushAsync(cancellationToken);
            }
            else
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

                bytesWritten = checksumCalculator?.HasChecksums == true
                    ? await ChecksumStreamHelper.WriteDataWithChecksumsAsync(dataReader, fileStream, checksumCalculator, cancellationToken)
                    : await PipeReaderHelper.CopyToAsync(dataReader, fileStream, false, cancellationToken);

                await fileStream.FlushAsync(cancellationToken);
            }

            var checksums = new Dictionary<string, string>();
            if (checksumCalculator?.HasChecksums == true)
            {
                var result = checksumCalculator.Finish();

                if (!result.IsValid)
                {
                    File.Delete(tempPath);
                    return StorageResult<PreparedData>.Error("InvalidChecksum", result.ErrorMessage ?? "Checksum validation failed");
                }

                checksums = result.CalculatedChecksums;
            }

            var etag = await ETagHelper.ComputeETagFromFileAsync(tempPath);

            var preparedData = new PreparedData
            {
                BucketName = bucketName,
                Key = key,
                Size = bytesWritten,
                ETag = etag,
                Checksums = checksums,
                Tag = tempPath
            };
            preparedData.SetDisposeAction(() =>
            {
                try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { /* ignore */ }
            });

            return StorageResult<PreparedData>.Success(preparedData);
        }
        catch
        {
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

    public async Task CommitPreparedDataAsync(PreparedData preparedData, CancellationToken cancellationToken = default)
    {
        var tempPath = preparedData.Tag
            ?? throw new InvalidOperationException($"PreparedData for {preparedData.BucketName}/{preparedData.Key} has no temp path");

        var dataPath = GetDataPath(preparedData.BucketName, preparedData.Key);
        await _networkHelper.AtomicMoveAsync(tempPath, dataPath, overwrite: true);
    }

    public Task AbortPreparedDataAsync(PreparedData preparedData, CancellationToken cancellationToken = default)
    {
        // Dispose will trigger the cleanup action which deletes the temp file
        preparedData.Dispose();
        return Task.CompletedTask;
    }

    public async Task<PreparedData> PrepareMultipartDataAsync(string bucketName, string key, IEnumerable<PipeReader> partReaders, CancellationToken cancellationToken = default)
    {
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            throw new InvalidOperationException(
                $"Cannot store data with key '{key}' as it conflicts with temporary file pattern '{_tempFilePrefix}' or metadata directory '{_inlineMetadataDirectoryName}'");
        }

        var dataPath = GetDataPath(bucketName, key);
        var dataDir = Path.GetDirectoryName(dataPath)!;
        await _networkHelper.EnsureDirectoryExistsAsync(dataDir, $"StoreMultipartObject-{bucketName}/{key}");

        var tempPath = Path.Combine(dataDir, $"{_tempFilePrefix}{Guid.NewGuid():N}");
        long totalBytesWritten = 0;

        try
        {
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

                foreach (var reader in partReaders)
                {
                    var bytesWritten = await PipeReaderHelper.CopyToAsync(reader, fileStream, true, cancellationToken);
                    totalBytesWritten += bytesWritten;
                }

                await fileStream.FlushAsync(cancellationToken);
            }

            // Intentionally no ETag compute here: the multipart ETag is the MD5-of-MD5-of-part-ETags
            // (AWS spec format "hash-N"), not MD5 of the assembled file. The facade computes that
            // separately via ETagHelper.ComputeMultipartETag from the part ETag list and overrides
            // this value, so re-reading the just-written tempfile here would be a wasted full-object
            // read (equal to S bytes per Complete).
            var preparedData = new PreparedData
            {
                BucketName = bucketName,
                Key = key,
                Size = totalBytesWritten,
                ETag = string.Empty,
                Checksums = new Dictionary<string, string>(),
                Tag = tempPath
            };
            preparedData.SetDisposeAction(() =>
            {
                try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { /* ignore */ }
            });

            return preparedData;
        }
        catch
        {
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

    public async Task<PreparedData?> PrepareMultipartDataFromFilesAsync(string bucketName, string key, IReadOnlyList<string> partPaths, CancellationToken cancellationToken = default)
    {
        // Opt-out escape hatch for deployments where kernel-side copy misbehaves.
        if (!_zeroCopyEnabled)
        {
            return null;
        }

        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            throw new InvalidOperationException(
                $"Cannot store data with key '{key}' as it conflicts with temporary file pattern '{_tempFilePrefix}' or metadata directory '{_inlineMetadataDirectoryName}'");
        }

        var dataPath = GetDataPath(bucketName, key);
        var dataDir = Path.GetDirectoryName(dataPath)!;
        await _networkHelper.EnsureDirectoryExistsAsync(dataDir, $"StoreMultipartObject-{bucketName}/{key}");

        var tempPath = Path.Combine(dataDir, $"{_tempFilePrefix}{Guid.NewGuid():N}");
        long totalBytesWritten = 0;

        try
        {
            using (var dstHandle = File.OpenHandle(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, FileOptions.Asynchronous))
            {
                foreach (var partPath in partPaths)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    totalBytesWritten += await AppendPartAsync(partPath, dstHandle, totalBytesWritten, cancellationToken);
                }
            }

            var preparedData = new PreparedData
            {
                BucketName = bucketName,
                Key = key,
                Size = totalBytesWritten,
                ETag = string.Empty,
                Checksums = new Dictionary<string, string>(),
                Tag = tempPath
            };
            preparedData.SetDisposeAction(() =>
            {
                try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { /* ignore */ }
            });

            return preparedData;
        }
        catch
        {
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

    /// <summary>
    /// Appends the entire contents of <paramref name="partPath"/> to <paramref name="dstHandle"/>
    /// at <paramref name="dstOffset"/>. Prefers Linux copy_file_range (server-side / reflink copy);
    /// on platforms or filesystems where that's unsupported, falls back to a userspace copy that
    /// still avoids the PipeReader overhead of the classic path.
    /// </summary>
    private async Task<long> AppendPartAsync(string partPath, SafeFileHandle dstHandle, long dstOffset, CancellationToken cancellationToken)
    {
        using var srcHandle = File.OpenHandle(partPath, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.Asynchronous);
        var length = RandomAccess.GetLength(srcHandle);
        if (length == 0)
        {
            return 0;
        }

        if (_zeroCopyHelper.IsSupported && _zeroCopyHelper.TryCopyFileRange(srcHandle, dstHandle, length, cancellationToken))
        {
            return length;
        }

        // Fallback: buffered userspace copy via RandomAccess at explicit offsets - no pipes, no
        // background tasks, same single-pass semantics as copy_file_range from the caller's view.
        var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(81920);
        try
        {
            long copied = 0;
            long srcOffset = 0;
            while (copied < length)
            {
                var read = await RandomAccess.ReadAsync(srcHandle, buffer.AsMemory(), srcOffset, cancellationToken);
                if (read == 0)
                {
                    throw new IOException($"Unexpected EOF reading part {partPath} at offset {srcOffset}");
                }
                await RandomAccess.WriteAsync(dstHandle, buffer.AsMemory(0, read), dstOffset + copied, cancellationToken);
                copied += read;
                srcOffset += read;
            }
            return copied;
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async Task<PreparedData?> PrepareCopyDataAsync(string sourceBucketName, string sourceKey, string destBucketName, string destKey, CancellationToken cancellationToken = default)
    {
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

        var sourceFileName = Path.GetFileName(sourcePath);
        if (FilesystemStorageHelper.IsTemporaryFile(sourceFileName, _tempFilePrefix))
        {
            return null;
        }

        var destPath = GetDataPath(destBucketName, destKey);
        var destDir = Path.GetDirectoryName(destPath)!;
        await _networkHelper.EnsureDirectoryExistsAsync(destDir, $"CopyObject-{destBucketName}/{destKey}");

        var tempPath = Path.Combine(destDir, $"{_tempFilePrefix}{Guid.NewGuid():N}");
        try
        {
            await _networkHelper.ExecuteWithRetryAsync(async () =>
                {
                    using var srcHandle = File.OpenHandle(sourcePath, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.Asynchronous);
                    using var dstHandle = File.OpenHandle(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, FileOptions.Asynchronous);
                    var length = RandomAccess.GetLength(srcHandle);

                    if (_zeroCopyHelper.IsSupported && _zeroCopyHelper.TryCopyFileRange(srcHandle, dstHandle, length, cancellationToken))
                    {
                        return true;
                    }

                    var buffer = ArrayPool<byte>.Shared.Rent(81920);
                    try
                    {
                        long copied = 0;
                        while (copied < length)
                        {
                            var read = await RandomAccess.ReadAsync(srcHandle, buffer.AsMemory(), copied, cancellationToken);
                            if (read == 0)
                                throw new IOException($"Unexpected EOF reading {sourcePath} at offset {copied}");
                            await RandomAccess.WriteAsync(dstHandle, buffer.AsMemory(0, read), copied, cancellationToken);
                            copied += read;
                        }
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                    }
                    return true;
                },
                "CopyFile");

            var fileInfo = new FileInfo(tempPath);
            var etag = await ETagHelper.ComputeETagFromFileAsync(tempPath);

            var preparedData = new PreparedData
            {
                BucketName = destBucketName,
                Key = destKey,
                Size = fileInfo.Length,
                ETag = etag,
                Checksums = new Dictionary<string, string>(),
                Tag = tempPath
            };
            preparedData.SetDisposeAction(() =>
            {
                try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { /* ignore */ }
            });

            return preparedData;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error copying data from {SourceBucket}/{SourceKey} to {DestBucket}/{DestKey}",
                sourceBucketName, sourceKey, destBucketName, destKey);

            if (File.Exists(tempPath))
            {
                try { File.Delete(tempPath); } catch { /* ignore */ }
            }

            return null;
        }
    }

    public async Task<bool> WriteDataToPipeAsync(string bucketName, string key, PipeWriter writer, long? byteRangeStart = null, long? byteRangeEnd = null, CancellationToken cancellationToken = default)
    {
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return false;
        }

        var dataPath = GetDataPath(bucketName, key);

        if (!File.Exists(dataPath))
        {
            return false;
        }

        var fileName = Path.GetFileName(dataPath);
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
        {
            return false;
        }

        await using var fileStream = File.OpenRead(dataPath);

        long startPosition = byteRangeStart ?? 0;
        long endPosition = byteRangeEnd ?? (fileStream.Length - 1);
        long bytesToRead = endPosition - startPosition + 1;

        if (startPosition < 0 || endPosition >= fileStream.Length || startPosition > endPosition)
        {
            _logger.LogWarning("Invalid byte range requested: {Start}-{End} for file size {Size}", startPosition, endPosition, fileStream.Length);
            return false;
        }

        if (startPosition > 0)
        {
            fileStream.Seek(startPosition, SeekOrigin.Begin);
        }

        const int bufferSize = 4096;
        var buffer = new byte[bufferSize];
        long totalBytesRead = 0;

        while (totalBytesRead < bytesToRead)
        {
            var remainingBytes = bytesToRead - totalBytesRead;
            var bytesToReadNow = (int)Math.Min(bufferSize, remainingBytes);

            var bytesRead = await fileStream.ReadAsync(buffer.AsMemory(0, bytesToReadNow), cancellationToken);
            if (bytesRead == 0)
            {
                break;
            }

            var memory = writer.GetMemory(bytesRead);
            buffer.AsMemory(0, bytesRead).CopyTo(memory);
            writer.Advance(bytesRead);
            await writer.FlushAsync(cancellationToken);

            totalBytesRead += bytesRead;
        }

        await writer.CompleteAsync();
        return true;
    }

    public async Task<bool> DeleteDataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return false;
        }

        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return false;
        }

        var fileName = Path.GetFileName(dataPath);
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
        {
            return false;
        }

        await _networkHelper.ExecuteWithRetryAsync(() =>
            {
                File.Delete(dataPath);
                return Task.FromResult(true);
            },
            "DeleteFile");

        try
        {
            var bucketDirectory = Path.Combine(_dataDirectory, bucketName);
            var directory = Path.GetDirectoryName(dataPath);

            if (!string.IsNullOrEmpty(directory) &&
                directory.StartsWith(_dataDirectory) &&
                directory != _dataDirectory &&
                directory != bucketDirectory)
            {
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
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return Task.FromResult(false);
        }

        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return Task.FromResult(false);
        }

        var fileName = Path.GetFileName(dataPath);
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
        {
            return Task.FromResult(false);
        }

        return Task.FromResult(true);
    }

    public Task<(long size, DateTime lastModified)?> GetDataInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return Task.FromResult<(long size, DateTime lastModified)?>(null);
        }

        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return Task.FromResult<(long size, DateTime lastModified)?>(null);
        }

        var fileName = Path.GetFileName(dataPath);
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
        {
            return Task.FromResult<(long size, DateTime lastModified)?>(null);
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

    public Task<string?> ComputeETagAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return Task.FromResult<string?>(null);
        }

        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return Task.FromResult<string?>(null);
        }

        var fileName = Path.GetFileName(dataPath);
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
        {
            return Task.FromResult<string?>(null);
        }

        return ETagHelper.ComputeETagFromFileAsync(dataPath)!;
    }

    public async Task<Dictionary<string, string>> ComputeChecksumsAsync(
        string bucketName,
        string key,
        IEnumerable<string> algorithms,
        CancellationToken cancellationToken = default)
    {
        var algorithmList = algorithms as List<string> ?? algorithms.ToList();
        if (algorithmList.Count == 0)
        {
            return new Dictionary<string, string>();
        }

        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return new Dictionary<string, string>();
        }

        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return new Dictionary<string, string>();
        }

        var fileName = Path.GetFileName(dataPath);
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
        {
            return new Dictionary<string, string>();
        }

        return await ChecksumHelper.ComputeSelectiveChecksumsFromFileAsync(dataPath, algorithmList, cancellationToken);
    }

    public async Task<(string? etag, Dictionary<string, string> checksums)> ComputeETagAndChecksumsAsync(
        string bucketName,
        string key,
        IEnumerable<string> checksumAlgorithms,
        CancellationToken cancellationToken = default)
    {
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
            return (null, new Dictionary<string, string>());

        var dataPath = GetDataPath(bucketName, key);
        var fileName = Path.GetFileName(dataPath);
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
            return (null, new Dictionary<string, string>());

        var (etag, checksums) = await ChecksumHelper.ComputeETagAndChecksumsFromFileAsync(dataPath, checksumAlgorithms, cancellationToken);
        return (etag, checksums);
    }

    private (bool IsValid, string Path) ValidateAndPreparePath(string bucketName, string? prefix)
    {
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
        if (FilesystemStorageHelper.IsTemporaryFile(fileName, _tempFilePrefix))
        {
            return true;
        }

        if (FilesystemStorageHelper.IsMetadataPath(entryName, _metadataMode, _inlineMetadataDirectoryName))
        {
            return true;
        }

        return false;
    }

    private string GetDataPath(string bucketName, string key)
    {
        return Path.Combine(_dataDirectory, bucketName, key);
    }
}
