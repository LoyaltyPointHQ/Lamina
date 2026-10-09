using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.Core.Listing;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Win32.SafeHandles;

namespace Lamina.Storage.Filesystem;

public class FilesystemObjectDataStorage : IObjectDataStorage, IFileBackedObjectDataStorage, IObjectListingFilter, IBatchObjectDataInfoStorage
{
    private readonly string _dataDirectory;
    private readonly FilesystemListingIndex? _listingIndex;
    private readonly FilesystemListingReadLimiter _listingReads;
    private readonly MetadataStorageMode _metadataMode;
    private readonly string _inlineMetadataDirectoryName;
    private readonly string _tempFilePrefix;
    private readonly bool _zeroCopyEnabled;
    private readonly NetworkFileSystemHelper _networkHelper;
    private readonly LinuxZeroCopyHelper _zeroCopyHelper;
    private readonly ILogger<FilesystemObjectDataStorage> _logger;

    public FilesystemObjectDataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        NetworkFileSystemHelper networkHelper,
        LinuxZeroCopyHelper zeroCopyHelper,
        ILogger<FilesystemObjectDataStorage> logger,
        FilesystemListingIndex? listingIndex = null,
        FilesystemListingReadLimiter? listingReads = null
    )
    {
        var settings = settingsOptions.Value;
        _listingIndex = listingIndex;
        _listingReads = listingReads ?? FilesystemListingReadLimiter.Shared;
        _dataDirectory = settings.DataDirectory;
        _metadataMode = settings.MetadataMode;
        _inlineMetadataDirectoryName = settings.InlineMetadataDirectoryName;
        _tempFilePrefix = settings.TempFilePrefix;
        _zeroCopyEnabled = settings.UseZeroCopyCompleteMultipart;
        _networkHelper = networkHelper;
        _zeroCopyHelper = zeroCopyHelper;
        _logger = logger;

        _networkHelper.EnsureDirectoryExists(_dataDirectory);
    }

    public async Task<StagedDataWrite> BeginWriteAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
            throw new InvalidOperationException($"Cannot store data with key '{key}' as it conflicts with temporary file pattern '{_tempFilePrefix}' or metadata directory '{_inlineMetadataDirectoryName}'");
        var directory = Path.GetDirectoryName(GetDataPath(bucketName, key))!;
        await _networkHelper.EnsureDirectoryExistsAsync(directory, $"StoreObject-{bucketName}/{key}");
        var path = Path.Combine(directory, $"{_tempFilePrefix}{Guid.NewGuid():N}");
        var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, true);
        void Cleanup() => File.Delete(path);
        return new StagedDataWrite(stream, size =>
        {
            var prepared = new PreparedData { BucketName = bucketName, Key = key, Size = size, Tag = path };
            prepared.SetDisposeAction(Cleanup);
            return prepared;
        }, Cleanup);
    }

    public Task<Stream?> OpenReadAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
            return Task.FromResult<Stream?>(null);
        try { return Task.FromResult<Stream?>(new FileStream(GetDataPath(bucketName, key), FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, 81920, true)); }
        catch (FileNotFoundException) { return Task.FromResult<Stream?>(null); }
        catch (DirectoryNotFoundException) { return Task.FromResult<Stream?>(null); }
    }

    public Task<Stream> OpenPreparedReadAsync(PreparedData preparedData, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult<Stream>(new FileStream(preparedData.Tag!, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, true));
    }

    public async Task CommitPreparedDataAsync(PreparedData preparedData, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var tempPath = preparedData.Tag
            ?? throw new InvalidOperationException($"PreparedData for {preparedData.BucketName}/{preparedData.Key} has no temp path");

        var dataPath = GetDataPath(preparedData.BucketName, preparedData.Key);
        var aliasesDirectory = HasIndexedDirectoryAlias(dataPath);
        await _networkHelper.AtomicMoveAsync(tempPath, dataPath, overwrite: true);
        NotifyListingChange(preparedData.BucketName, preparedData.Key, aliasesDirectory);
    }

    public Task AbortPreparedDataAsync(PreparedData preparedData, CancellationToken cancellationToken = default)
    {
        // Dispose will trigger the cleanup action which deletes the temp file
        preparedData.Dispose();
        return Task.CompletedTask;
    }

    public async Task<PreparedData> PrepareMultipartDataAsync(string bucketName, string key, IEnumerable<PipeReader> partReaders, CancellationToken cancellationToken = default)
    {
        var readers = partReaders.ToList();
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
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

                    foreach (var reader in readers)
                    {
                        var bytesWritten = await PipeReaderHelper.CopyToAsync(reader, fileStream, true, cancellationToken);
                        totalBytesWritten += bytesWritten;
                    }

                    await fileStream.FlushAsync(cancellationToken);
                }

                var preparedData = new PreparedData
                {
                    BucketName = bucketName,
                    Key = key,
                    Size = totalBytesWritten,
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
        finally
        {
            await Task.WhenAll(readers.Select(reader => reader.CompleteAsync().AsTask()));
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
        cancellationToken.ThrowIfCancellationRequested();
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

            var preparedData = new PreparedData
            {
                BucketName = destBucketName,
                Key = destKey,
                Size = fileInfo.Length,
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

            if (ex is OperationCanceledException) throw;
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
        if (fileStream.Length == 0 && byteRangeStart is null && byteRangeEnd is null)
        {
            await writer.CompleteAsync();
            return true;
        }

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

        var aliasesDirectory = HasIndexedDirectoryAlias(dataPath);
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

        NotifyListingChange(bucketName, key, aliasesDirectory);
        return true;
    }

    public Task<bool> DataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            _logger.LogDebug("DataExistsAsync: key={Key} forbidden", key);
            return Task.FromResult(false);
        }

        var dataPath = GetDataPath(bucketName, key);
        _logger.LogDebug("DataExistsAsync: bucket={Bucket} key={Key} path={Path} exists={Exists}",
            bucketName, key, dataPath, File.Exists(dataPath));
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
        cancellationToken.ThrowIfCancellationRequested();
        if (FilesystemStorageHelper.IsKeyForbidden(key, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
        {
            return Task.FromResult<(long size, DateTime lastModified)?>(null);
        }

        var fileInfo = new FileInfo(GetDataPath(bucketName, key));
        if (!fileInfo.Exists)
            return Task.FromResult<(long size, DateTime lastModified)?>(null);
        return Task.FromResult<(long size, DateTime lastModified)?>((fileInfo.Length, fileInfo.LastWriteTimeUtc));
    }


    public async Task<IReadOnlyDictionary<string, (long size, DateTime lastModified)?>> GetDataInfoBatchAsync(
        string bucketName, IReadOnlyList<string> keys, CancellationToken cancellationToken = default) =>
        await _listingReads.ReadBatchAsync(keys, (key, ct) => GetDataInfoAsync(bucketName, key, ct), cancellationToken);

    public bool IsVisibleInListing(string key) =>
        !FilesystemStorageHelper.HasInternalListingSegment(key, _inlineMetadataDirectoryName, _tempFilePrefix);

    public async Task<ListDataResult> ListDataKeysAsync(
        string bucketName, BucketType bucketType, string? prefix = null, string? delimiter = null,
        string? startAfter = null, int maxKeys = 1000, CancellationToken cancellationToken = default)
    {
        var order = bucketType == BucketType.Directory ? ListingOrder.DirectoryHashV1 : ListingOrder.Lexicographical;
        var query = new ListingQuery(bucketType, prefix, delimiter,
            string.IsNullOrEmpty(startAfter) ? null : new ListingPosition(order, startAfter), maxKeys);
        var candidates = await ListDataCandidatesAsync(bucketName, query, cancellationToken);
        return candidates.ToDataPage(query.MaxKeys);
    }

    public Task<ListingCandidates> ListDataCandidatesAsync(string bucketName, ListingQuery query,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        (ListingCandidates Page, bool HasSymlinks) Scan(ListingEntryObserver? observer, CancellationToken token)
        {
            var started = Stopwatch.GetTimestamp();
            var selector = new ListingPageSelector(query, observer: observer);
            var symlinks = false;
            if (!FilesystemStorageHelper.IsKeyForbidden(bucketName, _tempFilePrefix, _metadataMode, _inlineMetadataDirectoryName))
            {
                using var walker = new FilesystemListingWalker(Path.Combine(_dataDirectory, bucketName),
                    _inlineMetadataDirectoryName, _tempFilePrefix, query, selector, token, detectSymlinks: observer != null);
                walker.Walk();
                symlinks = walker.HasSymlinks;
            }
            FilesystemListingIndex.RecordScan(selector.Statistics, started);
            return (selector.Finish(), symlinks);
        }
        if (_listingIndex is not { Enabled: true })
            return Task.FromResult(Scan(null, cancellationToken).Page);
        return _listingIndex.GetAsync(Path.Combine(_dataDirectory, bucketName), query, Scan,
            entry => entry.IsCommonPrefix ? Directory.Exists(GetDataPath(bucketName, entry.Name))
                : File.Exists(GetDataPath(bucketName, entry.Name)), cancellationToken);
    }

    private bool HasIndexedDirectoryAlias(string dataPath)
    {
        if (_listingIndex is not { Enabled: true }) return false;
        // Check before deleting: empty-directory cleanup may remove the alias itself.
        // Links can target another bucket, so their mutations invalidate the shared index.
        for (var directory = new DirectoryInfo(Path.GetDirectoryName(dataPath)!); directory != null; directory = directory.Parent)
            if (directory.LinkTarget != null) return true;
        return false;
    }

    private void NotifyListingChange(string bucketName, string key, bool aliasesDirectory)
    {
        if (aliasesDirectory) _listingIndex?.InvalidateAll();
        // Hidden writes can still create visible parent directories/CommonPrefixes.
        // Rebuild instead of either publishing a hidden name or ignoring the mutation.
        else if (!IsVisibleInListing(key)) _listingIndex?.InvalidateBucket(Path.Combine(_dataDirectory, bucketName));
        else _listingIndex?.Changed(Path.Combine(_dataDirectory, bucketName), key);
    }

    private string GetDataPath(string bucketName, string key)
    {
        return Path.Combine(_dataDirectory, bucketName, key);
    }
}
