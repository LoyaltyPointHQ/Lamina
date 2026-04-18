using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.InMemory;

public class InMemoryObjectDataStorage : IObjectDataStorage
{
    private record StoredObject(byte[] Data, DateTime LastModified);

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, StoredObject>> _data = new();
    private readonly ConcurrentDictionary<string, byte[]> _pendingData = new();
    private readonly IChunkedDataParser _chunkedDataParser;

    public InMemoryObjectDataStorage(IChunkedDataParser chunkedDataParser)
    {
        _chunkedDataParser = chunkedDataParser;
    }

    public async Task<StorageResult<PreparedData>> PrepareDataAsync(
        string bucketName,
        string key,
        PipeReader dataReader,
        IChunkSignatureValidator? chunkValidator,
        ChecksumRequest? checksumRequest,
        CancellationToken cancellationToken = default)
    {
        byte[] combinedData;
        long bytesWritten;
        ChunkedDataResult? parseResult = null;

        if (chunkValidator != null)
        {
            using var memoryStream = new MemoryStream();
            // Use trailer-aware parser when the client signalled chunked trailers (AWS CLI v2
            // default checksum flow) so parseResult.Trailers carries through.
            parseResult = chunkValidator.ExpectsTrailers
                ? await _chunkedDataParser.ParseChunkedDataWithTrailersToStreamAsync(dataReader, memoryStream, chunkValidator, null, cancellationToken)
                : await _chunkedDataParser.ParseChunkedDataToStreamAsync(dataReader, memoryStream, chunkValidator, null, cancellationToken);

            if (!parseResult.Success)
            {
                return StorageResult<PreparedData>.Error("SignatureDoesNotMatch", "Chunk signature validation failed");
            }

            combinedData = memoryStream.ToArray();
            bytesWritten = parseResult.TotalBytesWritten;
        }
        else
        {
            combinedData = await PipeReaderHelper.ReadAllBytesAsync(dataReader, false, cancellationToken);
            bytesWritten = combinedData.Length;
        }

        var etag = ETagHelper.ComputeETag(combinedData);

        var checksums = new Dictionary<string, string>();
        if (checksumRequest != null)
        {
            using var calculator = new StreamingChecksumCalculator(checksumRequest.Algorithm, checksumRequest.ProvidedChecksums);
            if (calculator.HasChecksums)
            {
                calculator.Append(combinedData);
                // Merge client-delivered trailer checksums so Finish can validate them.
                if (parseResult?.Trailers.Count > 0)
                {
                    TrailerChecksumMerger.MergeIntoCalculator(parseResult.Trailers, calculator);
                }
                var result = calculator.Finish();

                if (!result.IsValid)
                {
                    return StorageResult<PreparedData>.Error("InvalidChecksum", result.ErrorMessage ?? "Checksum validation failed");
                }

                checksums = result.CalculatedChecksums;
            }
        }

        // Store in pending — not yet visible
        var pendingKey = GetPendingKey(bucketName, key);
        _pendingData[pendingKey] = combinedData;

        var preparedData = new PreparedData
        {
            BucketName = bucketName,
            Key = key,
            Size = bytesWritten,
            ETag = etag,
            Checksums = checksums
        };
        preparedData.SetDisposeAction(() => _pendingData.TryRemove(pendingKey, out _));

        return StorageResult<PreparedData>.Success(preparedData);
    }

    public Task CommitPreparedDataAsync(PreparedData preparedData, CancellationToken cancellationToken = default)
    {
        var pendingKey = GetPendingKey(preparedData.BucketName, preparedData.Key);
        if (!_pendingData.TryRemove(pendingKey, out var data))
        {
            throw new InvalidOperationException($"No pending data found for {preparedData.BucketName}/{preparedData.Key}");
        }

        var bucketData = _data.GetOrAdd(preparedData.BucketName, _ => new ConcurrentDictionary<string, StoredObject>());
        bucketData[preparedData.Key] = new StoredObject(data, DateTime.UtcNow);

        return Task.CompletedTask;
    }

    public Task AbortPreparedDataAsync(PreparedData preparedData, CancellationToken cancellationToken = default)
    {
        var pendingKey = GetPendingKey(preparedData.BucketName, preparedData.Key);
        _pendingData.TryRemove(pendingKey, out _);
        return Task.CompletedTask;
    }

    public async Task<PreparedData> PrepareMultipartDataAsync(string bucketName, string key, IEnumerable<PipeReader> partReaders, CancellationToken cancellationToken = default)
    {
        var allSegments = new List<byte[]>();

        foreach (var reader in partReaders)
        {
            var partData = await PipeReaderHelper.ReadAllBytesAsync(reader, true, cancellationToken);
            allSegments.Add(partData);
        }

        var totalSize = allSegments.Sum(s => s.Length);
        var combinedData = new byte[totalSize];
        var offset = 0;
        foreach (var segment in allSegments)
        {
            Buffer.BlockCopy(segment, 0, combinedData, offset, segment.Length);
            offset += segment.Length;
        }

        var etag = ETagHelper.ComputeETag(combinedData);

        var pendingKey = GetPendingKey(bucketName, key);
        _pendingData[pendingKey] = combinedData;

        var preparedData = new PreparedData
        {
            BucketName = bucketName,
            Key = key,
            Size = totalSize,
            ETag = etag,
            Checksums = new Dictionary<string, string>()
        };
        preparedData.SetDisposeAction(() => _pendingData.TryRemove(pendingKey, out _));

        return preparedData;
    }

    public Task<PreparedData?> PrepareCopyDataAsync(string sourceBucketName, string sourceKey, string destBucketName, string destKey, CancellationToken cancellationToken = default)
    {
        if (!_data.TryGetValue(sourceBucketName, out var sourceBucketData) ||
            !sourceBucketData.TryGetValue(sourceKey, out var sourceStored))
        {
            return Task.FromResult<PreparedData?>(null);
        }

        var copiedData = new byte[sourceStored.Data.Length];
        Buffer.BlockCopy(sourceStored.Data, 0, copiedData, 0, sourceStored.Data.Length);

        var etag = ETagHelper.ComputeETag(copiedData);

        var pendingKey = GetPendingKey(destBucketName, destKey);
        _pendingData[pendingKey] = copiedData;

        var preparedData = new PreparedData
        {
            BucketName = destBucketName,
            Key = destKey,
            Size = copiedData.Length,
            ETag = etag,
            Checksums = new Dictionary<string, string>()
        };
        preparedData.SetDisposeAction(() => _pendingData.TryRemove(pendingKey, out _));

        return Task.FromResult<PreparedData?>(preparedData);
    }

    public async Task<bool> WriteDataToPipeAsync(string bucketName, string key, PipeWriter writer, long? byteRangeStart = null, long? byteRangeEnd = null, CancellationToken cancellationToken = default)
    {
        if (!_data.TryGetValue(bucketName, out var bucketData) ||
            !bucketData.TryGetValue(key, out var stored))
        {
            return false;
        }

        var data = stored.Data;
        long startPosition = byteRangeStart ?? 0;
        long endPosition = byteRangeEnd ?? (data.Length - 1);

        if (startPosition < 0 || endPosition >= data.Length || startPosition > endPosition)
        {
            return false;
        }

        int length = (int)(endPosition - startPosition + 1);
        int offset = (int)startPosition;

        await writer.WriteAsync(new ReadOnlyMemory<byte>(data, offset, length), cancellationToken);
        await writer.CompleteAsync();
        return true;
    }

    public Task<bool> DeleteDataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (_data.TryGetValue(bucketName, out var bucketData))
        {
            return Task.FromResult(bucketData.TryRemove(key, out _));
        }

        return Task.FromResult(false);
    }

    public Task<bool> DataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_data.TryGetValue(bucketName, out var bucketData) && bucketData.ContainsKey(key));
    }

    public Task<(long size, DateTime lastModified)?> GetDataInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (_data.TryGetValue(bucketName, out var bucketData) &&
            bucketData.TryGetValue(key, out var stored))
        {
            return Task.FromResult<(long size, DateTime lastModified)?>((stored.Data.Length, stored.LastModified));
        }

        return Task.FromResult<(long size, DateTime lastModified)?>(null);
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
        var result = new ListDataResult();

        if (!_data.TryGetValue(bucketName, out var bucketData))
        {
            return Task.FromResult(result);
        }

        IEnumerable<string> keys = bucketType == BucketType.GeneralPurpose
            ? bucketData.Keys.OrderBy(k => k, StringComparer.Ordinal)
            : bucketData.Keys;

        if (!string.IsNullOrEmpty(prefix))
        {
            keys = keys.Where(k => k.StartsWith(prefix));
        }

        if (!string.IsNullOrEmpty(startAfter))
        {
            keys = keys.Where(k => string.Compare(k, startAfter, StringComparison.Ordinal) > 0);
        }

        if (string.IsNullOrEmpty(delimiter))
        {
            keys = keys.Take(maxKeys);
            result.Keys.AddRange(keys);
            return Task.FromResult(result);
        }

        var prefixLength = prefix?.Length ?? 0;
        var commonPrefixSet = new HashSet<string>();
        var resultKeys = new List<string>();
        var totalItems = 0;

        foreach (var key in keys)
        {
            if (totalItems >= maxKeys)
            {
                break;
            }

            var remainingKey = key.Substring(prefixLength);
            var delimiterIndex = remainingKey.IndexOf(delimiter, StringComparison.Ordinal);

            if (delimiterIndex >= 0)
            {
                var commonPrefix = key.Substring(0, prefixLength + delimiterIndex + delimiter.Length);
                if (commonPrefixSet.Add(commonPrefix))
                {
                    totalItems++;
                }
            }
            else
            {
                resultKeys.Add(key);
                totalItems++;
            }
        }

        result.Keys.AddRange(resultKeys);
        result.CommonPrefixes.AddRange(commonPrefixSet.OrderBy(p => p, StringComparer.Ordinal));
        result.Keys.Sort();

        return Task.FromResult(result);
    }

    public Task<string?> ComputeETagAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (_data.TryGetValue(bucketName, out var bucketData) &&
            bucketData.TryGetValue(key, out var stored))
        {
            var etag = ETagHelper.ComputeETag(stored.Data);
            return Task.FromResult<string?>(etag);
        }

        return Task.FromResult<string?>(null);
    }

    private static string GetPendingKey(string bucketName, string key) => $"{bucketName}/{key}";
}
