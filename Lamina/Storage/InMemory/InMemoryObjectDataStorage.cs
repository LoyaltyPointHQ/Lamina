using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using Lamina.Helpers;
using Lamina.Models;
using Lamina.Streaming.Chunked;
using Lamina.Streaming.Validation;
using Lamina.Storage.Abstract;

namespace Lamina.Storage.InMemory;

public class InMemoryObjectDataStorage : IObjectDataStorage
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte[]>> _data = new();
    private readonly IChunkedDataParser _chunkedDataParser;

    public InMemoryObjectDataStorage(IChunkedDataParser chunkedDataParser)
    {
        _chunkedDataParser = chunkedDataParser;
    }

    public async Task<(long size, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        var bucketData = _data.GetOrAdd(bucketName, _ => new ConcurrentDictionary<string, byte[]>());

        var combinedData = await PipeReaderHelper.ReadAllBytesAsync(dataReader, false, cancellationToken);

        var etag = ETagHelper.ComputeETag(combinedData);
        bucketData[key] = combinedData;

        return (combinedData.Length, etag);
    }

    public async Task<(long size, string etag)> StoreDataAsync(
        string bucketName,
        string key,
        PipeReader dataReader,
        IChunkSignatureValidator? chunkValidator,
        CancellationToken cancellationToken = default
    )
    {
        // If no chunk validator is provided, use the standard method
        if (chunkValidator == null)
        {
            return await StoreDataAsync(bucketName, key, dataReader, cancellationToken);
        }

        var bucketData = _data.GetOrAdd(bucketName, _ => new ConcurrentDictionary<string, byte[]>());

        // Parse AWS chunked encoding and collect decoded data with signature validation
        var decodedData = new List<byte[]>();
        long totalDecodedSize = 0;

        try
        {
            await foreach (var chunk in _chunkedDataParser.ParseChunkedDataAsync(dataReader, chunkValidator, cancellationToken))
            {
                var chunkArray = chunk.ToArray();
                decodedData.Add(chunkArray);
                totalDecodedSize += chunkArray.Length;
            }
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("Invalid") && ex.Message.Contains("signature"))
        {
            // Invalid chunk signature - return null to indicate failure
            return (0, string.Empty);
        }

        // Combine all decoded chunks
        var combinedData = new byte[totalDecodedSize];
        var offset = 0;
        foreach (var chunk in decodedData)
        {
            Buffer.BlockCopy(chunk, 0, combinedData, offset, chunk.Length);
            offset += chunk.Length;
        }

        var etag = ETagHelper.ComputeETag(combinedData);
        bucketData[key] = combinedData;

        return (totalDecodedSize, etag);
    }

    public async Task<(long size, string etag)> StoreMultipartDataAsync(string bucketName, string key, IEnumerable<PipeReader> partReaders, CancellationToken cancellationToken = default)
    {
        var bucketData = _data.GetOrAdd(bucketName, _ => new ConcurrentDictionary<string, byte[]>());

        var allSegments = new List<byte[]>();

        // Read all parts
        foreach (var reader in partReaders)
        {
            var partData = await PipeReaderHelper.ReadAllBytesAsync(reader, true, cancellationToken);
            allSegments.Add(partData);
        }

        // Combine all segments into one array
        var totalSize = allSegments.Sum(s => s.Length);
        var combinedData = new byte[totalSize];
        var offset = 0;
        foreach (var segment in allSegments)
        {
            Buffer.BlockCopy(segment, 0, combinedData, offset, segment.Length);
            offset += segment.Length;
        }

        var etag = ETagHelper.ComputeETag(combinedData);
        bucketData[key] = combinedData;

        return (totalSize, etag);
    }

    public async Task<bool> WriteDataToPipeAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default)
    {
        if (!_data.TryGetValue(bucketName, out var bucketData) ||
            !bucketData.TryGetValue(key, out var data))
        {
            return false;
        }

        await writer.WriteAsync(data, cancellationToken);
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
            bucketData.TryGetValue(key, out var data))
        {
            return Task.FromResult<(long size, DateTime lastModified)?>((data.Length, DateTime.UtcNow));
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

        // Sort keys lexicographically for General Purpose buckets only
        // Directory buckets should maintain insertion/enumeration order (non-lexicographical)
        IEnumerable<string> keys = bucketType == BucketType.GeneralPurpose
            ? bucketData.Keys.OrderBy(k => k, StringComparer.Ordinal)
            : bucketData.Keys;

        // Apply prefix filter
        if (!string.IsNullOrEmpty(prefix))
        {
            keys = keys.Where(k => k.StartsWith(prefix));
        }

        // Apply startAfter filter (marker/continuation token)
        if (!string.IsNullOrEmpty(startAfter))
        {
            keys = keys.Where(k => string.Compare(k, startAfter, StringComparison.Ordinal) > 0);
        }

        // If no delimiter, return keys with pagination
        if (string.IsNullOrEmpty(delimiter))
        {
            keys = keys.Take(maxKeys);
            result.Keys.AddRange(keys);
            return Task.FromResult(result);
        }

        // Process keys with delimiter and pagination
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

            // Get the part after the prefix
            var remainingKey = key.Substring(prefixLength);

            // Look for delimiter in the remaining key
            var delimiterIndex = remainingKey.IndexOf(delimiter, StringComparison.Ordinal);

            if (delimiterIndex >= 0)
            {
                // Found delimiter - this represents a "directory"
                var commonPrefix = key.Substring(0, prefixLength + delimiterIndex + delimiter.Length);
                if (commonPrefixSet.Add(commonPrefix))
                {
                    totalItems++;
                }
            }
            else
            {
                // No delimiter found - this is a direct key at this level
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
            bucketData.TryGetValue(key, out var data))
        {
            // Use ETagHelper to compute hash
            var etag = ETagHelper.ComputeETag(data);
            return Task.FromResult<string?>(etag);
        }

        return Task.FromResult<string?>(null);
    }
}