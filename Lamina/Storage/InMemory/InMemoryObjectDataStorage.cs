using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using Lamina.Helpers;
using Lamina.Services;
using Lamina.Storage.Abstract;
using Microsoft.Extensions.Logging;

namespace Lamina.Storage.InMemory;

public class InMemoryObjectDataStorage : IObjectDataStorage
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte[]>> _data = new();
    private readonly ILogger<InMemoryObjectDataStorage> _logger;

    public InMemoryObjectDataStorage(ILogger<InMemoryObjectDataStorage> logger)
    {
        _logger = logger;
    }

    public async Task<(long size, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        var bucketData = _data.GetOrAdd(bucketName, _ => new ConcurrentDictionary<string, byte[]>());

        var dataSegments = new List<byte[]>();
        long totalSize = 0;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await dataReader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (buffer.Length > 0)
                {
                    var data = buffer.ToArray();
                    dataSegments.Add(data);
                    totalSize += data.Length;
                }

                dataReader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        finally
        {
            await dataReader.CompleteAsync();
        }

        var combinedData = new byte[totalSize];
        var offset = 0;
        foreach (var segment in dataSegments)
        {
            Buffer.BlockCopy(segment, 0, combinedData, offset, segment.Length);
            offset += segment.Length;
        }

        var etag = ETagHelper.ComputeETag(combinedData);
        bucketData[key] = combinedData;

        return (totalSize, etag);
    }

    public async Task<(long size, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, CancellationToken cancellationToken = default)
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
            await foreach (var chunk in AwsChunkedEncodingHelper.ParseChunkedDataAsync(dataReader, chunkValidator, _logger, cancellationToken))
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
        long totalSize = 0;

        // Read all parts
        foreach (var reader in partReaders)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = await reader.ReadAsync(cancellationToken);
                    var buffer = result.Buffer;

                    if (buffer.Length > 0)
                    {
                        var data = buffer.ToArray();
                        allSegments.Add(data);
                        totalSize += data.Length;
                    }

                    reader.AdvanceTo(buffer.End);

                    if (result.IsCompleted)
                    {
                        break;
                    }
                }
            }
            finally
            {
                await reader.CompleteAsync();
            }
        }

        // Combine all segments into one array
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

    public Task<byte[]?> GetDataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (_data.TryGetValue(bucketName, out var bucketData) &&
            bucketData.TryGetValue(key, out var data))
        {
            return Task.FromResult<byte[]?>(data);
        }
        return Task.FromResult<byte[]?>(null);
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

    public Task<IEnumerable<string>> ListDataKeysAsync(string bucketName, string? prefix = null, CancellationToken cancellationToken = default)
    {
        if (!_data.TryGetValue(bucketName, out var bucketData))
        {
            return Task.FromResult(Enumerable.Empty<string>());
        }

        var keys = bucketData.Keys.AsEnumerable();

        if (!string.IsNullOrEmpty(prefix))
        {
            keys = keys.Where(k => k.StartsWith(prefix));
        }

        return Task.FromResult(keys);
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