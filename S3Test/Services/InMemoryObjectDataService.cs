using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using S3Test.Helpers;

namespace S3Test.Services;

public class InMemoryObjectDataService : IObjectDataService
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte[]>> _data = new();

    public async Task<(byte[] data, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, CancellationToken cancellationToken = default)
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

        return (combinedData, etag);
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
        return Task.FromResult(
            _data.TryGetValue(bucketName, out var bucketData) &&
            bucketData.ContainsKey(key));
    }

    public Task<long?> GetDataSizeAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (_data.TryGetValue(bucketName, out var bucketData) &&
            bucketData.TryGetValue(key, out var data))
        {
            return Task.FromResult<long?>(data.Length);
        }
        return Task.FromResult<long?>(null);
    }

}