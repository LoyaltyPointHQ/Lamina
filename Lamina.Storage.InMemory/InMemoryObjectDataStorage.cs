using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.Core.Listing;
using Microsoft.Extensions.Logging;

namespace Lamina.Storage.InMemory;

public class InMemoryObjectDataStorage : IObjectDataStorage
{
    private record StoredObject(byte[] Data, DateTime LastModified);

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, StoredObject>> _data = new();
    private readonly ConcurrentDictionary<string, byte[]> _pendingData = new();
    public InMemoryObjectDataStorage(ILogger<InMemoryObjectDataStorage> logger) { }

    public Task<StagedDataWrite> BeginWriteAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var stream = new MemoryStream();
        var identity = Guid.NewGuid().ToString("N");
        void Cleanup() => _pendingData.TryRemove(identity, out _);
        return Task.FromResult(new StagedDataWrite(stream, size =>
        {
            _pendingData[identity] = stream.ToArray();
            var prepared = new PreparedData { BucketName = bucketName, Key = key, Size = size, Tag = identity };
            prepared.SetDisposeAction(Cleanup);
            return prepared;
        }, Cleanup));
    }

    public Task<Stream?> OpenReadAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult<Stream?>(_data.TryGetValue(bucketName, out var bucket) && bucket.TryGetValue(key, out var stored)
            ? new MemoryStream(stored.Data, writable: false) : null);
    }

    public Task<Stream> OpenPreparedReadAsync(PreparedData preparedData, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult<Stream>(new MemoryStream(_pendingData[preparedData.Tag!], writable: false));
    }

    public Task CommitPreparedDataAsync(PreparedData preparedData, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var pendingKey = preparedData.Tag!;
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
        preparedData.Dispose();
        return Task.CompletedTask;
    }

    public async Task<PreparedData> PrepareMultipartDataAsync(string bucketName, string key, IEnumerable<PipeReader> partReaders, CancellationToken cancellationToken = default)
    {
        var readers = partReaders.ToList();
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            var allSegments = new List<byte[]>();

            foreach (var reader in readers)
            {
                var partData = await PipeReaderHelper.ReadAllBytesAsync(reader, true, cancellationToken);
                allSegments.Add(partData);
            }

            cancellationToken.ThrowIfCancellationRequested();
            var totalSize = allSegments.Sum(s => s.Length);
            var combinedData = new byte[totalSize];
            var offset = 0;
            foreach (var segment in allSegments)
            {
                Buffer.BlockCopy(segment, 0, combinedData, offset, segment.Length);
                offset += segment.Length;
            }

            var pendingKey = Guid.NewGuid().ToString("N");
            _pendingData[pendingKey] = combinedData;

            var preparedData = new PreparedData
            {
                BucketName = bucketName,
                Key = key,
                Size = totalSize,
                Tag = pendingKey
            };
            preparedData.SetDisposeAction(() => _pendingData.TryRemove(pendingKey, out _));

            return preparedData;
        }
        finally
        {
            await Task.WhenAll(readers.Select(reader => reader.CompleteAsync().AsTask()));
        }
    }

    public Task<PreparedData?> PrepareCopyDataAsync(string sourceBucketName, string sourceKey, string destBucketName, string destKey, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_data.TryGetValue(sourceBucketName, out var sourceBucketData) ||
            !sourceBucketData.TryGetValue(sourceKey, out var sourceStored))
        {
            return Task.FromResult<PreparedData?>(null);
        }

        var copiedData = new byte[sourceStored.Data.Length];
        Buffer.BlockCopy(sourceStored.Data, 0, copiedData, 0, sourceStored.Data.Length);


        var pendingKey = Guid.NewGuid().ToString("N");
        _pendingData[pendingKey] = copiedData;

        var preparedData = new PreparedData
        {
            BucketName = destBucketName,
            Key = destKey,
            Size = copiedData.Length,
            Tag = pendingKey
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
        if (data.Length == 0 && byteRangeStart == null && byteRangeEnd == null)
        {
            await writer.CompleteAsync();
            return true;
        }
        long startPosition = byteRangeStart ?? 0;
        long endPosition = byteRangeEnd ?? (data.Length - 1);

        if (startPosition < 0 || endPosition >= data.Length || startPosition > endPosition)
        {
            return false;
        }

        int length = (int)(endPosition - startPosition + 1);
        int offset = (int)startPosition;

        while (length > 0)
        {
            var count = Math.Min(length, 81920);
            await writer.WriteAsync(new ReadOnlyMemory<byte>(data, offset, count), cancellationToken);
            offset += count;
            length -= count;
        }
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
        var selector = new ListingPageSelector(query);
        if (query.MaxKeys != 0 && _data.TryGetValue(bucketName, out var bucketData))
        {
            // ConcurrentDictionary.Keys would allocate a snapshot of every key.
            foreach (var entry in bucketData)
            {
                cancellationToken.ThrowIfCancellationRequested();
                selector.Statistics.ScannedEntries++;
                selector.ConsiderKey(entry.Key);
            }
        }
        return Task.FromResult(selector.Finish());
    }

}
