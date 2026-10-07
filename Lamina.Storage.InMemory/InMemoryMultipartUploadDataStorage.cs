using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.InMemory;

public class InMemoryMultipartUploadDataStorage : IMultipartUploadDataStorage
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, UploadPart>> _uploadParts = new();

    private readonly ConcurrentDictionary<string, byte[]> _pending = new();

    public Task<StagedDataWrite> BeginPartWriteAsync(string bucketName, string key, string uploadId, int partNumber, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var stream = new MemoryStream();
        var identity = Guid.NewGuid().ToString("N");
        void Cleanup() => _pending.TryRemove(identity, out _);
        return Task.FromResult(new StagedDataWrite(stream, size =>
        {
            _pending[identity] = stream.ToArray();
            var prepared = new PreparedData { BucketName = bucketName, Key = key, Size = size, Tag = identity };
            prepared.SetDisposeAction(Cleanup);
            return prepared;
        }, Cleanup));
    }

    public Task CommitPreparedPartAsync(string bucketName, string key, string uploadId, int partNumber, PreparedData preparedData, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_pending.TryRemove(preparedData.Tag!, out var data)) throw new InvalidOperationException("Prepared part is unavailable");
        var parts = _uploadParts.GetOrAdd($"{bucketName}/{key}/{uploadId}", _ => new());
        parts[partNumber] = new UploadPart { PartNumber = partNumber, ETag = string.Empty, Size = data.Length, LastModified = DateTime.UtcNow, Data = data };
        return Task.CompletedTask;
    }

    public Task AbortPreparedPartAsync(PreparedData preparedData, CancellationToken cancellationToken = default)
    {
        preparedData.Dispose();
        return Task.CompletedTask;
    }

    public async Task<IEnumerable<PipeReader>> GetPartReadersAsync(string bucketName, string key, string uploadId, List<CompletedPart> parts, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        if (!_uploadParts.TryGetValue(uploadKey, out var storedParts))
        {
            return Enumerable.Empty<PipeReader>();
        }

        var orderedParts = parts.OrderBy(p => p.PartNumber).ToList();
        var readers = new List<PipeReader>();

        foreach (var completePart in orderedParts)
        {
            if (!storedParts.TryGetValue(completePart.PartNumber, out var part) || part.Data == null)
            {
                // Clean up any readers we've already created
                foreach (var r in readers)
                {
                    await r.CompleteAsync();
                }
                return Enumerable.Empty<PipeReader>();
            }

            // Create a PipeReader from the in-memory data
            var stream = new MemoryStream(part.Data);
            var partReader = PipeReader.Create(stream);
            readers.Add(partReader);
        }

        return readers;
    }

    public Task<bool> DeleteAllPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        return Task.FromResult(_uploadParts.TryRemove(uploadKey, out _));
    }

    public Task<bool> HasAnyPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        return Task.FromResult(_uploadParts.TryGetValue(uploadKey, out var parts) && !parts.IsEmpty);
    }

    public Task<List<UploadPart>> GetStoredPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        if (_uploadParts.TryGetValue(uploadKey, out var parts))
        {
            return Task.FromResult(parts.Values.OrderBy(p => p.PartNumber).Select(p => new UploadPart { PartNumber = p.PartNumber, ETag = string.Empty, Size = p.Size, LastModified = p.LastModified }).ToList());
        }
        return Task.FromResult(new List<UploadPart>());
    }
}