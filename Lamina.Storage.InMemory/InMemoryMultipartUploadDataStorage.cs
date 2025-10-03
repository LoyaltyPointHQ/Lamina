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

    public async Task<UploadPart> StorePartDataAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        var parts = _uploadParts.GetOrAdd(uploadKey, _ => new ConcurrentDictionary<int, UploadPart>());

        var combinedData = await PipeReaderHelper.ReadAllBytesAsync(dataReader, false, cancellationToken);

        var etag = ETagHelper.ComputeETag(combinedData);

        var part = new UploadPart
        {
            PartNumber = partNumber,
            ETag = etag,
            Size = combinedData.Length,
            LastModified = DateTime.UtcNow,
            Data = combinedData
        };

        parts[partNumber] = part;
        return part;
    }

    public Task<IEnumerable<PipeReader>> GetPartReadersAsync(string bucketName, string key, string uploadId, List<CompletedPart> parts, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        if (!_uploadParts.TryGetValue(uploadKey, out var storedParts))
        {
            return Task.FromResult(Enumerable.Empty<PipeReader>());
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
                    _ = r.CompleteAsync();
                }
                return Task.FromResult(Enumerable.Empty<PipeReader>());
            }

            // Create a PipeReader from the in-memory data
            var stream = new MemoryStream(part.Data);
            var partReader = PipeReader.Create(stream);
            readers.Add(partReader);
        }

        return Task.FromResult<IEnumerable<PipeReader>>(readers);
    }

    public Task<bool> DeleteAllPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        return Task.FromResult(_uploadParts.TryRemove(uploadKey, out _));
    }

    public Task<List<UploadPart>> GetStoredPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        if (_uploadParts.TryGetValue(uploadKey, out var parts))
        {
            return Task.FromResult(parts.Values.OrderBy(p => p.PartNumber).ToList());
        }
        return Task.FromResult(new List<UploadPart>());
    }

}