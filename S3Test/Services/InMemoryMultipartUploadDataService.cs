using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using S3Test.Helpers;
using S3Test.Models;

namespace S3Test.Services;

public class InMemoryMultipartUploadDataService : IMultipartUploadDataService
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, UploadPart>> _uploadParts = new();

    public async Task<UploadPart> StorePartDataAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        var parts = _uploadParts.GetOrAdd(uploadKey, _ => new ConcurrentDictionary<int, UploadPart>());

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

        var part = new UploadPart
        {
            PartNumber = partNumber,
            ETag = etag,
            Size = totalSize,
            LastModified = DateTime.UtcNow,
            Data = combinedData
        };

        parts[partNumber] = part;
        return part;
    }

    public Task<byte[]?> GetPartDataAsync(string bucketName, string key, string uploadId, int partNumber, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        if (_uploadParts.TryGetValue(uploadKey, out var parts) &&
            parts.TryGetValue(partNumber, out var part))
        {
            return Task.FromResult<byte[]?>(part.Data);
        }
        return Task.FromResult<byte[]?>(null);
    }

    public Task<byte[]?> AssemblePartsAsync(string bucketName, string key, string uploadId, List<CompletedPart> parts, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        if (!_uploadParts.TryGetValue(uploadKey, out var storedParts))
        {
            return Task.FromResult<byte[]?>(null);
        }

        var orderedParts = parts.OrderBy(p => p.PartNumber).ToList();
        var totalSize = 0L;

        foreach (var completePart in orderedParts)
        {
            if (!storedParts.TryGetValue(completePart.PartNumber, out var part))
            {
                return Task.FromResult<byte[]?>(null);
            }
            totalSize += part.Size;
        }

        var combinedData = new byte[totalSize];
        var offset = 0;

        foreach (var completePart in orderedParts)
        {
            if (storedParts.TryGetValue(completePart.PartNumber, out var part))
            {
                Buffer.BlockCopy(part.Data!, 0, combinedData, offset, (int)part.Size);
                offset += (int)part.Size;
            }
        }

        return Task.FromResult<byte[]?>(combinedData);
    }

    public Task<bool> DeletePartDataAsync(string bucketName, string key, string uploadId, int partNumber, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        if (_uploadParts.TryGetValue(uploadKey, out var parts))
        {
            return Task.FromResult(parts.TryRemove(partNumber, out _));
        }
        return Task.FromResult(false);
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