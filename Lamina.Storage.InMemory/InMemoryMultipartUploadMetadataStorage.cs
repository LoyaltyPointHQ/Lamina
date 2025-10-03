using System.Collections.Concurrent;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;

namespace Lamina.Storage.InMemory;

public class InMemoryMultipartUploadMetadataStorage : IMultipartUploadMetadataStorage
{
    private readonly ConcurrentDictionary<string, MultipartUpload> _uploads = new();

    public Task<MultipartUpload> InitiateUploadAsync(string bucketName, string key, InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        var uploadId = Guid.NewGuid().ToString();
        var upload = new MultipartUpload
        {
            UploadId = uploadId,
            Key = key,
            BucketName = bucketName,
            Initiated = DateTime.UtcNow,
            ContentType = request.ContentType ?? "application/octet-stream",
            Metadata = request.Metadata ?? new Dictionary<string, string>()
        };

        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        _uploads[uploadKey] = upload;

        return Task.FromResult(upload);
    }

    public Task<MultipartUpload?> GetUploadMetadataAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        if (_uploads.TryGetValue(uploadKey, out var upload))
        {
            return Task.FromResult<MultipartUpload?>(upload);
        }
        return Task.FromResult<MultipartUpload?>(null);
    }

    public Task<bool> DeleteUploadMetadataAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        return Task.FromResult(_uploads.TryRemove(uploadKey, out _));
    }

    public Task<List<MultipartUpload>> ListUploadsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var bucketUploads = _uploads.Values
            .Where(u => u.BucketName == bucketName)
            .OrderBy(u => u.Initiated)
            .ToList();

        return Task.FromResult(bucketUploads);
    }
}