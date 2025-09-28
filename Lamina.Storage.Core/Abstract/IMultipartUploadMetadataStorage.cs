using Lamina.Core.Models;

namespace Lamina.Storage.Core.Abstract;

public interface IMultipartUploadMetadataStorage
{
    Task<MultipartUpload> InitiateUploadAsync(string bucketName, string key, InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default);
    Task<MultipartUpload?> GetUploadMetadataAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
    Task<bool> DeleteUploadMetadataAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
    Task<List<MultipartUpload>> ListUploadsAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<bool> UploadExistsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
}