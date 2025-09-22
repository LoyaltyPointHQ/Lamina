using Lamina.Models;

namespace Lamina.Storage.Abstract;

public interface IObjectMetadataStorage
{
    Task<S3Object?> StoreMetadataAsync(string bucketName, string key, string etag, long size, PutObjectRequest? request = null, CancellationToken cancellationToken = default);
    Task<S3ObjectInfo?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<bool> DeleteMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<bool> MetadataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    bool IsValidObjectKey(string key);
}