using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;

namespace Lamina.Storage.Core.Abstract;

public interface IObjectStorageFacade
{
    Task<S3Object?> PutObjectAsync(string bucketName, string key, PipeReader dataReader, PutObjectRequest? request = null, CancellationToken cancellationToken = default);
    Task<S3Object?> PutObjectAsync(string bucketName, string key, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, PutObjectRequest? request = null, CancellationToken cancellationToken = default);
    Task<S3ObjectInfo?> GetObjectInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<bool> WriteObjectToStreamAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default);
    Task<bool> DeleteObjectAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<StorageResult<ListObjectsResponse>> ListObjectsAsync(string bucketName, ListObjectsRequest? request = null, CancellationToken cancellationToken = default);
    Task<bool> ObjectExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    bool IsValidObjectKey(string key);
    Task<DeleteMultipleObjectsResponse> DeleteMultipleObjectsAsync(string bucketName, List<ObjectIdentifier> objectsToDelete, bool quiet = false, CancellationToken cancellationToken = default);
}