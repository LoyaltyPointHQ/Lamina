using System.IO.Pipelines;
using Lamina.Models;

namespace Lamina.Services;

public interface IObjectServiceFacade
{
    Task<S3Object?> PutObjectAsync(string bucketName, string key, PipeReader dataReader, PutObjectRequest? request = null, CancellationToken cancellationToken = default);
    Task<S3ObjectInfo?> GetObjectInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<bool> WriteObjectToStreamAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default);
    Task<bool> DeleteObjectAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<ListObjectsResponse> ListObjectsAsync(string bucketName, ListObjectsRequest? request = null, CancellationToken cancellationToken = default);
    Task<bool> ObjectExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default);
}