using Lamina.Core.Models;

namespace Lamina.Storage.Core.Abstract;

public interface IObjectMetadataStorage
{
    /// <summary>
    /// Persists object metadata. <paramref name="lastModified"/>, when supplied, pins the
    /// LastModified field explicitly — used by CompleteMultipartUpload to write metadata
    /// before committing data (so a GET during the tiny in-between window returns 404
    /// rather than 200 with an auto-generated full-file MD5 ETag). When null, the
    /// filesystem backend falls back to FileInfo.LastWriteTimeUtc of the data file.
    /// </summary>
    Task<S3Object?> StoreMetadataAsync(string bucketName, string key, string etag, long size, PutObjectRequest? request = null, Dictionary<string, string>? calculatedChecksums = null, DateTime? lastModified = null, CancellationToken cancellationToken = default);
    Task<S3ObjectInfo?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<bool> DeleteMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<bool> MetadataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    IAsyncEnumerable<(string bucketName, string key)> ListAllMetadataKeysAsync(CancellationToken cancellationToken = default);
    bool IsValidObjectKey(string key);

    Task<Dictionary<string, string>?> GetObjectTagsAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<bool> SetObjectTagsAsync(string bucketName, string key, Dictionary<string, string> tags, CancellationToken cancellationToken = default);
    Task<bool> DeleteObjectTagsAsync(string bucketName, string key, CancellationToken cancellationToken = default);
}