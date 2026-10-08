using Lamina.Core.Models;

namespace Lamina.Storage.Core.Abstract;

public interface IMultipartUploadMetadataStorage
{
    /// <summary>Lists all uploads in one scan for background maintenance.</summary>
    Task<List<MultipartUpload>> ListAllUploadsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams active upload keys without ordering or deduplication; multiple uploads may share a key.
    /// </summary>
    IAsyncEnumerable<string> EnumerateUploadKeysAsync(string bucketName, CancellationToken cancellationToken = default);

    Task<MultipartUpload> InitiateUploadAsync(string bucketName, string key, InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default);
    Task<MultipartUpload?> GetUploadMetadataAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
    Task<bool> DeleteUploadMetadataAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
    Task<List<MultipartUpload>> ListUploadsAsync(string bucketName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates the multipart upload metadata.
    /// This is used to persist part checksums in the upload metadata.
    /// </summary>
    Task UpdateUploadMetadataAsync(string bucketName, string key, string uploadId, MultipartUpload upload, CancellationToken cancellationToken = default);
}