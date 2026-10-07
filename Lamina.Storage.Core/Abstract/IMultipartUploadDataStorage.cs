using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.Core.Abstract;

public interface IMultipartUploadDataStorage
{
    Task<StagedDataWrite> BeginPartWriteAsync(string bucketName, string key, string uploadId, int partNumber, CancellationToken cancellationToken = default);
    Task CommitPreparedPartAsync(string bucketName, string key, string uploadId, int partNumber, PreparedData preparedData, CancellationToken cancellationToken = default);
    Task AbortPreparedPartAsync(PreparedData preparedData, CancellationToken cancellationToken = default);
    Task<IEnumerable<PipeReader>> GetPartReadersAsync(string bucketName, string key, string uploadId, List<CompletedPart> parts, CancellationToken cancellationToken = default);
    Task<bool> DeleteAllPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cheap existence check - returns true if any part data exists for the upload.
    /// Used by the Complete flow to preserve the data-first invariant (existence is decided
    /// by data, not metadata) while allowing metadata to be loaded before the full part listing
    /// so ETag/checksum fast paths can kick in.
    /// </summary>
    Task<bool> HasAnyPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
    Task<List<UploadPart>> GetStoredPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
}
