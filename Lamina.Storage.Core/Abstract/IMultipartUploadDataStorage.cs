using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.Core.Abstract;

public interface IMultipartUploadDataStorage
{
    /// <summary>
    /// Stores a part. If <paramref name="expectedMd5"/> is non-null (from the client's Content-MD5
    /// header), the implementation validates the server-computed MD5 against it and returns a
    /// <c>BadDigest</c> error with cleanup on mismatch.
    /// </summary>
    Task<StorageResult<UploadPart>> StorePartDataAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, ChecksumRequest? checksumRequest, byte[]? expectedMd5 = null, CancellationToken cancellationToken = default);
    Task<StorageResult<UploadPart>> StorePartDataAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkedDataParser chunkedDataParser, IChunkSignatureValidator chunkValidator, ChecksumRequest? checksumRequest, byte[]? expectedMd5 = null, CancellationToken cancellationToken = default);
    Task<IEnumerable<PipeReader>> GetPartReadersAsync(string bucketName, string key, string uploadId, List<CompletedPart> parts, CancellationToken cancellationToken = default);
    Task<bool> DeleteAllPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cheap existence check - returns true if any part data exists for the upload.
    /// Used by the Complete flow to preserve the data-first invariant (existence is decided
    /// by data, not metadata) while allowing metadata to be loaded before the full part listing
    /// so ETag/checksum fast paths can kick in.
    /// </summary>
    Task<bool> HasAnyPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
    /// <summary>
    /// Returns the list of uploaded parts for the given upload.
    /// <paramref name="knownMetadata"/> is an optional hint from the caller: if a part number
    /// has a non-empty ETag in the dictionary, the implementation MAY use it as-is instead of
    /// recomputing (e.g. filesystem backend skips a full MD5 pass over every part file).
    /// When absent or empty, the implementation falls back to its default behavior.
    /// </summary>
    Task<List<UploadPart>> GetStoredPartsAsync(string bucketName, string key, string uploadId, IReadOnlyDictionary<int, PartMetadata>? knownMetadata = null, CancellationToken cancellationToken = default);
}