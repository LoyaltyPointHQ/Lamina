using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;

namespace Lamina.Storage.Core.Abstract;

public interface IMultipartUploadDataStorage
{
    Task<UploadPart> StorePartDataAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, CancellationToken cancellationToken = default);
    Task<UploadPart?> StorePartDataAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkedDataParser chunkedDataParser, IChunkSignatureValidator chunkValidator, CancellationToken cancellationToken = default);
    Task<IEnumerable<PipeReader>> GetPartReadersAsync(string bucketName, string key, string uploadId, List<CompletedPart> parts, CancellationToken cancellationToken = default);
    Task<bool> DeleteAllPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
    Task<List<UploadPart>> GetStoredPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
}