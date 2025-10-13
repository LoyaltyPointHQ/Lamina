using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.Core.Abstract;

public interface IMultipartUploadStorageFacade
{
    Task<MultipartUpload> InitiateMultipartUploadAsync(string bucketName, string key, InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default);
    Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, CancellationToken cancellationToken = default);
    Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, CancellationToken cancellationToken = default);
    Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, ChecksumRequest? checksumRequest, CancellationToken cancellationToken = default);
    Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, ChecksumRequest? checksumRequest, CancellationToken cancellationToken = default);
    Task<StorageResult<CompleteMultipartUploadResponse>> CompleteMultipartUploadAsync(string bucketName, string key, CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default);
    Task<bool> AbortMultipartUploadAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
    Task<List<UploadPart>> ListPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
    Task<List<MultipartUpload>> ListMultipartUploadsAsync(string bucketName, CancellationToken cancellationToken = default);
}