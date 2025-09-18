using System.IO.Pipelines;
using S3Test.Models;

namespace S3Test.Services;

public interface IMultipartUploadService
{
    Task<InitiateMultipartUploadResponse> InitiateMultipartUploadAsync(
        string bucketName,
        InitiateMultipartUploadRequest request,
        CancellationToken cancellationToken = default);

    Task<UploadPartResponse> UploadPartAsync(
        string bucketName,
        string key,
        string uploadId,
        int partNumber,
        PipeReader dataReader,
        CancellationToken cancellationToken = default);

    Task<CompleteMultipartUploadResponse?> CompleteMultipartUploadAsync(
        string bucketName,
        string key,
        CompleteMultipartUploadRequest request,
        CancellationToken cancellationToken = default);

    Task<bool> AbortMultipartUploadAsync(
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default);

    Task<List<UploadPart>> ListPartsAsync(
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default);

    Task<List<MultipartUpload>> ListMultipartUploadsAsync(
        string bucketName,
        CancellationToken cancellationToken = default);
}