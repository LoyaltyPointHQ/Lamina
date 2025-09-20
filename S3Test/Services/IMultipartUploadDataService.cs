using System.IO.Pipelines;
using S3Test.Models;

namespace S3Test.Services;

public interface IMultipartUploadDataService
{
    Task<UploadPart> StorePartDataAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, CancellationToken cancellationToken = default);
    Task<byte[]?> GetPartDataAsync(string bucketName, string key, string uploadId, int partNumber, CancellationToken cancellationToken = default);
    Task<byte[]?> AssemblePartsAsync(string bucketName, string key, string uploadId, List<CompletedPart> parts, CancellationToken cancellationToken = default);
    Task<IEnumerable<PipeReader>> GetPartReadersAsync(string bucketName, string key, string uploadId, List<CompletedPart> parts, CancellationToken cancellationToken = default);
    Task<bool> DeletePartDataAsync(string bucketName, string key, string uploadId, int partNumber, CancellationToken cancellationToken = default);
    Task<bool> DeleteAllPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
    Task<List<UploadPart>> GetStoredPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default);
}