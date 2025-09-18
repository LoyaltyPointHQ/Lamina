using System.IO.Pipelines;

namespace S3Test.Services;

public interface IObjectDataService
{
    Task<(byte[] data, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, CancellationToken cancellationToken = default);
    Task<byte[]?> GetDataAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<bool> WriteDataToPipeAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default);
    Task<bool> DeleteDataAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<bool> DataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<long?> GetDataSizeAsync(string bucketName, string key, CancellationToken cancellationToken = default);
}