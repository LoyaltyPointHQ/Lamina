using System.IO.Pipelines;

namespace Lamina.Storage.Abstract;

public interface IObjectDataStorage
{
    Task<(long size, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, CancellationToken cancellationToken = default);
    Task<(long size, string etag)> StoreMultipartDataAsync(string bucketName, string key, IEnumerable<PipeReader> partReaders, CancellationToken cancellationToken = default);
    Task<bool> WriteDataToPipeAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default);
    Task<bool> DeleteDataAsync(string bucketName, string key, CancellationToken cancellationToken = default);
}
