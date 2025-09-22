using System.IO.Pipelines;
using Lamina.Services;

namespace Lamina.Storage.Abstract;

public interface IObjectDataStorage
{
    Task<(long size, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, CancellationToken cancellationToken = default);
    Task<(long size, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, CancellationToken cancellationToken = default);
    Task<(long size, string etag)> StoreMultipartDataAsync(string bucketName, string key, IEnumerable<PipeReader> partReaders, CancellationToken cancellationToken = default);
    Task<bool> WriteDataToPipeAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default);
    Task<bool> DeleteDataAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<bool> DataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<(long size, DateTime lastModified)?> GetDataInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<IEnumerable<string>> ListDataKeysAsync(string bucketName, string? prefix = null, CancellationToken cancellationToken = default);
    Task<string?> ComputeETagAsync(string bucketName, string key, CancellationToken cancellationToken = default);
}
