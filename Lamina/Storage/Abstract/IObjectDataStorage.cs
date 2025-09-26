using System.IO.Pipelines;
using Lamina.Models;
using Lamina.Streaming.Validation;

namespace Lamina.Storage.Abstract;

public class ListDataResult
{
    public List<string> Keys { get; set; } = new();
    public List<string> CommonPrefixes { get; set; } = new();
    public bool IsTruncated { get; set; } = false;
    public string? StartAfter { get; set; } = null;
}

public interface IObjectDataStorage
{
    Task<(long size, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, CancellationToken cancellationToken = default);
    Task<(long size, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, CancellationToken cancellationToken = default);
    Task<(long size, string etag)> StoreMultipartDataAsync(string bucketName, string key, IEnumerable<PipeReader> partReaders, CancellationToken cancellationToken = default);
    Task<bool> WriteDataToPipeAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default);
    Task<bool> DeleteDataAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<bool> DataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<(long size, DateTime lastModified)?> GetDataInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<ListDataResult> ListDataKeysAsync(string bucketName, BucketType bucketType, string? prefix = null, string? delimiter = null, string? startAfter = null, int maxKeys = 1000, CancellationToken cancellationToken = default);
    Task<string?> ComputeETagAsync(string bucketName, string key, CancellationToken cancellationToken = default);
}
