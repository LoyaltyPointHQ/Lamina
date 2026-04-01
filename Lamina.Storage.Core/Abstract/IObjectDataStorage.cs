using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.Core.Abstract;

public class ListDataResult
{
    public List<string> Keys { get; set; } = new();
    public List<string> CommonPrefixes { get; set; } = new();
    public bool IsTruncated { get; set; } = false;
    public string? StartAfter { get; set; } = null;
}

/// <summary>
/// Represents data that has been processed (written to temp storage) but not yet made visible.
/// Call CommitPreparedDataAsync to publish, or AbortPreparedDataAsync to discard.
/// </summary>
public class PreparedData : IDisposable
{
    public required string BucketName { get; init; }
    public required string Key { get; init; }
    public required long Size { get; init; }
    public required string ETag { get; init; }
    public required Dictionary<string, string> Checksums { get; init; }

    /// <summary>
    /// Implementation-specific state (e.g., temp file path for filesystem, pending key for in-memory).
    /// </summary>
    public string? Tag { get; init; }

    private bool _disposed;
    private Action? _disposeAction;

    public void SetDisposeAction(Action action) => _disposeAction = action;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _disposeAction?.Invoke();
    }
}

public interface IObjectDataStorage
{
    // Two-phase commit: prepare processes data without making it visible
    Task<StorageResult<PreparedData>> PrepareDataAsync(string bucketName, string key, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, ChecksumRequest? checksumRequest, CancellationToken cancellationToken = default);
    Task CommitPreparedDataAsync(PreparedData preparedData, CancellationToken cancellationToken = default);
    Task AbortPreparedDataAsync(PreparedData preparedData, CancellationToken cancellationToken = default);

    // Two-phase commit for multipart
    Task<PreparedData> PrepareMultipartDataAsync(string bucketName, string key, IEnumerable<PipeReader> partReaders, CancellationToken cancellationToken = default);

    // Two-phase commit for copy
    Task<PreparedData?> PrepareCopyDataAsync(string sourceBucketName, string sourceKey, string destBucketName, string destKey, CancellationToken cancellationToken = default);

    Task<bool> WriteDataToPipeAsync(string bucketName, string key, PipeWriter writer, long? byteRangeStart = null, long? byteRangeEnd = null, CancellationToken cancellationToken = default);
    Task<bool> DeleteDataAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<bool> DataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<(long size, DateTime lastModified)?> GetDataInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default);
    Task<ListDataResult> ListDataKeysAsync(string bucketName, BucketType bucketType, string? prefix = null, string? delimiter = null, string? startAfter = null, int maxKeys = 1000, CancellationToken cancellationToken = default);
    Task<string?> ComputeETagAsync(string bucketName, string key, CancellationToken cancellationToken = default);
}
