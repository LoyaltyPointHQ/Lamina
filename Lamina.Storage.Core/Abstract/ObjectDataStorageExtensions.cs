using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.Core.Abstract;

/// <summary>
/// Convenience extension methods that combine prepare + commit into a single call.
/// Used by tests and code that doesn't need metadata-before-data ordering.
/// </summary>
public static class ObjectDataStorageExtensions
{
    public static async Task<StorageResult<(long size, string etag, Dictionary<string, string> checksums)>> StoreDataAsync(
        this IObjectDataStorage storage,
        string bucketName,
        string key,
        PipeReader dataReader,
        IChunkSignatureValidator? chunkValidator,
        ChecksumRequest? checksumRequest,
        CancellationToken cancellationToken = default)
    {
        var prepareResult = await storage.PrepareDataAsync(bucketName, key, dataReader, chunkValidator, checksumRequest, cancellationToken);

        if (!prepareResult.IsSuccess)
        {
            return StorageResult<(long size, string etag, Dictionary<string, string> checksums)>.Error(prepareResult.ErrorCode!, prepareResult.ErrorMessage!);
        }

        using var preparedData = prepareResult.Value!;
        await storage.CommitPreparedDataAsync(preparedData, cancellationToken);

        return StorageResult<(long size, string etag, Dictionary<string, string> checksums)>.Success(
            (preparedData.Size, preparedData.ETag, preparedData.Checksums));
    }

    public static async Task<(long size, string etag)> StoreMultipartDataAsync(
        this IObjectDataStorage storage,
        string bucketName,
        string key,
        IEnumerable<PipeReader> partReaders,
        CancellationToken cancellationToken = default)
    {
        using var preparedData = await storage.PrepareMultipartDataAsync(bucketName, key, partReaders, cancellationToken);
        await storage.CommitPreparedDataAsync(preparedData, cancellationToken);
        return (preparedData.Size, preparedData.ETag);
    }

    public static async Task<(long size, string etag)?> CopyDataAsync(
        this IObjectDataStorage storage,
        string sourceBucketName,
        string sourceKey,
        string destBucketName,
        string destKey,
        CancellationToken cancellationToken = default)
    {
        using var preparedData = await storage.PrepareCopyDataAsync(sourceBucketName, sourceKey, destBucketName, destKey, cancellationToken);
        if (preparedData == null)
            return null;

        await storage.CommitPreparedDataAsync(preparedData, cancellationToken);
        return (preparedData.Size, preparedData.ETag);
    }
}
