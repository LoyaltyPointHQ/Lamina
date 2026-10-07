using System.IO.Pipelines;
using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.Core.Abstract;

/// <summary>Raw I/O conveniences. Integrity policy belongs to the calling facade.</summary>
public static class ObjectDataStorageExtensions
{
    public static async Task<long> StoreDataAsync(this IObjectDataStorage storage, string bucketName, string key,
        PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        await using var write = await storage.BeginWriteAsync(bucketName, key, cancellationToken);
        await PipeReaderHelper.CopyToAsync(dataReader, write.Stream, false, cancellationToken);
        using var prepared = await write.SealAsync(cancellationToken);
        await storage.CommitPreparedDataAsync(prepared, cancellationToken);
        return prepared.Size;
    }

    public static async Task<long> StoreMultipartDataAsync(this IObjectDataStorage storage, string bucketName, string key,
        IEnumerable<PipeReader> partReaders, CancellationToken cancellationToken = default)
    {
        using var prepared = await storage.PrepareMultipartDataAsync(bucketName, key, partReaders, cancellationToken);
        await storage.CommitPreparedDataAsync(prepared, cancellationToken);
        return prepared.Size;
    }

    public static async Task<long?> CopyDataAsync(this IObjectDataStorage storage, string sourceBucketName, string sourceKey,
        string destBucketName, string destKey, CancellationToken cancellationToken = default)
    {
        using var prepared = await storage.PrepareCopyDataAsync(sourceBucketName, sourceKey, destBucketName, destKey, cancellationToken);
        if (prepared is null) return null;
        await storage.CommitPreparedDataAsync(prepared, cancellationToken);
        return prepared.Size;
    }
}
