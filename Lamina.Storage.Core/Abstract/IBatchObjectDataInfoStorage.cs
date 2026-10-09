namespace Lamina.Storage.Core.Abstract;

/// <summary>Optional bounded batch stat capability. Never reads or hashes object contents.</summary>
public interface IBatchObjectDataInfoStorage
{
    Task<IReadOnlyDictionary<string, (long size, DateTime lastModified)?>> GetDataInfoBatchAsync(
        string bucketName, IReadOnlyList<string> keys, CancellationToken cancellationToken = default);
}
