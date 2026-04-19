namespace Lamina.Storage.Core.Abstract;

public interface IBucketDataStorage
{
    Task<bool> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<bool> DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<bool> BucketExistsAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<List<string>> ListBucketNamesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the creation time of the bucket in the data backend, or null if the bucket does
    /// not exist. Implementations are expected to derive this from their native representation
    /// (directory ctime for filesystem, stored timestamp for in-memory). Metadata storages
    /// should call this instead of sampling the data backend directly (e.g. DirectoryInfo.ctime),
    /// so that the choice of data backend stays hidden behind the abstraction.
    /// </summary>
    Task<DateTime?> GetBucketCreationTimeAsync(string bucketName, CancellationToken cancellationToken = default);
}