namespace Lamina.Storage.Abstract;

public interface IBucketDataStorage
{
    Task<bool> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<bool> DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<bool> BucketExistsAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<List<string>> ListBucketNamesAsync(CancellationToken cancellationToken = default);
}