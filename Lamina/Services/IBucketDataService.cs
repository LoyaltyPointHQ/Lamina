using Lamina.Models;

namespace Lamina.Services;

public interface IBucketDataService
{
    Task<bool> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<bool> DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<bool> BucketExistsAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<List<string>> ListBucketNamesAsync(CancellationToken cancellationToken = default);
}