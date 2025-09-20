using Lamina.Models;

namespace Lamina.Services;

public interface IBucketServiceFacade
{
    Task<Bucket?> CreateBucketAsync(string bucketName, CreateBucketRequest? request = null, CancellationToken cancellationToken = default);
    Task<Bucket?> GetBucketAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<ListBucketsResponse> ListBucketsAsync(CancellationToken cancellationToken = default);
    Task<bool> DeleteBucketAsync(string bucketName, bool force = false, CancellationToken cancellationToken = default);
    Task<bool> BucketExistsAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<Bucket?> UpdateBucketTagsAsync(string bucketName, Dictionary<string, string> tags, CancellationToken cancellationToken = default);
}