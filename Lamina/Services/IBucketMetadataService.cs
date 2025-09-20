using Lamina.Models;

namespace Lamina.Services;

public interface IBucketMetadataService
{
    Task<Bucket?> StoreBucketMetadataAsync(string bucketName, CreateBucketRequest? request = null, CancellationToken cancellationToken = default);
    Task<Bucket?> GetBucketMetadataAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<List<Bucket>> GetAllBucketsMetadataAsync(CancellationToken cancellationToken = default);
    Task<bool> DeleteBucketMetadataAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<Bucket?> UpdateBucketTagsAsync(string bucketName, Dictionary<string, string> tags, CancellationToken cancellationToken = default);
}