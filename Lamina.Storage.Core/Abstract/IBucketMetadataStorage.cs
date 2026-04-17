using Lamina.Core.Models;

namespace Lamina.Storage.Core.Abstract;

public interface IBucketMetadataStorage
{
    Task<Bucket?> StoreBucketMetadataAsync(string bucketName, CreateBucketRequest request, CancellationToken cancellationToken = default);
    Task<Bucket?> GetBucketMetadataAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<List<Bucket>> GetAllBucketsMetadataAsync(CancellationToken cancellationToken = default);
    Task<bool> DeleteBucketMetadataAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<Bucket?> UpdateBucketTagsAsync(string bucketName, Dictionary<string, string> tags, CancellationToken cancellationToken = default);

    Task<LifecycleConfiguration?> GetLifecycleConfigurationAsync(string bucketName, CancellationToken cancellationToken = default);
    Task<bool> SetLifecycleConfigurationAsync(string bucketName, LifecycleConfiguration configuration, CancellationToken cancellationToken = default);
    Task<bool> DeleteLifecycleConfigurationAsync(string bucketName, CancellationToken cancellationToken = default);
}