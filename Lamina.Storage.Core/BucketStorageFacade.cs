using Lamina.Core.Models;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Core;

public class BucketStorageFacade : IBucketStorageFacade
{
    private readonly IBucketDataStorage _dataStorage;
    private readonly IBucketMetadataStorage _metadataStorage;
    private readonly BucketDefaultsSettings _bucketDefaults;
    private readonly ILogger<BucketStorageFacade> _logger;
    public BucketStorageFacade(
        IBucketDataStorage dataStorage,
        IBucketMetadataStorage metadataStorage,
        IOptions<BucketDefaultsSettings> bucketDefaultsOptions,
        ILogger<BucketStorageFacade> logger)
    {
        _dataStorage = dataStorage;
        _metadataStorage = metadataStorage;
        _bucketDefaults = bucketDefaultsOptions.Value;
        _logger = logger;
    }

    public async Task<Bucket?> CreateBucketAsync(string bucketName, CreateBucketRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (!BucketNameValidator.IsValid(bucketName))
        {
            _logger.LogWarning("Invalid bucket name: {BucketName}", bucketName);
            return null;
        }

        // Create default request if null was provided
        request ??= new CreateBucketRequest();
        request.Type ??= _bucketDefaults.Type;
        request.StorageClass ??= _bucketDefaults.StorageClass;
        

        // Create bucket in data service
        var created = await _dataStorage.CreateBucketAsync(bucketName, cancellationToken);
        if (!created)
        {
            return null;
        }

        // Store metadata
        var bucket = await _metadataStorage.StoreBucketMetadataAsync(bucketName, request, cancellationToken);
        if (bucket == null)
        {
            // Rollback data creation if metadata storage failed
            await _dataStorage.DeleteBucketAsync(bucketName, force: true, cancellationToken);
            _logger.LogError("Failed to store metadata for bucket {BucketName}", bucketName);
            return null;
        }

        return bucket;
    }

    public async Task<Bucket?> GetBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.GetBucketMetadataAsync(bucketName, cancellationToken);
    }

    public async Task<ListBucketsResponse> ListBucketsAsync(CancellationToken cancellationToken = default)
    {
        var buckets = await _metadataStorage.GetAllBucketsMetadataAsync(cancellationToken);

        return new ListBucketsResponse
        {
            Buckets = buckets
        };
    }

    public async Task<DeleteBucketResult> DeleteBucketAsync(string bucketName, bool force = false, CancellationToken cancellationToken = default)
    {
        var result = await _dataStorage.DeleteBucketAsync(bucketName, force, cancellationToken);
        if (result != DeleteBucketResult.Success)
        {
            return result;
        }

        await _metadataStorage.DeleteBucketMetadataAsync(bucketName, cancellationToken);
        return DeleteBucketResult.Success;
    }

    public async Task<bool> BucketExistsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await _dataStorage.BucketExistsAsync(bucketName, cancellationToken);
    }

    public async Task<Bucket?> UpdateBucketTagsAsync(string bucketName, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.UpdateBucketTagsAsync(bucketName, tags, cancellationToken);
    }

    public Task<bool> UpdateBucketLifecycleAsync(string bucketName, LifecycleConfiguration? lifecycle, CancellationToken cancellationToken = default)
    {
        return _metadataStorage.UpdateBucketLifecycleAsync(bucketName, lifecycle, cancellationToken);
    }
}