using System.IO.Pipelines;
using S3Test.Models;

namespace S3Test.Services;

public class ObjectServiceFacade : IObjectServiceFacade
{
    private readonly IObjectDataService _dataService;
    private readonly IObjectMetadataService _metadataService;
    private readonly ILogger<ObjectServiceFacade> _logger;

    public ObjectServiceFacade(
        IObjectDataService dataService,
        IObjectMetadataService metadataService,
        ILogger<ObjectServiceFacade> logger)
    {
        _dataService = dataService;
        _metadataService = metadataService;
        _logger = logger;
    }

    public async Task<S3Object?> PutObjectAsync(string bucketName, string key, PipeReader dataReader, PutObjectRequest? request = null, CancellationToken cancellationToken = default)
    {
        try
        {
            // Store data and get ETag
            var (data, etag) = await _dataService.StoreDataAsync(bucketName, key, dataReader, cancellationToken);

            // Store metadata
            var s3Object = await _metadataService.StoreMetadataAsync(bucketName, key, etag, data.Length, request, cancellationToken);

            if (s3Object == null)
            {
                // Rollback data if metadata storage failed
                await _dataService.DeleteDataAsync(bucketName, key, cancellationToken);
                _logger.LogError("Failed to store metadata for object {Key} in bucket {BucketName}", key, bucketName);
                return null;
            }

            return s3Object;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error storing object {Key} in bucket {BucketName}", key, bucketName);
            // Try to clean up any partial data
            await _dataService.DeleteDataAsync(bucketName, key, cancellationToken);
            throw;
        }
    }

    public async Task<bool> WriteObjectToStreamAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default)
    {
        return await _dataService.WriteDataToPipeAsync(bucketName, key, writer, cancellationToken);
    }

    public async Task<bool> DeleteObjectAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataDeleted = await _dataService.DeleteDataAsync(bucketName, key, cancellationToken);
        var metadataDeleted = await _metadataService.DeleteMetadataAsync(bucketName, key, cancellationToken);

        return dataDeleted || metadataDeleted; // Return true if at least one was deleted
    }

    public async Task<S3ObjectInfo?> GetObjectInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        return await _metadataService.GetMetadataAsync(bucketName, key, cancellationToken);
    }

    public async Task<ListObjectsResponse> ListObjectsAsync(string bucketName, ListObjectsRequest? request = null, CancellationToken cancellationToken = default)
    {
        return await _metadataService.ListObjectsAsync(bucketName, request, cancellationToken);
    }

    public async Task<bool> ObjectExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        return await _metadataService.MetadataExistsAsync(bucketName, key, cancellationToken);
    }
}