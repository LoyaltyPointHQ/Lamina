using System.IO.Pipelines;
using Lamina.Models;

namespace Lamina.Storage.Abstract;

public class ObjectStorageFacade : IObjectStorageFacade
{
    private readonly IObjectDataStorage _dataStorage;
    private readonly IObjectMetadataStorage _metadataStorage;
    private readonly ILogger<ObjectStorageFacade> _logger;

    public ObjectStorageFacade(
        IObjectDataStorage dataStorage,
        IObjectMetadataStorage metadataStorage,
        ILogger<ObjectStorageFacade> logger)
    {
        _dataStorage = dataStorage;
        _metadataStorage = metadataStorage;
        _logger = logger;
    }

    public async Task<S3Object?> PutObjectAsync(string bucketName, string key, PipeReader dataReader, PutObjectRequest? request = null, CancellationToken cancellationToken = default)
    {
        try
        {
            // Store data and get ETag
            var (size, etag) = await _dataStorage.StoreDataAsync(bucketName, key, dataReader, cancellationToken);

            // Store metadata
            var s3Object = await _metadataStorage.StoreMetadataAsync(bucketName, key, etag, size, request, cancellationToken);

            if (s3Object == null)
            {
                // Rollback data if metadata storage failed
                await _dataStorage.DeleteDataAsync(bucketName, key, cancellationToken);
                _logger.LogError("Failed to store metadata for object {Key} in bucket {BucketName}", key, bucketName);
                return null;
            }

            return s3Object;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error storing object {Key} in bucket {BucketName}", key, bucketName);
            // Try to clean up any partial data
            await _dataStorage.DeleteDataAsync(bucketName, key, cancellationToken);
            throw;
        }
    }

    public async Task<bool> WriteObjectToStreamAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default)
    {
        return await _dataStorage.WriteDataToPipeAsync(bucketName, key, writer, cancellationToken);
    }

    public async Task<bool> DeleteObjectAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataDeleted = await _dataStorage.DeleteDataAsync(bucketName, key, cancellationToken);
        var metadataDeleted = await _metadataStorage.DeleteMetadataAsync(bucketName, key, cancellationToken);

        return dataDeleted || metadataDeleted; // Return true if at least one was deleted
    }

    public async Task<S3ObjectInfo?> GetObjectInfoAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.GetMetadataAsync(bucketName, key, cancellationToken);
    }

    public async Task<ListObjectsResponse> ListObjectsAsync(string bucketName, ListObjectsRequest? request = null, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.ListObjectsAsync(bucketName, request, cancellationToken);
    }

    public async Task<bool> ObjectExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.MetadataExistsAsync(bucketName, key, cancellationToken);
    }
}