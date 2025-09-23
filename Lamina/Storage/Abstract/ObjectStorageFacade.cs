using System.IO.Pipelines;
using Lamina.Models;
using Lamina.Streaming.Validation;
using Microsoft.AspNetCore.StaticFiles;

namespace Lamina.Storage.Abstract;

public class ObjectStorageFacade : IObjectStorageFacade
{
    private readonly IObjectDataStorage _dataStorage;
    private readonly IObjectMetadataStorage _metadataStorage;
    private readonly ILogger<ObjectStorageFacade> _logger;
    private readonly IContentTypeProvider _contentTypeProvider;

    public ObjectStorageFacade(
        IObjectDataStorage dataStorage,
        IObjectMetadataStorage metadataStorage,
        ILogger<ObjectStorageFacade> logger)
    {
        _dataStorage = dataStorage;
        _metadataStorage = metadataStorage;
        _logger = logger;
        _contentTypeProvider = new FileExtensionContentTypeProvider();
    }

    public async Task<S3Object?> PutObjectAsync(string bucketName, string key, PipeReader dataReader, PutObjectRequest? request = null, CancellationToken cancellationToken = default)
    {
        return await PutObjectAsync(bucketName, key, dataReader, null, request, cancellationToken);
    }

    public async Task<S3Object?> PutObjectAsync(string bucketName, string key, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, PutObjectRequest? request = null, CancellationToken cancellationToken = default)
    {
        try
        {
            // Store data with chunk validation and get ETag
            var (size, etag) = await _dataStorage.StoreDataAsync(bucketName, key, dataReader, chunkValidator, cancellationToken);

            // Check if validation failed (indicated by empty etag)
            if (string.IsNullOrEmpty(etag))
            {
                _logger.LogWarning("Chunk signature validation failed for object {Key} in bucket {BucketName}", key, bucketName);
                return null;
            }

            // Check if we should store metadata or just return auto-generated object
            if (ShouldStoreMetadata(key, request))
            {
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
            else
            {
                // Return object with auto-generated metadata without storing it
                var dataPath = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
                if (dataPath == null)
                {
                    _logger.LogError("Data was stored but cannot be retrieved for object {Key} in bucket {BucketName}", key, bucketName);
                    return null;
                }

                var contentType = GetContentTypeFromKey(key);
                return new S3Object
                {
                    Key = key,
                    BucketName = bucketName,
                    Size = size,
                    LastModified = dataPath.Value.lastModified,
                    ETag = etag,
                    ContentType = contentType,
                    Metadata = new Dictionary<string, string>()
                };
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error storing object {Key} in bucket {BucketName} with chunk validation", key, bucketName);
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
        // First check if data exists
        var dataInfo = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
        if (dataInfo == null)
        {
            return null;
        }

        // Try to get metadata
        var metadata = await _metadataStorage.GetMetadataAsync(bucketName, key, cancellationToken);
        if (metadata != null)
        {
            return metadata;
        }

        // If data exists but metadata doesn't, generate metadata on the fly
        _logger.LogInformation("Generating metadata on-the-fly for object {Key} in bucket {BucketName}", key, bucketName);
        return await GenerateMetadataOnTheFlyAsync(bucketName, key, dataInfo.Value.size, dataInfo.Value.lastModified, cancellationToken);
    }

    public async Task<ListObjectsResponse> ListObjectsAsync(string bucketName, ListObjectsRequest? request = null, CancellationToken cancellationToken = default)
    {
        // Get data keys to find objects without metadata
        var dataKeys = await _dataStorage.ListDataKeysAsync(bucketName, request?.Prefix, cancellationToken);
        var response = new ListObjectsResponse
        {
            Prefix = request?.Prefix,
            MaxKeys = request?.MaxKeys ?? 1000,
            IsTruncated = false,
            NextContinuationToken = null
        };

        foreach (var key in dataKeys)
        {
            var meta = await _metadataStorage.GetMetadataAsync(bucketName, key, cancellationToken);
            if (meta == null)
            {
                var dataInfo = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
                if (dataInfo != null)
                {
                    _logger.LogInformation("Found orphaned data without metadata for key {Key} in bucket {BucketName}", key, bucketName);

                    meta = await GenerateMetadataOnTheFlyAsync(bucketName, key, dataInfo.Value.size, dataInfo.Value.lastModified, cancellationToken);
                    
                }
            }
            if (meta != null)
                response.Contents.Add(meta);
        }

        return response;
    }

    public async Task<bool> ObjectExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        // Check data existence first (source of truth)
        return await _dataStorage.DataExistsAsync(bucketName, key, cancellationToken);
    }

    public bool IsValidObjectKey(string key) => _metadataStorage.IsValidObjectKey(key);

    private async Task<S3ObjectInfo?> GenerateMetadataOnTheFlyAsync(string bucketName, string key, long size, DateTime lastModified, CancellationToken cancellationToken)
    {
        // Compute ETag from the data efficiently
        var etag = await _dataStorage.ComputeETagAsync(bucketName, key, cancellationToken);
        if (etag == null)
        {
            _logger.LogWarning("Failed to compute ETag for object {Key} in bucket {BucketName}", key, bucketName);
            return null;
        }

        // Determine content type based on file extension
        var contentType = GetContentTypeFromKey(key);

        return new S3ObjectInfo
        {
            Key = key,
            LastModified = lastModified,
            ETag = etag,
            Size = size,
            ContentType = contentType,
            Metadata = new Dictionary<string, string>()
        };
    }

    private string GetContentTypeFromKey(string key)
    {
        // Try to determine content type from file extension
        if (_contentTypeProvider.TryGetContentType(key, out var contentType))
        {
            return contentType;
        }

        // Check for some common extensions that might not be in the default provider
        var extension = Path.GetExtension(key).ToLowerInvariant();
        return extension switch
        {
            ".log" => "text/plain",
            ".yaml" or ".yml" => "text/yaml",
            ".toml" => "text/plain",
            ".env" => "text/plain",
            ".dockerfile" => "text/plain",
            ".gitignore" => "text/plain",
            ".editorconfig" => "text/plain",
            ".properties" => "text/plain",
            ".conf" or ".config" => "text/plain",
            _ => "application/octet-stream" // Default fallback
        };
    }

    private bool ShouldStoreMetadata(string key, PutObjectRequest? request)
    {
        // If no request provided, no metadata to store
        if (request == null)
        {
            return false;
        }

        // Get the auto-detected content type for comparison
        var autoDetectedContentType = GetContentTypeFromKey(key);

        // Check if provided content type differs from auto-detected
        var providedContentType = request.ContentType ?? autoDetectedContentType;
        if (!string.Equals(providedContentType, autoDetectedContentType, StringComparison.OrdinalIgnoreCase))
        {
            return true; // Content type is custom, store metadata
        }

        // Check if there's any user metadata
        if (request.Metadata is { Count: > 0 })
        {
            return true; // User metadata exists, store metadata
        }

        // Metadata matches defaults, no need to store
        return false;
    }
}