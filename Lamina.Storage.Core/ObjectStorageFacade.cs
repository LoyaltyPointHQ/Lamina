using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Microsoft.Extensions.Logging;

namespace Lamina.Storage.Core;

public class ObjectStorageFacade : IObjectStorageFacade
{
    private readonly IObjectDataStorage _dataStorage;
    private readonly IObjectMetadataStorage _metadataStorage;
    private readonly IBucketStorageFacade _bucketStorage;
    private readonly IMultipartUploadStorageFacade _multipartUploadStorage;
    private readonly ILogger<ObjectStorageFacade> _logger;
    private readonly IContentTypeDetector _contentTypeDetector;

    public ObjectStorageFacade(
        IObjectDataStorage dataStorage,
        IObjectMetadataStorage metadataStorage,
        IBucketStorageFacade bucketStorage,
        IMultipartUploadStorageFacade multipartUploadStorage,
        ILogger<ObjectStorageFacade> logger,
        IContentTypeDetector contentTypeDetector)
    {
        _dataStorage = dataStorage;
        _metadataStorage = metadataStorage;
        _bucketStorage = bucketStorage;
        _multipartUploadStorage = multipartUploadStorage;
        _logger = logger;
        _contentTypeDetector = contentTypeDetector;
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
            var storeResult = await _dataStorage.StoreDataAsync(bucketName, key, dataReader, chunkValidator, cancellationToken);

            // Check if storage failed
            if (!storeResult.IsSuccess)
            {
                _logger.LogWarning("Failed to store object {Key} in bucket {BucketName}: {ErrorCode} - {ErrorMessage}", 
                    key, bucketName, storeResult.ErrorCode, storeResult.ErrorMessage);
                return null;
            }

            var (size, etag) = storeResult.Value;

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
                    Metadata = new Dictionary<string, string>(),
                    OwnerId = request?.OwnerId,
                    OwnerDisplayName = request?.OwnerDisplayName
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

    public async Task<StorageResult<ListObjectsResponse>> ListObjectsAsync(string bucketName, ListObjectsRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new ListObjectsRequest();
        var effectiveMaxKeys = Math.Min(request.MaxKeys, 1000); // S3 limits to 1000

        // Get bucket type for storage optimization
        var bucket = await _bucketStorage.GetBucketAsync(bucketName, cancellationToken);
        var bucketType = bucket?.Type ?? BucketType.GeneralPurpose;

        // Validate Directory bucket constraints
        var validationError = ValidateDirectoryBucketListRequest(bucketType, request.Prefix, request.Delimiter);
        if (validationError != null)
        {
            return StorageResult<ListObjectsResponse>.Error("InvalidArgument", validationError);
        }

        var dataResult = await _dataStorage.ListDataKeysAsync(
            bucketName,
            bucketType,
            request.Prefix,
            request.Delimiter,
            request.ContinuationToken, // This could be marker or continuation-token
            effectiveMaxKeys, // Don't limit if not checking truncation
            cancellationToken);

        var response = new ListObjectsResponse
        {
            Prefix = request.Prefix,
            Delimiter = request.Delimiter,
            MaxKeys = effectiveMaxKeys,
            IsTruncated = dataResult.IsTruncated,
            NextContinuationToken = dataResult.StartAfter,
            CommonPrefixes = dataResult.CommonPrefixes ?? new List<string>()
        };

        // For Directory buckets with delimiter, include prefixes from in-progress multipart uploads in CommonPrefixes
        if (bucketType == BucketType.Directory && !string.IsNullOrEmpty(request.Delimiter))
        {
            var multipartUploads = await _multipartUploadStorage.ListMultipartUploadsAsync(bucketName, cancellationToken);

            // Extract prefixes from multipart upload keys
            var prefixLength = request.Prefix?.Length ?? 0;
            var multipartPrefixes = new HashSet<string>();

            foreach (var upload in multipartUploads)
            {
                // Filter by prefix if specified
                if (!string.IsNullOrEmpty(request.Prefix) && !upload.Key.StartsWith(request.Prefix))
                    continue;

                // Find the first delimiter after the prefix
                var delimiterIndex = upload.Key.IndexOf(request.Delimiter, prefixLength, StringComparison.Ordinal);
                if (delimiterIndex > 0)
                {
                    var prefix = upload.Key.Substring(0, delimiterIndex + request.Delimiter.Length);
                    multipartPrefixes.Add(prefix);
                }
            }

            // Merge multipart prefixes into CommonPrefixes (avoiding duplicates)
            foreach (var prefix in multipartPrefixes)
            {
                if (!response.CommonPrefixes.Contains(prefix))
                {
                    response.CommonPrefixes.Add(prefix);
                }
            }
        }

        // Process keys to get object metadata
        foreach (var key in dataResult.Keys)
        {
            var meta = await _metadataStorage.GetMetadataAsync(bucketName, key, cancellationToken);
            if (meta == null)
            {
                var dataInfo = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
                if (dataInfo != null)
                    meta = await GenerateMetadataOnTheFlyAsync(bucketName, key, dataInfo.Value.size, dataInfo.Value.lastModified, cancellationToken);
            }
            if (meta != null)
                response.Contents.Add(meta);
        }

        return StorageResult<ListObjectsResponse>.Success(response);
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
        if (_contentTypeDetector.TryGetContentType(key, out var contentType) && contentType != null)
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

    private string? ValidateDirectoryBucketListRequest(BucketType bucketType, string? prefix, string? delimiter)
    {
        if (bucketType != BucketType.Directory)
        {
            return null; // No validation needed for general-purpose buckets
        }

        // For Directory buckets, validate delimiter constraints
        // Only "/" delimiter is supported for Directory buckets
        if (!string.IsNullOrEmpty(delimiter) && delimiter != "/")
            return "Directory buckets only support '/' as a delimiter";

        // When delimiter is specified, prefix must end with delimiter (if prefix is not empty)
        if (!string.IsNullOrEmpty(delimiter) && !string.IsNullOrEmpty(prefix) && !prefix.EndsWith(delimiter))
            return "For Directory buckets, prefixes must end with the delimiter";

        return null; // Validation passed
    }

    public async Task<DeleteMultipleObjectsResponse> DeleteMultipleObjectsAsync(string bucketName, List<ObjectIdentifier> objectsToDelete, bool quiet = false, CancellationToken cancellationToken = default)
    {
        var deleted = new List<DeletedObjectResult>();
        var errors = new List<DeleteErrorResult>();

        foreach (var objectToDelete in objectsToDelete)
        {
            try
            {
                // Validate the object key
                if (!IsValidObjectKey(objectToDelete.Key))
                {
                    errors.Add(new DeleteErrorResult(objectToDelete.Key, "InvalidObjectName", "Object key forbidden", objectToDelete.VersionId));
                    continue;
                }

                // Attempt to delete the object
                _ = await DeleteObjectAsync(bucketName, objectToDelete.Key, cancellationToken);

                // S3 delete operations always report success for non-existing objects
                // (idempotent operation), so we don't check the return value
                deleted.Add(new DeletedObjectResult(objectToDelete.Key, objectToDelete.VersionId));

                _logger.LogDebug("Successfully deleted object {Key} from bucket {BucketName}", objectToDelete.Key, bucketName);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete object {Key} from bucket {BucketName}", objectToDelete.Key, bucketName);
                errors.Add(new DeleteErrorResult(objectToDelete.Key, "InternalError", "Internal error occurred during delete", objectToDelete.VersionId));
            }
        }

        // In quiet mode, return only errors
        if (quiet)
        {
            return new DeleteMultipleObjectsResponse(new List<DeletedObjectResult>(), errors);
        }

        return new DeleteMultipleObjectsResponse(deleted, errors);
    }

    public async Task<S3Object?> CopyObjectAsync(
        string sourceBucketName,
        string sourceKey,
        string destBucketName,
        string destKey,
        string? metadataDirective = null,
        PutObjectRequest? request = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // Validate keys
            if (!IsValidObjectKey(sourceKey) || !IsValidObjectKey(destKey))
            {
                _logger.LogWarning("Invalid object key: source={SourceKey}, dest={DestKey}", sourceKey, destKey);
                return null;
            }

            // Check if source object exists
            var sourceInfo = await GetObjectInfoAsync(sourceBucketName, sourceKey, cancellationToken);
            if (sourceInfo == null)
            {
                _logger.LogWarning("Source object {SourceKey} not found in bucket {SourceBucket}", sourceKey, sourceBucketName);
                return null;
            }

            // Copy the data at storage level
            var copyResult = await _dataStorage.CopyDataAsync(sourceBucketName, sourceKey, destBucketName, destKey, cancellationToken);
            if (copyResult == null)
            {
                _logger.LogError("Failed to copy data from {SourceBucket}/{SourceKey} to {DestBucket}/{DestKey}",
                    sourceBucketName, sourceKey, destBucketName, destKey);
                return null;
            }

            var (size, etag) = copyResult.Value;

            // Determine metadata handling based on directive
            // Default is "COPY" per S3 spec
            var directive = metadataDirective?.ToUpperInvariant() ?? "COPY";

            PutObjectRequest effectiveRequest;
            if (directive == "REPLACE" && request != null)
            {
                // Use the provided metadata
                effectiveRequest = request;
            }
            else
            {
                // COPY mode: use source object's metadata
                effectiveRequest = new PutObjectRequest
                {
                    Key = destKey,
                    ContentType = sourceInfo.ContentType,
                    Metadata = new Dictionary<string, string>(sourceInfo.Metadata),
                    OwnerId = request?.OwnerId ?? sourceInfo.OwnerId,
                    OwnerDisplayName = request?.OwnerDisplayName ?? sourceInfo.OwnerDisplayName
                };
            }

            // Decide if we need to store metadata
            if (ShouldStoreMetadata(destKey, effectiveRequest))
            {
                // Store metadata
                var s3Object = await _metadataStorage.StoreMetadataAsync(destBucketName, destKey, etag, size, effectiveRequest, cancellationToken);

                if (s3Object == null)
                {
                    // Rollback data if metadata storage failed
                    await _dataStorage.DeleteDataAsync(destBucketName, destKey, cancellationToken);
                    _logger.LogError("Failed to store metadata for copied object {DestKey} in bucket {DestBucket}", destKey, destBucketName);
                    return null;
                }

                return s3Object;
            }
            else
            {
                // Return object with auto-generated metadata without storing it
                var dataInfo = await _dataStorage.GetDataInfoAsync(destBucketName, destKey, cancellationToken);
                if (dataInfo == null)
                {
                    _logger.LogError("Data was copied but cannot be retrieved for object {DestKey} in bucket {DestBucket}", destKey, destBucketName);
                    return null;
                }

                var contentType = GetContentTypeFromKey(destKey);
                return new S3Object
                {
                    Key = destKey,
                    BucketName = destBucketName,
                    Size = size,
                    LastModified = dataInfo.Value.lastModified,
                    ETag = etag,
                    ContentType = contentType,
                    Metadata = new Dictionary<string, string>(),
                    OwnerId = effectiveRequest.OwnerId,
                    OwnerDisplayName = effectiveRequest.OwnerDisplayName
                };
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error copying object from {SourceBucket}/{SourceKey} to {DestBucket}/{DestKey}",
                sourceBucketName, sourceKey, destBucketName, destKey);
            return null;
        }
    }
}