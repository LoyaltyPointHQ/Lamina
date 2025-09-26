using System.IO.Pipelines;
using Lamina.Models;
using Lamina.Streaming.Validation;
using Microsoft.AspNetCore.StaticFiles;

namespace Lamina.Storage.Abstract;

public class ObjectStorageFacade : IObjectStorageFacade
{
    private readonly IObjectDataStorage _dataStorage;
    private readonly IObjectMetadataStorage _metadataStorage;
    private readonly IBucketStorageFacade _bucketStorage;
    private readonly ILogger<ObjectStorageFacade> _logger;
    private readonly IContentTypeProvider _contentTypeProvider;

    public ObjectStorageFacade(
        IObjectDataStorage dataStorage,
        IObjectMetadataStorage metadataStorage,
        IBucketStorageFacade bucketStorage,
        ILogger<ObjectStorageFacade> logger)
    {
        _dataStorage = dataStorage;
        _metadataStorage = metadataStorage;
        _bucketStorage = bucketStorage;
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

        // Only request extra if maxKeys is specified and less than 1000
        var shouldCheckTruncation = request.MaxKeys > 0 && request.MaxKeys < 1000;
        var requestLimit = shouldCheckTruncation ? effectiveMaxKeys + 1 : effectiveMaxKeys;

        var dataResult = await _dataStorage.ListDataKeysAsync(
            bucketName,
            bucketType,
            request.Prefix,
            request.Delimiter,
            request.ContinuationToken, // This could be marker or continuation-token
            shouldCheckTruncation ? requestLimit : null, // Don't limit if not checking truncation
            cancellationToken);

        // Ensure dataResult is not null
        if (dataResult == null)
        {
            dataResult = new ListDataResult();
        }

        var response = new ListObjectsResponse
        {
            Prefix = request.Prefix,
            Delimiter = request.Delimiter,
            MaxKeys = effectiveMaxKeys,
            IsTruncated = false,
            NextContinuationToken = null,
            CommonPrefixes = dataResult.CommonPrefixes ?? new List<string>()
        };

        // Calculate total items (keys + common prefixes) for truncation detection
        var totalItems = dataResult.Keys.Count + (dataResult.CommonPrefixes?.Count ?? 0);

        // Check if we need to truncate:
        // If we requested N+1 items and got exactly N+1 items, then there are more items available
        // If we requested N+1 items and got N or fewer items, then that's all there are
        if (shouldCheckTruncation && totalItems == effectiveMaxKeys + 1)
        {
            response.IsTruncated = true;

            // Combine keys and common prefixes, sort them, and take the limit
            var allItems = new List<(string item, bool isPrefix)>();
            allItems.AddRange(dataResult.Keys.Select(k => (k, false)));
            allItems.AddRange((dataResult.CommonPrefixes ?? new List<string>()).Select(p => (p, true)));

            // Sort lexicographically
            allItems.Sort((x, y) => string.Compare(x.item, y.item, StringComparison.Ordinal));

            // Take only up to maxKeys and determine NextMarker
            var limitedItems = allItems.Take(effectiveMaxKeys).ToList();
            if (limitedItems.Count > 0)
            {
                response.NextContinuationToken = limitedItems.Last().item;
            }

            // Update the response with limited results
            response.CommonPrefixes = limitedItems.Where(item => item.isPrefix).Select(item => item.item).ToList();
            dataResult.Keys = limitedItems.Where(item => !item.isPrefix).Select(item => item.item).ToList();
        }

        // Process keys to get object metadata
        foreach (var key in dataResult.Keys)
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
}