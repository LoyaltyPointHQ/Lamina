using System.IO.Pipelines;
using System.Diagnostics;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.Core.Listing;
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

    public async Task<StorageResult<S3Object>> PutObjectAsync(string bucketName, string key, PipeReader dataReader, PutObjectRequest? request = null, byte[]? expectedMd5 = null, CancellationToken cancellationToken = default)
    {
        return await PutObjectAsync(bucketName, key, dataReader, null, request, expectedMd5, cancellationToken);
    }

    public async Task<StorageResult<S3Object>> PutObjectAsync(string bucketName, string key, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, PutObjectRequest? request = null, byte[]? expectedMd5 = null, CancellationToken cancellationToken = default)
    {
        try
        {
            // Create ChecksumRequest from PutObjectRequest
            ChecksumRequest? checksumRequest = null;
            if (request != null)
            {
                checksumRequest = new ChecksumRequest
                {
                    Algorithm = request.ChecksumAlgorithm,
                    ProvidedChecksums = new Dictionary<string, string>()
                };

                if (!string.IsNullOrEmpty(request.ChecksumCRC32))
                    checksumRequest.ProvidedChecksums["CRC32"] = request.ChecksumCRC32;
                if (!string.IsNullOrEmpty(request.ChecksumCRC32C))
                    checksumRequest.ProvidedChecksums["CRC32C"] = request.ChecksumCRC32C;
                if (!string.IsNullOrEmpty(request.ChecksumCRC64NVME))
                    checksumRequest.ProvidedChecksums["CRC64NVME"] = request.ChecksumCRC64NVME;
                if (!string.IsNullOrEmpty(request.ChecksumSHA1))
                    checksumRequest.ProvidedChecksums["SHA1"] = request.ChecksumSHA1;
                if (!string.IsNullOrEmpty(request.ChecksumSHA256))
                    checksumRequest.ProvidedChecksums["SHA256"] = request.ChecksumSHA256;

                // Pre-register any algorithms the client will deliver via chunked trailer so the
                // streaming calculator activates their hash state from byte 0. The actual trailer
                // value is merged into the calculator inside the storage chunked path.
                if (request.ExpectedChecksumTrailers.Count > 0)
                {
                    checksumRequest = TrailerChecksumMerger.RegisterExpectedTrailers(request.ExpectedChecksumTrailers, checksumRequest);
                }
            }

            // Phase 1: Prepare data (process to temp storage, not yet visible)
            var prepareResult = await _dataStorage.PrepareDataAsync(bucketName, key, dataReader, chunkValidator, checksumRequest, expectedMd5, cancellationToken);

            if (!prepareResult.IsSuccess)
            {
                _logger.LogWarning("Failed to prepare object {Key} in bucket {BucketName}: {ErrorCode} - {ErrorMessage}",
                    key, bucketName, prepareResult.ErrorCode, prepareResult.ErrorMessage);
                return StorageResult<S3Object>.Error(prepareResult.ErrorCode!, prepareResult.ErrorMessage!);
            }

            using var preparedData = prepareResult.Value!;
            var size = preparedData.Size;
            var etag = preparedData.ETag;
            var checksums = preparedData.Checksums;

            if (ShouldStoreMetadata(key, request))
            {
                // Xattr backends bind metadata to the data file, so commit must happen first.
                if (_metadataStorage is IRequiresDataFileForMetadata)
                {
                    await _dataStorage.CommitPreparedDataAsync(preparedData, cancellationToken);

                    var s3Object = await _metadataStorage.StoreMetadataAsync(bucketName, key, etag, size, request, checksums, DateTime.UtcNow, cancellationToken);
                    if (s3Object == null)
                    {
                        await _dataStorage.DeleteDataAsync(bucketName, key, cancellationToken);
                        _logger.LogError("Failed to store metadata for object {Key} in bucket {BucketName}", key, bucketName);
                        return StorageResult<S3Object>.Error("InternalError", "Failed to store metadata");
                    }

                    return StorageResult<S3Object>.Success(s3Object);
                }

                // Default: metadata-before-data. Pass explicit LastModified — FileInfo on the
                // yet-uncommitted path returns Windows epoch (1601-01-01 UTC), which would make
                // GET stale-detect and trigger a full ETag recompute after commit.
                var s3ObjectDefault = await _metadataStorage.StoreMetadataAsync(bucketName, key, etag, size, request, checksums, DateTime.UtcNow, cancellationToken);

                if (s3ObjectDefault == null)
                {
                    await _dataStorage.AbortPreparedDataAsync(preparedData, cancellationToken);
                    _logger.LogError("Failed to store metadata for object {Key} in bucket {BucketName}", key, bucketName);
                    return StorageResult<S3Object>.Error("InternalError", "Failed to store metadata");
                }

                try
                {
                    await _dataStorage.CommitPreparedDataAsync(preparedData, cancellationToken);
                }
                catch (Exception ex)
                {
                    await _metadataStorage.DeleteMetadataAsync(bucketName, key, cancellationToken);
                    _logger.LogError(ex, "Failed to commit data for object {Key} in bucket {BucketName}", key, bucketName);
                    return StorageResult<S3Object>.Error("InternalError", "Failed to commit data");
                }

                return StorageResult<S3Object>.Success(s3ObjectDefault);
            }
            else
            {
                // No metadata to store — commit data directly
                await _dataStorage.CommitPreparedDataAsync(preparedData, cancellationToken);

                var dataPath = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
                if (dataPath == null)
                {
                    _logger.LogError("Data was stored but cannot be retrieved for object {Key} in bucket {BucketName}", key, bucketName);
                    return StorageResult<S3Object>.Error("InternalError", "Failed to retrieve stored data");
                }

                var contentType = GetContentTypeFromKey(key);
                var s3Object = new S3Object
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

                if (checksums.TryGetValue("CRC32", out var crc32))
                    s3Object.ChecksumCRC32 = crc32;
                if (checksums.TryGetValue("CRC32C", out var crc32c))
                    s3Object.ChecksumCRC32C = crc32c;
                if (checksums.TryGetValue("CRC64NVME", out var crc64nvme))
                    s3Object.ChecksumCRC64NVME = crc64nvme;
                if (checksums.TryGetValue("SHA1", out var sha1))
                    s3Object.ChecksumSHA1 = sha1;
                if (checksums.TryGetValue("SHA256", out var sha256))
                    s3Object.ChecksumSHA256 = sha256;

                return StorageResult<S3Object>.Success(s3Object);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error storing object {Key} in bucket {BucketName} with chunk validation", key, bucketName);
            // PreparedData cleanup is handled by using/Dispose
            throw;
        }
    }

    public async Task<bool> WriteObjectToStreamAsync(string bucketName, string key, PipeWriter writer, long? byteRangeStart = null, long? byteRangeEnd = null, CancellationToken cancellationToken = default)
    {
        return await _dataStorage.WriteDataToPipeAsync(bucketName, key, writer, byteRangeStart, byteRangeEnd, cancellationToken);
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
        cancellationToken.ThrowIfCancellationRequested();
        request ??= new ListObjectsRequest();
        if (request.MaxKeys < 0)
            return StorageResult<ListObjectsResponse>.Error("InvalidArgument", "max-keys must not be negative.");

        var bucket = await _bucketStorage.GetBucketAsync(bucketName, cancellationToken);
        var bucketType = bucket?.Type ?? BucketType.GeneralPurpose;
        var isDirectory = bucketType == BucketType.Directory;
        var isV2 = request.ListType == 2;
        var validationError = ValidateDirectoryBucketListRequest(bucketType, request.Prefix, request.Delimiter);
        if (validationError != null)
            return StorageResult<ListObjectsResponse>.Error("InvalidArgument", validationError);

        ListingPosition? after = null;
        if (isDirectory)
        {
            if (!string.IsNullOrEmpty(request.StartAfter))
                return StorageResult<ListObjectsResponse>.Error("InvalidArgument", "Directory buckets do not support start-after; use a continuation token.");
            if (!string.IsNullOrEmpty(request.ContinuationToken)
                && !DirectoryListingToken.TryDecode(request.ContinuationToken, bucketName, request.Prefix, request.Delimiter, out after))
                return StorageResult<ListObjectsResponse>.Error("InvalidArgument", "Invalid Directory continuation token or query context. Restart listing without a token.");
        }
        else
        {
            var key = isV2
                ? (!string.IsNullOrEmpty(request.ContinuationToken)
                    ? ContinuationToken.Decode(request.ContinuationToken) ?? request.ContinuationToken
                    : request.StartAfter)
                : request.ContinuationToken;
            if (!string.IsNullOrEmpty(key))
                after = new ListingPosition(ListingOrder.Lexicographical, key);
        }

        var query = new ListingQuery(bucketType, request.Prefix, request.Delimiter, after, request.MaxKeys);
        var response = new ListObjectsResponse
        {
            Prefix = request.Prefix,
            Delimiter = request.Delimiter,
            MaxKeys = query.MaxKeys
        };
        var statistics = new ListingStatistics();
        var started = Stopwatch.GetTimestamp();
        var scanElapsed = TimeSpan.Zero;
        var metadataElapsed = TimeSpan.Zero;
        var outcome = "completed";
        try
        {
            if (query.MaxKeys == 0)
                return StorageResult<ListObjectsResponse>.Success(response);

            var data = await _dataStorage.ListDataCandidatesAsync(bucketName, query, cancellationToken);
            statistics = data.Statistics;
            var selector = new ListingPageSelector(query, statistics);
            foreach (var entry in data.Entries)
                selector.ConsiderEntry(entry.Name, entry.IsCommonPrefix);

            if (isDirectory && query.Delimiter != null)
            {
                await foreach (var key in _multipartUploadStorage.EnumerateUploadKeysAsync(bucketName, cancellationToken))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    statistics.MultipartUploads++;
                    if (_dataStorage is IObjectListingFilter filter && !filter.IsVisibleInListing(key))
                    {
                        statistics.ExcludedEntries++;
                        continue;
                    }
                    selector.ConsiderKey(key, prefixesOnly: true);
                }
            }

            var combined = selector.Finish();
            var page = combined.Entries.Take(query.MaxKeys).ToArray();
            response.IsTruncated = combined.Entries.Count > query.MaxKeys;
            if (response.IsTruncated)
            {
                var last = page[^1].Name;
                response.NextContinuationToken = isDirectory
                    ? DirectoryListingToken.Encode(bucketName, query, last)
                    : (isV2 ? ContinuationToken.Encode(last) : last);
            }
            foreach (var entry in page)
                if (entry.IsCommonPrefix)
                    response.CommonPrefixes.Add(entry.Name);
            var keys = page.Where(e => !e.IsCommonPrefix).Select(e => e.Name).ToArray();
            scanElapsed = Stopwatch.GetElapsedTime(started);
            var metadataStarted = Stopwatch.GetTimestamp();

            Dictionary<string, S3ObjectInfo?>? batch = null;
            if (keys.Length > 0 && _metadataStorage is IBatchObjectMetadataStorage batchStorage)
                batch = await batchStorage.GetMetadataBatchAsync(bucketName, keys, cancellationToken);
            foreach (var key in keys)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var meta = batch != null ? batch.GetValueOrDefault(key)
                        : await _metadataStorage.GetMetadataAsync(bucketName, key, cancellationToken);
                    if (meta == null)
                    {
                        var info = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
                        if (info != null)
                            meta = await GenerateMetadataOnTheFlyAsync(bucketName, key, info.Value.size, info.Value.lastModified, cancellationToken);
                    }
                    if (meta != null)
                        response.Contents.Add(meta);
                }
                catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
                {
                    // Data can disappear between selecting a page and reading its metadata.
                    // Keep the selected boundary so pagination still makes progress.
                }
            }
            metadataElapsed = Stopwatch.GetElapsedTime(metadataStarted);
            cancellationToken.ThrowIfCancellationRequested();
            return StorageResult<ListObjectsResponse>.Success(response);
        }
        catch (OperationCanceledException)
        {
            outcome = "cancelled";
            throw;
        }
        catch
        {
            outcome = "error";
            throw;
        }
        finally
        {
            _logger.LogDebug(
                "Object listing {Outcome}: order={Order} scanned={Scanned} excluded={Excluded} excludedSubtrees={ExcludedSubtrees} uploads={Uploads} peakCandidatesPerSelector={PeakCandidates} objects={Objects} prefixes={Prefixes} scanMs={ScanMs} metadataMs={MetadataMs} totalMs={TotalMs}",
                outcome, query.Order, statistics.ScannedEntries, statistics.ExcludedEntries, statistics.ExcludedSubtrees,
                statistics.MultipartUploads, statistics.PeakCandidates, response.Contents.Count, response.CommonPrefixes.Count,
                scanElapsed.TotalMilliseconds, metadataElapsed.TotalMilliseconds, Stopwatch.GetElapsedTime(started).TotalMilliseconds);
        }
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

        // Check if there are any checksums that need to be persisted
        if (!string.IsNullOrEmpty(request.ChecksumCRC32) ||
            !string.IsNullOrEmpty(request.ChecksumCRC32C) ||
            !string.IsNullOrEmpty(request.ChecksumCRC64NVME) ||
            !string.IsNullOrEmpty(request.ChecksumSHA1) ||
            !string.IsNullOrEmpty(request.ChecksumSHA256) ||
            !string.IsNullOrEmpty(request.ChecksumAlgorithm))
        {
            return true; // Checksums present, store metadata to preserve them
        }

        // Check if there are any tags that need to be persisted
        if (request.Tags is { Count: > 0 })
        {
            return true; // Tags present, store metadata to preserve them
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
        string? taggingDirective = null,
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

            // Phase 1: Prepare copy (temp file, not yet visible)
            using var preparedData = await _dataStorage.PrepareCopyDataAsync(sourceBucketName, sourceKey, destBucketName, destKey, cancellationToken);
            if (preparedData == null)
            {
                _logger.LogError("Failed to copy data from {SourceBucket}/{SourceKey} to {DestBucket}/{DestKey}",
                    sourceBucketName, sourceKey, destBucketName, destKey);
                return null;
            }

            var size = preparedData.Size;
            var etag = preparedData.ETag;

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
                // COPY mode: use source object's metadata and checksums
                effectiveRequest = new PutObjectRequest
                {
                    Key = destKey,
                    ContentType = sourceInfo.ContentType,
                    Metadata = new Dictionary<string, string>(sourceInfo.Metadata),
                    OwnerId = request?.OwnerId ?? sourceInfo.OwnerId,
                    OwnerDisplayName = request?.OwnerDisplayName ?? sourceInfo.OwnerDisplayName,
                    ChecksumCRC32 = sourceInfo.ChecksumCRC32,
                    ChecksumCRC32C = sourceInfo.ChecksumCRC32C,
                    ChecksumSHA1 = sourceInfo.ChecksumSHA1,
                    ChecksumSHA256 = sourceInfo.ChecksumSHA256,
                    ChecksumCRC64NVME = sourceInfo.ChecksumCRC64NVME
                };
            }

            // Tagging directive: default COPY per S3 spec. REPLACE uses request.Tags.
            var tagDirective = taggingDirective?.ToUpperInvariant() ?? "COPY";
            if (tagDirective == "REPLACE")
            {
                effectiveRequest.Tags = request?.Tags ?? new Dictionary<string, string>();
            }
            else
            {
                effectiveRequest.Tags = new Dictionary<string, string>(sourceInfo.Tags);
            }

            if (ShouldStoreMetadata(destKey, effectiveRequest))
            {
                if (_metadataStorage is IRequiresDataFileForMetadata)
                {
                    await _dataStorage.CommitPreparedDataAsync(preparedData, cancellationToken);

                    var s3Object = await _metadataStorage.StoreMetadataAsync(destBucketName, destKey, etag, size, effectiveRequest, null, DateTime.UtcNow, cancellationToken);
                    if (s3Object == null)
                    {
                        await _dataStorage.DeleteDataAsync(destBucketName, destKey, cancellationToken);
                        _logger.LogError("Failed to store metadata for copied object {DestKey} in bucket {DestBucket}", destKey, destBucketName);
                        return null;
                    }

                    return s3Object;
                }

                // Default: metadata-before-data. Pass explicit LastModified — FileInfo on the
                // yet-uncommitted dest path returns Windows epoch, which would cause stale-detect
                // and trigger a full ETag recompute from file bytes after commit.
                var s3ObjectDefault = await _metadataStorage.StoreMetadataAsync(destBucketName, destKey, etag, size, effectiveRequest, null, DateTime.UtcNow, cancellationToken);

                if (s3ObjectDefault == null)
                {
                    await _dataStorage.AbortPreparedDataAsync(preparedData, cancellationToken);
                    _logger.LogError("Failed to store metadata for copied object {DestKey} in bucket {DestBucket}", destKey, destBucketName);
                    return null;
                }

                try
                {
                    await _dataStorage.CommitPreparedDataAsync(preparedData, cancellationToken);
                }
                catch (Exception ex)
                {
                    await _metadataStorage.DeleteMetadataAsync(destBucketName, destKey, cancellationToken);
                    _logger.LogError(ex, "Failed to commit copied data for object {DestKey} in bucket {DestBucket}", destKey, destBucketName);
                    return null;
                }

                return s3ObjectDefault;
            }
            else
            {
                // No metadata — commit data directly
                await _dataStorage.CommitPreparedDataAsync(preparedData, cancellationToken);

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

    public async Task<UploadPart?> CopyObjectPartAsync(
        string sourceBucketName,
        string sourceKey,
        string destBucketName,
        string destKey,
        string uploadId,
        int partNumber,
        long? byteRangeStart = null,
        long? byteRangeEnd = null,
        ChecksumRequest? checksumRequest = null,
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

            // Check if source object exists and get its info
            var sourceInfo = await GetObjectInfoAsync(sourceBucketName, sourceKey, cancellationToken);
            if (sourceInfo == null)
            {
                _logger.LogWarning("Source object {SourceKey} not found in bucket {SourceBucket}", sourceKey, sourceBucketName);
                return null;
            }

            // Validate byte range
            if (byteRangeStart.HasValue || byteRangeEnd.HasValue)
            {
                long startByte = byteRangeStart ?? 0;
                long endByte = byteRangeEnd ?? (sourceInfo.Size - 1);

                if (startByte < 0 || endByte >= sourceInfo.Size || startByte > endByte)
                {
                    _logger.LogWarning("Invalid byte range: {Start}-{End} for object size {Size}",
                        startByte, endByte, sourceInfo.Size);
                    return null;
                }
            }

            // Create a pipe to stream data from source to destination
            var pipe = new Pipe();

            // Start a background task to write source data (or byte range) to the pipe
            var writeTask = Task.Run(async () =>
            {
                try
                {
                    await _dataStorage.WriteDataToPipeAsync(sourceBucketName, sourceKey, pipe.Writer, byteRangeStart, byteRangeEnd, cancellationToken);
                    await pipe.Writer.CompleteAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error writing source data to pipe for {SourceBucket}/{SourceKey}",
                        sourceBucketName, sourceKey);
                    await pipe.Writer.CompleteAsync(ex);
                }
            }, cancellationToken);

            try
            {
                // Upload the data (or byte range) as a multipart part
                var uploadPartResult = await _multipartUploadStorage.UploadPartAsync(
                    destBucketName, destKey, uploadId, partNumber, pipe.Reader, checksumRequest, expectedMd5: null, cancellationToken);

                // Wait for write task to complete
                await writeTask;

                _logger.LogInformation("Copied part {PartNumber} from {SourceBucket}/{SourceKey} (bytes {Start}-{End}) to {DestBucket}/{DestKey} upload {UploadId}",
                    partNumber, sourceBucketName, sourceKey, byteRangeStart ?? 0, byteRangeEnd ?? (sourceInfo.Size - 1), destBucketName, destKey, uploadId);

                return uploadPartResult.IsSuccess ? uploadPartResult.Value : null;
            }
            finally
            {
                // Ensure reader is completed
                await pipe.Reader.CompleteAsync();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error copying object part from {SourceBucket}/{SourceKey} to {DestBucket}/{DestKey} part {PartNumber}",
                sourceBucketName, sourceKey, destBucketName, destKey, partNumber);
            return null;
        }
    }

    public Task<Dictionary<string, string>?> GetObjectTagsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        return _metadataStorage.GetObjectTagsAsync(bucketName, key, cancellationToken);
    }

    public Task<bool> SetObjectTagsAsync(string bucketName, string key, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        return _metadataStorage.SetObjectTagsAsync(bucketName, key, tags, cancellationToken);
    }

    public Task<bool> DeleteObjectTagsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        return _metadataStorage.DeleteObjectTagsAsync(bucketName, key, cancellationToken);
    }
}
