using System.IO.Pipelines;
using System.Text.Json;
using System.Xml.Serialization;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.WebApi.Authorization;
using Lamina.WebApi.Controllers.Attributes;
using Lamina.WebApi.Controllers.Base;
using Lamina.WebApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace Lamina.WebApi.Controllers;

[Route("{bucketName}")]
public class S3ObjectsController : S3ControllerBase
{
    private readonly IObjectStorageFacade _objectStorage;
    private readonly IBucketStorageFacade _bucketStorage;
    private readonly IAuthenticationService _authService;
    private readonly ILogger<S3ObjectsController> _logger;

    public S3ObjectsController(
        IObjectStorageFacade objectStorage,
        IBucketStorageFacade bucketStorage,
        IAuthenticationService authService,
        ILogger<S3ObjectsController> logger
    )
    {
        _objectStorage = objectStorage;
        _bucketStorage = bucketStorage;
        _authService = authService;
        _logger = logger;
    }

    [HttpGet("")]
    [RequireNoQueryParameters("location", "uploads", "versioning")]
    [S3Authorize(S3Operations.List, S3ResourceType.Object)]
    public async Task<IActionResult> ListObjects(
        string bucketName,
        [FromQuery(Name = "list-type")] int? listType,
        [FromQuery] string? prefix,
        [FromQuery] string? delimiter,
        [FromQuery(Name = "max-keys")] int? maxKeys,
        [FromQuery] string? marker,
        [FromQuery(Name = "continuation-token")]
        string? continuationToken,
        [FromQuery(Name = "start-after")] string? startAfter,
        [FromQuery(Name = "encoding-type")] string? encodingType,
        [FromQuery(Name = "fetch-owner")] bool fetchOwner = false,
        CancellationToken cancellationToken = default
    )
    {
        var exists = await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken);
        if (!exists)
        {
            return S3Error("NoSuchBucket", "The specified bucket does not exist", bucketName, 404);
        }

        // Determine API version: listType=2 means ListObjectsV2, otherwise ListObjects V1
        var isV2 = listType == 2;

        // Validate encoding-type parameter
        if (!string.IsNullOrEmpty(encodingType) && !encodingType.Equals("url", StringComparison.OrdinalIgnoreCase))
        {
            return S3Error("InvalidArgument",
                $"Invalid Encoding Method specified in Request. The encoding-type parameter value '{encodingType}' is invalid. Valid value is 'url'.",
                $"/{bucketName}",
                400);
        }

        // Get objects from service
        var listRequest = new ListObjectsRequest
        {
            Prefix = prefix,
            Delimiter = delimiter,
            MaxKeys = maxKeys ?? 1000,
            ContinuationToken = continuationToken ?? marker,
            ListType = listType ?? 1,
            StartAfter = startAfter,
            FetchOwner = fetchOwner,
            EncodingType = encodingType
        };

        var result = await _objectStorage.ListObjectsAsync(bucketName, listRequest, cancellationToken);

        if (!result.IsSuccess)
        {
            // Handle validation errors from the facade (e.g., Directory bucket constraints)
            _logger.LogWarning("ListObjects call failed for bucket {BucketName} ({ErrorCode}): {ErrorMessage}", bucketName, result.ErrorCode!, result.ErrorMessage!);
            return S3Error(result.ErrorCode!, result.ErrorMessage!, $"/{bucketName}", 400);
        }

        var objects = result.Value!;

        // Get bucket information to check if it's a Directory bucket for response ordering
        var bucket = await _bucketStorage.GetBucketAsync(bucketName, cancellationToken);
        var isDirectoryBucket = bucket?.Type == BucketType.Directory;

        // For Directory buckets, objects should not be in lexicographical order
        if (isDirectoryBucket)
        {
            // Randomize the order to simulate non-lexicographical ordering
            objects.Contents = objects.Contents.OrderBy(x => Random.Shared.Next()).ToList();
        }

        Response.ContentType = "application/xml";

        if (isV2)
        {
            // ListObjectsV2 response
            var resultV2 = new ListBucketResultV2
            {
                Name = bucketName,
                Prefix = S3UrlEncoder.ConditionalEncode(prefix, encodingType),
                StartAfter = S3UrlEncoder.ConditionalEncode(startAfter, encodingType),
                ContinuationToken = continuationToken,
                NextContinuationToken = objects.IsTruncated ? objects.NextContinuationToken : null,
                KeyCount = objects.Contents.Count,
                MaxKeys = maxKeys ?? 1000,
                EncodingType = encodingType,
                IsTruncated = objects.IsTruncated,
                ContentsList = objects.Contents.Select(o => new Contents
                {
                    Key = S3UrlEncoder.ConditionalEncode(o.Key, encodingType) ?? o.Key,
                    LastModified = o.LastModified.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'"),
                    ETag = $"\"{o.ETag}\"",
                    Size = o.Size,
                    StorageClass = "STANDARD",
                    Owner = fetchOwner && o.OwnerId != null ? new Owner(o.OwnerId, o.OwnerDisplayName ?? o.OwnerId) : null // Only include owner if requested in V2
                }).ToList(),
                CommonPrefixesList = objects.CommonPrefixes.Select(cp => new CommonPrefixes
                {
                    Prefix = S3UrlEncoder.ConditionalEncode(cp, encodingType) ?? cp
                }).ToList()
            };
            return Ok(resultV2);
        }
        else
        {
            // ListObjects V1 response
            var listResult = new ListBucketResult
            {
                Name = bucketName,
                Prefix = S3UrlEncoder.ConditionalEncode(prefix, encodingType),
                Marker = S3UrlEncoder.ConditionalEncode(marker, encodingType),
                MaxKeys = maxKeys ?? 1000,
                EncodingType = encodingType,
                IsTruncated = objects.IsTruncated,
                NextMarker = objects.IsTruncated ? S3UrlEncoder.ConditionalEncode(objects.NextContinuationToken, encodingType) : null,
                ContentsList = objects.Contents.Select(o => new Contents
                {
                    Key = S3UrlEncoder.ConditionalEncode(o.Key, encodingType) ?? o.Key,
                    LastModified = o.LastModified.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'"),
                    ETag = $"\"{o.ETag}\"",
                    Size = o.Size,
                    StorageClass = "STANDARD",
                    Owner = new Owner(o.OwnerId ?? "anonymous", o.OwnerDisplayName ?? "anonymous") // Always include owner in V1
                }).ToList(),
                CommonPrefixesList = objects.CommonPrefixes.Select(cp => new CommonPrefixes
                {
                    Prefix = S3UrlEncoder.ConditionalEncode(cp, encodingType) ?? cp
                }).ToList()
            };
            return Ok(listResult);
        }
    }

    [HttpPut("{*key}")]
    [RequireNoQueryParameters("partNumber", "uploadId")]
    [DisableRequestSizeLimit]
    [S3Authorize(S3Operations.Write, S3ResourceType.Object)]
    public async Task<IActionResult> PutObject(
        string bucketName,
        string key,
        CancellationToken cancellationToken = default
    )
    {
        // Check if this is a copy operation (presence of x-amz-copy-source header)
        if (Request.Headers.TryGetValue("x-amz-copy-source", out var copySourceHeader) &&
            !string.IsNullOrEmpty(copySourceHeader.ToString()))
        {
            // Handle CopyObject operation
            return await HandleCopyObject(bucketName, key, copySourceHeader.ToString(), cancellationToken);
        }

        // Handle regular PutObject operation
        // Log comprehensive request headers for diagnostics
        LogUploadRequestHeaders(_logger, "PutObject", bucketName, key);

        // Validate Content-Length header (required by S3 API)
        var contentLengthError = ValidateContentLengthHeader($"/{bucketName}/{key}");
        if (contentLengthError != null)
        {
            _logger.LogWarning("PutObject request missing Content-Length header for key {Key} in bucket {BucketName}", key, bucketName);
            return contentLengthError;
        }

        // Validate x-amz-content-sha256 header when using AWS Signature V4
        var contentSha256Error = ValidateContentSha256Header($"/{bucketName}/{key}");
        if (contentSha256Error != null)
        {
            _logger.LogWarning("PutObject request has invalid or missing x-amz-content-sha256 header for key {Key} in bucket {BucketName}", key, bucketName);
            return contentSha256Error;
        }

        // Validate the object key
        if (!_objectStorage.IsValidObjectKey(key))
        {
            return S3Error("InvalidObjectName", "Object key forbidden", $"/{bucketName}/{key}", 400);
        }

        if (!await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return S3Error("NoSuchBucket", "The specified bucket does not exist", bucketName, 404);
        }

        var contentType = Request.ContentType;

        // Get authenticated user from claims
        var authenticatedUser = GetS3UserFromClaims();

        // Parse checksum headers
        Request.Headers.TryGetValue("x-amz-checksum-algorithm", out var checksumAlgorithm);
        Request.Headers.TryGetValue("x-amz-checksum-crc32", out var checksumCrc32);
        Request.Headers.TryGetValue("x-amz-checksum-crc32c", out var checksumCrc32c);
        Request.Headers.TryGetValue("x-amz-checksum-sha1", out var checksumSha1);
        Request.Headers.TryGetValue("x-amz-checksum-sha256", out var checksumSha256);
        Request.Headers.TryGetValue("x-amz-checksum-crc64nvme", out var checksumCrc64nvme);
        
        var checksumAlgorithmValue = checksumAlgorithm.ToString();
        
        // Validate checksum algorithm if provided
        if (!string.IsNullOrEmpty(checksumAlgorithmValue) && !StreamingChecksumCalculator.IsValidAlgorithm(checksumAlgorithmValue))
        {
            return S3Error("InvalidArgument", $"Invalid checksum algorithm: {checksumAlgorithmValue}. Valid values are: CRC32, CRC32C, SHA1, SHA256, CRC64NVME", $"/{bucketName}/{key}", 400);
        }

        var putRequest = new PutObjectRequest
        {
            Key = key,
            ContentType = contentType,
            Metadata = Request.Headers
                .Where(h => h.Key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
                .ToDictionary(h => h.Key[11..], h => h.Value.ToString()),
            OwnerId = authenticatedUser?.AccessKeyId ?? "anonymous",
            OwnerDisplayName = authenticatedUser?.Name ?? "anonymous",
            ChecksumAlgorithm = string.IsNullOrEmpty(checksumAlgorithmValue) ? null : checksumAlgorithmValue,
            ChecksumCRC32 = string.IsNullOrEmpty(checksumCrc32.ToString()) ? null : checksumCrc32.ToString(),
            ChecksumCRC32C = string.IsNullOrEmpty(checksumCrc32c.ToString()) ? null : checksumCrc32c.ToString(),
            ChecksumSHA1 = string.IsNullOrEmpty(checksumSha1.ToString()) ? null : checksumSha1.ToString(),
            ChecksumSHA256 = string.IsNullOrEmpty(checksumSha256.ToString()) ? null : checksumSha256.ToString(),
            ChecksumCRC64NVME = string.IsNullOrEmpty(checksumCrc64nvme.ToString()) ? null : checksumCrc64nvme.ToString()
        };

        // Use PipeReader from the request body
        var reader = Request.BodyReader;

        _logger.LogInformation("Putting object {Key} in bucket {BucketName}", key, bucketName);

        // Check if there's a chunk validator from the authentication middleware
        var chunkValidator = HttpContext.Items["ChunkValidator"] as IChunkSignatureValidator;

        var storeResult = chunkValidator != null
            ? await _objectStorage.PutObjectAsync(bucketName, key, reader, chunkValidator, putRequest, cancellationToken)
            : await _objectStorage.PutObjectAsync(bucketName, key, reader, putRequest, cancellationToken);

        if (!storeResult.IsSuccess)
        {
            // Handle specific error cases
            if (storeResult.ErrorCode == "InvalidChecksum")
            {
                _logger.LogWarning("Checksum validation failed for object {Key} in bucket {BucketName}: {ErrorMessage}",
                    key, bucketName, storeResult.ErrorMessage);
                return S3Error("InvalidChecksum", storeResult.ErrorMessage!, $"/{bucketName}/{key}", 400);
            }

            // If we had a chunk validator and the operation failed, it's likely due to invalid signature
            if (chunkValidator != null)
            {
                _logger.LogWarning("Chunk signature validation failed for object {Key} in bucket {BucketName}", key, bucketName);
                return S3Error("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", $"/{bucketName}/{key}", 403);
            }

            _logger.LogError("Failed to put object {Key} in bucket {BucketName}: {ErrorCode} - {ErrorMessage}",
                key, bucketName, storeResult.ErrorCode, storeResult.ErrorMessage);
            return StatusCode(500);
        }

        var s3Object = storeResult.Value!;

        _logger.LogInformation("Object {Key} stored successfully in bucket {BucketName}, ETag: {ETag}, Size: {Size}", key, bucketName, s3Object.ETag, s3Object.Size);
        Response.Headers.Append("ETag", $"\"{s3Object.ETag}\"");
        Response.Headers.Append("x-amz-version-id", "null");
        
        // Add checksum headers if present
        if (!string.IsNullOrEmpty(s3Object.ChecksumCRC32))
            Response.Headers.Append("x-amz-checksum-crc32", s3Object.ChecksumCRC32);
        if (!string.IsNullOrEmpty(s3Object.ChecksumCRC32C))
            Response.Headers.Append("x-amz-checksum-crc32c", s3Object.ChecksumCRC32C);
        if (!string.IsNullOrEmpty(s3Object.ChecksumSHA1))
            Response.Headers.Append("x-amz-checksum-sha1", s3Object.ChecksumSHA1);
        if (!string.IsNullOrEmpty(s3Object.ChecksumSHA256))
            Response.Headers.Append("x-amz-checksum-sha256", s3Object.ChecksumSHA256);
        if (!string.IsNullOrEmpty(s3Object.ChecksumCRC64NVME))
            Response.Headers.Append("x-amz-checksum-crc64nvme", s3Object.ChecksumCRC64NVME);
        
        return Ok();
    }

    private async Task<IActionResult> HandleCopyObject(
        string bucketName,
        string key,
        string copySourceHeader,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Copying object to {Key} in bucket {BucketName} from {CopySource}", key, bucketName, copySourceHeader);

        // Validate the destination object key
        if (!_objectStorage.IsValidObjectKey(key))
        {
            return S3Error("InvalidObjectName", "Object key forbidden", $"/{bucketName}/{key}", 400);
        }

        // Check if destination bucket exists
        if (!await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return S3Error("NoSuchBucket", "The specified bucket does not exist", bucketName, 404);
        }

        // Parse the copy source header: format is /source-bucket/source-key or source-bucket/source-key
        var copySource = copySourceHeader.TrimStart('/');
        var firstSlash = copySource.IndexOf('/');
        if (firstSlash <= 0)
        {
            return S3Error("InvalidArgument", "Copy Source must be in the format /source-bucket/source-key", $"/{bucketName}/{key}", 400);
        }

        var sourceBucketName = copySource.Substring(0, firstSlash);
        var sourceKey = Uri.UnescapeDataString(copySource.Substring(firstSlash + 1));

        // Validate the source object key
        if (!_objectStorage.IsValidObjectKey(sourceKey))
        {
            return S3Error("InvalidObjectName", "Source object key forbidden", $"/{sourceBucketName}/{sourceKey}", 400);
        }

        // Check if source bucket exists
        if (!await _bucketStorage.BucketExistsAsync(sourceBucketName, cancellationToken))
        {
            return S3Error("NoSuchBucket", "The specified source bucket does not exist", sourceBucketName, 404);
        }

        // Get metadata directive (COPY or REPLACE)
        Request.Headers.TryGetValue("x-amz-metadata-directive", out var metadataDirective);

        // Get authenticated user from claims
        var authenticatedUser = GetS3UserFromClaims();

        // Build request for new metadata if REPLACE directive
        PutObjectRequest? putRequest = null;
        if (metadataDirective.ToString().Equals("REPLACE", StringComparison.OrdinalIgnoreCase))
        {
            var contentType = Request.Headers.ContentType.ToString();
            putRequest = new PutObjectRequest
            {
                Key = key,
                ContentType = contentType,
                Metadata = Request.Headers
                    .Where(h => h.Key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
                    .ToDictionary(h => h.Key[11..], h => h.Value.ToString()),
                OwnerId = authenticatedUser?.AccessKeyId ?? "anonymous",
                OwnerDisplayName = authenticatedUser?.Name ?? "anonymous"
            };
        }
        else
        {
            // For COPY mode, still pass owner info
            putRequest = new PutObjectRequest
            {
                Key = key,
                OwnerId = authenticatedUser?.AccessKeyId ?? "anonymous",
                OwnerDisplayName = authenticatedUser?.Name ?? "anonymous"
            };
        }

        // Perform the copy
        var copiedObject = await _objectStorage.CopyObjectAsync(
            sourceBucketName,
            sourceKey,
            bucketName,
            key,
            metadataDirective.ToString(),
            putRequest,
            cancellationToken
        );

        if (copiedObject == null)
        {
            _logger.LogError("Failed to copy object from {SourceBucket}/{SourceKey} to {DestBucket}/{DestKey}",
                sourceBucketName, sourceKey, bucketName, key);
            return S3Error("NoSuchKey", "The specified source key does not exist.", $"/{sourceBucketName}/{sourceKey}", 404);
        }

        _logger.LogInformation("Object copied successfully from {SourceBucket}/{SourceKey} to {DestBucket}/{DestKey}, ETag: {ETag}",
            sourceBucketName, sourceKey, bucketName, key, copiedObject.ETag);

        // Return CopyObjectResult XML response
        var result = new CopyObjectResult
        {
            ETag = $"\"{copiedObject.ETag}\"",
            LastModified = copiedObject.LastModified.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'"),
            ChecksumCRC32 = copiedObject.ChecksumCRC32,
            ChecksumCRC32C = copiedObject.ChecksumCRC32C,
            ChecksumCRC64NVME = copiedObject.ChecksumCRC64NVME,
            ChecksumSHA1 = copiedObject.ChecksumSHA1,
            ChecksumSHA256 = copiedObject.ChecksumSHA256
        };

        Response.ContentType = "application/xml";
        Response.Headers.Append("x-amz-version-id", "null");
        return Ok(result);
    }

    [HttpGet("{*key}")]
    [RequireNoQueryParameters("uploadId")]
    [S3Authorize(S3Operations.Read, S3ResourceType.Object)]
    public async Task<IActionResult> GetObject(
        string bucketName,
        string key,
        CancellationToken cancellationToken = default
    )
    {
        _logger.LogInformation("Getting object {Key} from bucket {BucketName}", key, bucketName);

        var metadata = await _objectStorage.GetObjectInfoAsync(bucketName, key, cancellationToken);
        if (metadata == null)
        {
            _logger.LogWarning("Object not found: {Key} in bucket {BucketName}", key, bucketName);
            return S3Error("NoSuchKey", "The specified key does not exist.", $"{bucketName}/{key}", 404);
        }

        // Parse Range header if present
        long? byteRangeStart = null;
        long? byteRangeEnd = null;
        bool isRangeRequest = false;

        if (Request.Headers.TryGetValue("Range", out var rangeHeader) && !string.IsNullOrEmpty(rangeHeader.ToString()))
        {
            var rangeStr = rangeHeader.ToString();
            if (rangeStr.StartsWith("bytes=", StringComparison.OrdinalIgnoreCase))
            {
                var bytesRange = rangeStr.Substring(6); // Remove "bytes=" prefix
                var rangeParts = bytesRange.Split('-');

                if (rangeParts.Length == 2)
                {
                    // Parse start and end
                    var startStr = rangeParts[0].Trim();
                    var endStr = rangeParts[1].Trim();

                    if (!string.IsNullOrEmpty(startStr) && long.TryParse(startStr, out var start))
                    {
                        byteRangeStart = start;
                    }

                    if (!string.IsNullOrEmpty(endStr) && long.TryParse(endStr, out var end))
                    {
                        byteRangeEnd = end;
                    }
                    else if (string.IsNullOrEmpty(endStr) && byteRangeStart.HasValue)
                    {
                        // Open-ended range: bytes=100-
                        byteRangeEnd = metadata.Size - 1;
                    }

                    // Handle suffix-byte-range: bytes=-500 (last 500 bytes)
                    if (string.IsNullOrEmpty(startStr) && byteRangeEnd.HasValue)
                    {
                        var suffixLength = byteRangeEnd.Value;
                        byteRangeStart = Math.Max(0, metadata.Size - suffixLength);
                        byteRangeEnd = metadata.Size - 1;
                    }

                    isRangeRequest = true;
                }
            }
        }

        // Validate range
        if (isRangeRequest)
        {
            if (byteRangeStart.HasValue && byteRangeStart.Value >= metadata.Size)
            {
                _logger.LogWarning("Invalid range for object {Key} in bucket {BucketName}: start {Start} >= size {Size}",
                    key, bucketName, byteRangeStart.Value, metadata.Size);
                Response.Headers.Append("Content-Range", $"bytes */{metadata.Size}");
                return S3Error("InvalidRange", "The requested range is not satisfiable", $"/{bucketName}/{key}", 416);
            }

            if (byteRangeStart.HasValue && byteRangeEnd.HasValue && byteRangeStart.Value > byteRangeEnd.Value)
            {
                _logger.LogWarning("Invalid range for object {Key} in bucket {BucketName}: start {Start} > end {End}",
                    key, bucketName, byteRangeStart.Value, byteRangeEnd.Value);
                Response.Headers.Append("Content-Range", $"bytes */{metadata.Size}");
                return S3Error("InvalidRange", "The requested range is not satisfiable", $"/{bucketName}/{key}", 416);
            }

            // Clamp end to object size
            if (byteRangeEnd.HasValue && byteRangeEnd.Value >= metadata.Size)
            {
                byteRangeEnd = metadata.Size - 1;
            }
        }

        // Set response headers
        Response.Headers.Append("ETag", $"\"{metadata.ETag}\"");
        Response.Headers.Append("Last-Modified", metadata.LastModified.ToString("R"));
        Response.Headers.Append("Accept-Ranges", "bytes");

        foreach (var metadataItem in metadata.Metadata)
        {
            Response.Headers.Append($"x-amz-meta-{metadataItem.Key}", metadataItem.Value);
        }

        // Add checksum headers if present
        if (!string.IsNullOrEmpty(metadata.ChecksumCRC32))
            Response.Headers.Append("x-amz-checksum-crc32", metadata.ChecksumCRC32);
        if (!string.IsNullOrEmpty(metadata.ChecksumCRC32C))
            Response.Headers.Append("x-amz-checksum-crc32c", metadata.ChecksumCRC32C);
        if (!string.IsNullOrEmpty(metadata.ChecksumSHA1))
            Response.Headers.Append("x-amz-checksum-sha1", metadata.ChecksumSHA1);
        if (!string.IsNullOrEmpty(metadata.ChecksumSHA256))
            Response.Headers.Append("x-amz-checksum-sha256", metadata.ChecksumSHA256);
        if (!string.IsNullOrEmpty(metadata.ChecksumCRC64NVME))
            Response.Headers.Append("x-amz-checksum-crc64nvme", metadata.ChecksumCRC64NVME);

        // Calculate content length and set status code
        long contentLength;
        if (isRangeRequest && byteRangeStart.HasValue && byteRangeEnd.HasValue)
        {
            contentLength = byteRangeEnd.Value - byteRangeStart.Value + 1;
            Response.Headers.Append("Content-Range", $"bytes {byteRangeStart.Value}-{byteRangeEnd.Value}/{metadata.Size}");
            Response.Headers.Append("Content-Length", contentLength.ToString());
            Response.StatusCode = 206; // Partial Content
            _logger.LogInformation("Returning partial content for {Key} in bucket {BucketName}: bytes {Start}-{End}/{Total}",
                key, bucketName, byteRangeStart.Value, byteRangeEnd.Value, metadata.Size);
        }
        else
        {
            contentLength = metadata.Size;
            Response.Headers.Append("Content-Length", contentLength.ToString());
        }

        // Create a pipe to stream data back
        var pipe = new Pipe();
        var writer = pipe.Writer;

        // Start writing to pipe in background
        _ = Task.Run(async () =>
        {
            try
            {
                await _objectStorage.WriteObjectToStreamAsync(bucketName, key, writer, byteRangeStart, byteRangeEnd, cancellationToken);
                await writer.CompleteAsync();
            }
            catch (Exception ex)
            {
                await writer.CompleteAsync(ex);
            }
        });

        return new FileStreamResult(pipe.Reader.AsStream(), metadata.ContentType);
    }

    [HttpDelete("{*key}")]
    [RequireNoQueryParameters("uploadId")]
    [S3Authorize(S3Operations.Delete, S3ResourceType.Object)]
    public async Task<IActionResult> DeleteObject(
        string bucketName,
        string key,
        CancellationToken cancellationToken = default
    )
    {
        if (!await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            // S3 returns 204 even if bucket doesn't exist for delete operations
            return NoContent();
        }

        _logger.LogInformation("Deleting object {Key} from bucket {BucketName}", key, bucketName);
        await _objectStorage.DeleteObjectAsync(bucketName, key, cancellationToken);
        return NoContent();
    }

    [HttpHead("{*key}")]
    [S3Authorize(S3Operations.Read, S3ResourceType.Object)]
    public async Task<IActionResult> HeadObject(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var objectInfo = await _objectStorage.GetObjectInfoAsync(bucketName, key, cancellationToken);
        if (objectInfo == null)
        {
            Response.StatusCode = 404;
            Response.ContentType = "application/xml";
            return new EmptyResult();
        }

        Response.Headers.Append("ETag", $"\"{objectInfo.ETag}\"");
        Response.Headers.Append("Content-Length", objectInfo.Size.ToString());
        Response.Headers.Append("Content-Type", objectInfo.ContentType);
        Response.Headers.Append("Last-Modified", objectInfo.LastModified.ToString("R"));

        // Add custom metadata headers with x-amz-meta- prefix
        foreach (var metadata in objectInfo.Metadata)
        {
            Response.Headers.Append($"x-amz-meta-{metadata.Key}", metadata.Value);
        }

        // Add checksum headers if present
        if (!string.IsNullOrEmpty(objectInfo.ChecksumCRC32))
            Response.Headers.Append("x-amz-checksum-crc32", objectInfo.ChecksumCRC32);
        if (!string.IsNullOrEmpty(objectInfo.ChecksumCRC32C))
            Response.Headers.Append("x-amz-checksum-crc32c", objectInfo.ChecksumCRC32C);
        if (!string.IsNullOrEmpty(objectInfo.ChecksumSHA1))
            Response.Headers.Append("x-amz-checksum-sha1", objectInfo.ChecksumSHA1);
        if (!string.IsNullOrEmpty(objectInfo.ChecksumSHA256))
            Response.Headers.Append("x-amz-checksum-sha256", objectInfo.ChecksumSHA256);
        if (!string.IsNullOrEmpty(objectInfo.ChecksumCRC64NVME))
            Response.Headers.Append("x-amz-checksum-crc64nvme", objectInfo.ChecksumCRC64NVME);

        return Ok();
    }

    private static XmlDeserializationResult<DeleteMultipleObjectsRequest> TryDeserializeDeleteRequest(string xmlContent)
    {
        // Try without namespace first (for compatibility with most clients)
        if (TryDeserializeDeleteWithoutNamespace(xmlContent, out var deleteRequestNoNs))
        {
            return XmlDeserializationResult<DeleteMultipleObjectsRequest>.Success(deleteRequestNoNs);
        }

        // Try with S3 namespace
        if (TryDeserializeDeleteWithNamespace(xmlContent, out var deleteRequestWithNs))
        {
            return XmlDeserializationResult<DeleteMultipleObjectsRequest>.Success(deleteRequestWithNs);
        }

        return XmlDeserializationResult<DeleteMultipleObjectsRequest>.Error("The XML you provided was not well-formed or did not validate against our published schema.");
    }

    private static bool TryDeserializeDeleteWithoutNamespace(string xmlContent, out DeleteMultipleObjectsRequest deleteRequest)
    {
        deleteRequest = null!;
        try
        {
            var serializer = new XmlSerializer(typeof(DeleteMultipleObjectsRequestNoNamespace));
            using var stringReader = new StringReader(xmlContent);
            var deleteRequestNoNamespace = serializer.Deserialize(stringReader) as DeleteMultipleObjectsRequestNoNamespace;

            if (deleteRequestNoNamespace?.Objects != null)
            {
                deleteRequest = new DeleteMultipleObjectsRequest
                {
                    Objects = deleteRequestNoNamespace.Objects,
                    Quiet = deleteRequestNoNamespace.Quiet
                };
                return true;
            }
            return false;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryDeserializeDeleteWithNamespace(string xmlContent, out DeleteMultipleObjectsRequest deleteRequest)
    {
        deleteRequest = null!;
        try
        {
            var serializer = new XmlSerializer(typeof(DeleteMultipleObjectsRequest));
            using var stringReader = new StringReader(xmlContent);
            var result = serializer.Deserialize(stringReader) as DeleteMultipleObjectsRequest;
            if (result?.Objects != null)
            {
                deleteRequest = result;
                return true;
            }
            return false;
        }
        catch
        {
            return false;
        }
    }

    [HttpPost("")]
    [RequireQueryParameter("delete")]
    [S3Authorize(S3Operations.Delete, S3ResourceType.Object)]
    public async Task<IActionResult> DeleteMultipleObjects(string bucketName, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Deleting multiple objects in bucket: {BucketName}", bucketName);

        // Check if bucket exists
        if (!await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return S3Error("NoSuchBucket", "The specified bucket does not exist", bucketName, 404);
        }

        // Read the request body once and parse the XML
        string xmlContent;
        try
        {
            xmlContent = await PipeReaderHelper.ReadAllTextAsync(Request.BodyReader, false, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to read request body for delete multiple objects in bucket {BucketName}", bucketName);
            return S3Error("MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.", bucketName, 400);
        }

        var deserializationResult = TryDeserializeDeleteRequest(xmlContent);
        if (!deserializationResult.IsSuccess)
        {
            _logger.LogWarning("Failed to parse delete multiple objects request XML for bucket {BucketName}: {ErrorMessage}", bucketName, deserializationResult.ErrorMessage);
            return S3Error("MalformedXML", deserializationResult.ErrorMessage!, bucketName, 400);
        }

        var deleteRequest = deserializationResult.Value!;
        if (deleteRequest.Objects.Count == 0)
        {
            return S3Error("MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.", bucketName, 400);
        }

        // S3 has a limit of 1000 objects per delete request
        if (deleteRequest.Objects.Count > 1000)
        {
            return S3Error("TooManyKeys", "You have provided too many keys. The maximum allowed is 1000.", bucketName, 400);
        }

        // Call the facade to perform the deletions
        var result = await _objectStorage.DeleteMultipleObjectsAsync(bucketName, deleteRequest.Objects, deleteRequest.Quiet, cancellationToken);

        // Convert the result to XML response format
        var xmlResult = new DeleteMultipleObjectsResult
        {
            Deleted = result.Deleted.Select(d => new DeletedObject
            {
                Key = d.Key,
                VersionId = d.VersionId,
                DeleteMarker = d.DeleteMarker,
                DeleteMarkerVersionId = d.DeleteMarkerVersionId
            }).ToList(),
            Errors = result.Errors.Select(e => new DeleteError
            {
                Key = e.Key,
                Code = e.Code,
                Message = e.Message,
                VersionId = e.VersionId
            }).ToList()
        };

        _logger.LogInformation("Deleted {DeletedCount} objects with {ErrorCount} errors in bucket {BucketName}",
            result.Deleted.Count, result.Errors.Count, bucketName);

        Response.ContentType = "application/xml";
        return Ok(xmlResult);
    }

    /// <summary>
    /// Gets the S3 user from the current user's claims.
    /// </summary>
    private S3User? GetS3UserFromClaims()
    {
        if (!User.Identity?.IsAuthenticated == true)
        {
            return null;
        }

        var userClaim = User.FindFirst("s3_user");
        if (userClaim == null)
        {
            return null;
        }

        try
        {
            return JsonSerializer.Deserialize<S3User>(userClaim.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to deserialize S3 user from claims");
            return null;
        }
    }
}