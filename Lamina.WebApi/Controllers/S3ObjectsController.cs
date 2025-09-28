using System.IO.Pipelines;
using System.Text.Json;
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
    [RequireNoQueryParameters("location", "uploads")]
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
            var random = new Random();
            objects.Contents = objects.Contents.OrderBy(x => random.Next()).ToList();
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

        var putRequest = new PutObjectRequest
        {
            Key = key,
            ContentType = contentType,
            Metadata = Request.Headers
                .Where(h => h.Key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
                .ToDictionary(h => h.Key[11..], h => h.Value.ToString()),
            OwnerId = authenticatedUser?.AccessKeyId ?? "anonymous",
            OwnerDisplayName = authenticatedUser?.Name ?? "anonymous"
        };

        // Use PipeReader from the request body
        var reader = Request.BodyReader;

        _logger.LogInformation("Putting object {Key} in bucket {BucketName}", key, bucketName);

        // Check if there's a chunk validator from the authentication middleware
        var chunkValidator = HttpContext.Items["ChunkValidator"] as IChunkSignatureValidator;

        var s3Object = chunkValidator != null
            ? await _objectStorage.PutObjectAsync(bucketName, key, reader, chunkValidator, putRequest, cancellationToken)
            : await _objectStorage.PutObjectAsync(bucketName, key, reader, putRequest, cancellationToken);
        if (s3Object == null)
        {
            // If we had a chunk validator and the operation failed, it's likely due to invalid signature
            if (chunkValidator != null)
            {
                _logger.LogWarning("Chunk signature validation failed for object {Key} in bucket {BucketName}", key, bucketName);
                return S3Error("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", $"/{bucketName}/{key}", 403);
            }

            _logger.LogError("Failed to put object {Key} in bucket {BucketName}", key, bucketName);
            return StatusCode(500);
        }

        _logger.LogInformation("Object {Key} stored successfully in bucket {BucketName}, ETag: {ETag}, Size: {Size}", key, bucketName, s3Object.ETag, s3Object.Size);
        Response.Headers.Append("ETag", $"\"{s3Object.ETag}\"");
        Response.Headers.Append("x-amz-version-id", "null");
        return Ok();
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

        Response.Headers.Append("ETag", $"\"{metadata.ETag}\"");
        Response.Headers.Append("Last-Modified", metadata.LastModified.ToString("R"));
        Response.Headers.Append("Content-Length", metadata.Size.ToString());

        foreach (var metadataItem in metadata.Metadata)
        {
            Response.Headers.Append($"x-amz-meta-{metadataItem.Key}", metadataItem.Value);
        }

        // Create a pipe to stream data back
        var pipe = new Pipe();
        var writer = pipe.Writer;

        // Start writing to pipe in background
        _ = Task.Run(async () =>
        {
            try
            {
                await _objectStorage.WriteObjectToStreamAsync(bucketName, key, writer, cancellationToken);
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

        return Ok();
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