using Microsoft.AspNetCore.Mvc;
using Lamina.Models;
using Lamina.Services;
using System.Xml.Serialization;
using Lamina.Storage.Abstract;
using Lamina.Configuration;
using Lamina.Helpers;
using Microsoft.Extensions.Options;

namespace Lamina.Controllers;

[ApiController]
[Produces("application/xml")]
public class S3BucketsController : ControllerBase
{
    private readonly IBucketStorageFacade _bucketStorage;
    private readonly IObjectStorageFacade _objectStorage;
    private readonly IMultipartUploadStorageFacade _multipartStorage;
    private readonly IAuthenticationService _authService;
    private readonly BucketDefaultsSettings _bucketDefaults;
    private readonly ILogger<S3BucketsController> _logger;

    public S3BucketsController(
        IBucketStorageFacade bucketStorage,
        IObjectStorageFacade objectStorage,
        IMultipartUploadStorageFacade multipartStorage,
        IAuthenticationService authService,
        IOptions<BucketDefaultsSettings> bucketDefaultsOptions,
        ILogger<S3BucketsController> logger)
    {
        _bucketStorage = bucketStorage;
        _objectStorage = objectStorage;
        _multipartStorage = multipartStorage;
        _authService = authService;
        _bucketDefaults = bucketDefaultsOptions.Value;
        _logger = logger;
    }

    [HttpPut("{bucketName}")]
    public async Task<IActionResult> CreateBucket(string bucketName, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Creating bucket: {BucketName}", bucketName);

        if (!IsValidBucketName(bucketName))
        {
            _logger.LogWarning("Invalid bucket name: {BucketName}", bucketName);
            var error = new S3Error
            {
                Code = "InvalidBucketName",
                Message = "The specified bucket is not valid. Bucket names must be between 3-63 characters, contain only lowercase letters, numbers, dots, and hyphens, and follow S3 naming conventions.",
                Resource = bucketName
            };
            Response.StatusCode = 400;
            Response.ContentType = "application/xml";
            return new ObjectResult(error);
        }

        // Parse bucket configuration from request body or headers
        var createRequest = ParseCreateBucketRequest(cancellationToken);
        
        // Get authenticated user from HttpContext if available
        var authenticatedUser = HttpContext.Items["AuthenticatedUser"] as S3User;
        createRequest.OwnerId = authenticatedUser?.AccessKeyId ?? "anonymous";
        createRequest.OwnerDisplayName = authenticatedUser?.Name ?? "anonymous";

        var bucket = await _bucketStorage.CreateBucketAsync(bucketName, createRequest, cancellationToken);
        if (bucket == null)
        {
            _logger.LogWarning("Bucket already exists: {BucketName}", bucketName);
            var error = new S3Error
            {
                Code = "BucketAlreadyExists",
                Message = "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",
                Resource = bucketName
            };
            Response.StatusCode = 409;
            Response.ContentType = "application/xml";
            return new ObjectResult(error);
        }

        _logger.LogInformation("Bucket created successfully: {BucketName}", bucketName);
        Response.Headers.Append("Location", $"/{bucketName}");
        return Ok();
    }

    private static bool IsValidBucketName(string bucketName)
    {
        if (string.IsNullOrWhiteSpace(bucketName) || bucketName.Length < 3 || bucketName.Length > 63)
            return false;

        var regex = new System.Text.RegularExpressions.Regex(@"^[a-z0-9][a-z0-9.-]*[a-z0-9]$");
        if (!regex.IsMatch(bucketName))
            return false;

        if (bucketName.Contains("..") || bucketName.Contains(".-") || bucketName.Contains("-."))
            return false;

        var ipRegex = new System.Text.RegularExpressions.Regex(@"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$");
        if (ipRegex.IsMatch(bucketName))
            return false;

        string[] reservedPrefixes = { "xn--", "sthree-", "amzn-s3-demo-" };
        foreach (var prefix in reservedPrefixes)
        {
            if (bucketName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return true;
    }

    private CreateBucketRequest ParseCreateBucketRequest(CancellationToken cancellationToken)
    {
        var request = new CreateBucketRequest();

        // Check for bucket type in header
        if (Request.Headers.TryGetValue("x-amz-bucket-type", out var bucketTypeHeader))
        {
            if (Enum.TryParse<BucketType>(bucketTypeHeader.ToString(), true, out var bucketType))
            {
                request.Type = bucketType;
            }
        }

        // Check for storage class in header
        if (Request.Headers.TryGetValue("x-amz-storage-class", out var storageClassHeader))
        {
            request.StorageClass = storageClassHeader.ToString();
        }

// TODO: Parse XML request body for CreateBucketConfiguration if needed
        // For now, we'll just use headers and apply defaults

        // Apply defaults from configuration when not specified
        if (!request.Type.HasValue)
        {
            request.Type = _bucketDefaults.Type;
        }

if (string.IsNullOrEmpty(request.StorageClass) && !string.IsNullOrEmpty(_bucketDefaults.StorageClass))
        {
            request.StorageClass = _bucketDefaults.StorageClass;
        }

        return request;
    }

    [HttpGet("")]
    [Route("/")]
    public async Task<IActionResult> ListBuckets(CancellationToken cancellationToken = default)
    {
        var buckets = await _bucketStorage.ListBucketsAsync(cancellationToken);

        // Filter buckets based on user permissions
        var filteredBuckets = buckets.Buckets;

        if (_authService.IsAuthenticationEnabled())
        {
            var user = HttpContext.Items["AuthenticatedUser"] as S3User;
            if (user != null)
            {
                filteredBuckets = buckets.Buckets.Where(bucket =>
                    _authService.UserHasAccessToBucket(user, bucket.Name, "list") ||
                    _authService.UserHasAccessToBucket(user, bucket.Name, "read") ||
                    _authService.UserHasAccessToBucket(user, bucket.Name, "write") ||
                    _authService.UserHasAccessToBucket(user, bucket.Name, "delete")
                ).ToList();
            }
        }

        var result = new ListAllMyBucketsResult
        {
            Owner = new Owner(),
            Buckets = filteredBuckets.Select(b => new BucketInfo
            {
                Name = b.Name,
                CreationDate = b.CreationDate.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'"),
                BucketType = b.Type.ToString()
            }).ToList()
        };

        Response.ContentType = "application/xml";
        return Ok(result);
    }

    [HttpGet("{bucketName}")]
    public async Task<IActionResult> ListObjects(
        string bucketName,
        [FromQuery(Name = "list-type")] int? listType,
        [FromQuery] string? uploads,
        [FromQuery] string? prefix,
        [FromQuery] string? delimiter,
        [FromQuery(Name = "max-keys")] int? maxKeys,
        [FromQuery] string? marker,
        [FromQuery(Name = "key-marker")] string? keyMarker,
        [FromQuery(Name = "upload-id-marker")] string? uploadIdMarker,
        [FromQuery(Name = "max-uploads")] int? maxUploads,
        [FromQuery(Name = "continuation-token")] string? continuationToken,
        [FromQuery(Name = "start-after")] string? startAfter,
        [FromQuery(Name = "encoding-type")] string? encodingType,
        [FromQuery(Name = "fetch-owner")] bool fetchOwner = false,
        CancellationToken cancellationToken = default)
    {
        // List multipart uploads
        if (Request.Query.ContainsKey("uploads"))
        {
            return await ListMultipartUploadsInternal(bucketName, keyMarker, uploadIdMarker, maxUploads, cancellationToken);
        }
        var exists = await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken);
        if (!exists)
        {
            var error = new S3Error
            {
                Code = "NoSuchBucket",
                Message = "The specified bucket does not exist",
                Resource = bucketName
            };
            Response.StatusCode = 404;
            Response.ContentType = "application/xml";
            return new ObjectResult(error);
        }

        // Determine API version: listType=2 means ListObjectsV2, otherwise ListObjects V1
        var isV2 = listType == 2;

        // Validate encoding-type parameter
        if (!string.IsNullOrEmpty(encodingType) && !encodingType.Equals("url", StringComparison.OrdinalIgnoreCase))
        {
            var error = new S3Error
            {
                Code = "InvalidArgument",
                Message = $"Invalid Encoding Method specified in Request. The encoding-type parameter value '{encodingType}' is invalid. Valid value is 'url'.",
                Resource = $"/{bucketName}"
            };
            Response.StatusCode = 400;
            Response.ContentType = "application/xml";
            return new ObjectResult(error);
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
            var error = new S3Error
            {
                Code = result.ErrorCode!,
                Message = result.ErrorMessage!,
                Resource = $"/{bucketName}"
            };
            Response.StatusCode = 400;
            Response.ContentType = "application/xml";
            return new ObjectResult(error);
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
                    Owner = fetchOwner && o.OwnerId != null ? new Owner(o.OwnerId, o.OwnerDisplayName ?? o.OwnerId) : null  // Only include owner if requested in V2
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
                    Owner = new Owner(o.OwnerId ?? "anonymous", o.OwnerDisplayName ?? "anonymous")  // Always include owner in V1
                }).ToList(),
                CommonPrefixesList = objects.CommonPrefixes.Select(cp => new CommonPrefixes
                {
                    Prefix = S3UrlEncoder.ConditionalEncode(cp, encodingType) ?? cp
                }).ToList()
            };
            return Ok(listResult);
        }
    }

    [HttpDelete("{bucketName}")]
    public async Task<IActionResult> DeleteBucket(string bucketName, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Deleting bucket: {BucketName}", bucketName);

        var deleted = await _bucketStorage.DeleteBucketAsync(bucketName, force: true, cancellationToken);
        if (!deleted)
        {
            _logger.LogWarning("Bucket not found for deletion: {BucketName}", bucketName);
            var error = new S3Error
            {
                Code = "NoSuchBucket",
                Message = "The specified bucket does not exist",
                Resource = bucketName
            };
            Response.StatusCode = 404;
            Response.ContentType = "application/xml";
            return new ObjectResult(error);
        }

        _logger.LogInformation("Bucket deleted successfully: {BucketName}", bucketName);
        return NoContent();
    }

    [HttpHead("{bucketName}")]
    public async Task<IActionResult> HeadBucket(string bucketName, CancellationToken cancellationToken = default)
    {
        var bucket = await _bucketStorage.GetBucketAsync(bucketName, cancellationToken);
        if (bucket == null)
        {
            return NotFound();
        }

        // Add bucket type headers
        Response.Headers.Append("x-amz-bucket-type", bucket.Type.ToString());
        if (!string.IsNullOrEmpty(bucket.StorageClass))
        {
            Response.Headers.Append("x-amz-storage-class", bucket.StorageClass);
        }

        return Ok();
    }

    private async Task<IActionResult> ListMultipartUploadsInternal(
        string bucketName,
        string? keyMarker,
        string? uploadIdMarker,
        int? maxUploads,
        CancellationToken cancellationToken)
    {
        var uploads = await _multipartStorage.ListMultipartUploadsAsync(bucketName, cancellationToken);

        var result = new ListMultipartUploadsResult
        {
            Bucket = bucketName,
            KeyMarker = keyMarker,
            UploadIdMarker = uploadIdMarker,
            MaxUploads = maxUploads ?? 1000,
            IsTruncated = false,
            Uploads = uploads.Select(u => new Upload
            {
                Key = u.Key,
                UploadId = u.UploadId,
                StorageClass = "STANDARD",
                Initiated = u.Initiated.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'"),
                Owner = new Owner(),
                Initiator = new Owner()
            }).ToList()
        };

        Response.ContentType = "application/xml";
        return Ok(result);
    }


}