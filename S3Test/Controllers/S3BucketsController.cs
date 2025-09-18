using Microsoft.AspNetCore.Mvc;
using S3Test.Models;
using S3Test.Services;
using System.Xml.Serialization;

namespace S3Test.Controllers;

[ApiController]
[Produces("application/xml")]
public class S3BucketsController : ControllerBase
{
    private readonly IBucketService _bucketService;
    private readonly IObjectService _objectService;
    private readonly IMultipartUploadService _multipartUploadService;
    private readonly ILogger<S3BucketsController> _logger;

    public S3BucketsController(
        IBucketService bucketService,
        IObjectService objectService,
        IMultipartUploadService multipartUploadService,
        ILogger<S3BucketsController> logger)
    {
        _bucketService = bucketService;
        _objectService = objectService;
        _multipartUploadService = multipartUploadService;
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

        var bucket = await _bucketService.CreateBucketAsync(bucketName, null, cancellationToken);
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

    [HttpGet("")]
    [Route("/")]
    public async Task<IActionResult> ListBuckets(CancellationToken cancellationToken = default)
    {
        var buckets = await _bucketService.ListBucketsAsync(cancellationToken);

        var result = new ListAllMyBucketsResult
        {
            Owner = new Owner(),
            Buckets = buckets.Buckets.Select(b => new BucketInfo
            {
                Name = b.Name,
                CreationDate = b.CreationDate.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'")
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
        CancellationToken cancellationToken = default)
    {
        // List multipart uploads
        if (Request.Query.ContainsKey("uploads"))
        {
            return await ListMultipartUploadsInternal(bucketName, keyMarker, uploadIdMarker, maxUploads, cancellationToken);
        }
        var exists = await _bucketService.BucketExistsAsync(bucketName, cancellationToken);
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

        // Get objects from service
        var listRequest = new ListObjectsRequest
        {
            Prefix = prefix,
            Delimiter = delimiter,
            MaxKeys = maxKeys ?? 1000,
            ContinuationToken = continuationToken ?? marker
        };

        var objects = await _objectService.ListObjectsAsync(bucketName, listRequest, cancellationToken);

        var result = new ListBucketResult
        {
            Name = bucketName,
            Prefix = prefix,
            Marker = marker,
            MaxKeys = maxKeys ?? 1000,
            IsTruncated = objects.IsTruncated,
            ContentsList = objects.Contents.Select(o => new Contents
            {
                Key = o.Key,
                LastModified = o.LastModified.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'"),
                ETag = $"\"{o.ETag}\"",
                Size = o.Size,
                StorageClass = "STANDARD",
                Owner = new Owner()
            }).ToList()
        };

        Response.ContentType = "application/xml";
        return Ok(result);
    }

    [HttpDelete("{bucketName}")]
    public async Task<IActionResult> DeleteBucket(string bucketName, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Deleting bucket: {BucketName}", bucketName);
        var deleted = await _bucketService.DeleteBucketAsync(bucketName, cancellationToken);
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
        var exists = await _bucketService.BucketExistsAsync(bucketName, cancellationToken);
        if (!exists)
        {
            return NotFound();
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
        var uploads = await _multipartUploadService.ListMultipartUploadsAsync(bucketName, cancellationToken);

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