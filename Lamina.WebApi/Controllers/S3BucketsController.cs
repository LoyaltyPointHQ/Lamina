using System.Text.Json;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Lamina.WebApi.Authorization;
using Lamina.WebApi.Controllers.Attributes;
using Lamina.WebApi.Controllers.Base;
using Lamina.WebApi.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace Lamina.WebApi.Controllers;

public class S3BucketsController : S3ControllerBase
{
    private readonly IBucketStorageFacade _bucketStorage;
    private readonly IAuthenticationService _authService;
    private readonly BucketDefaultsSettings _bucketDefaults;
    private readonly ILogger<S3BucketsController> _logger;

    public S3BucketsController(
        IBucketStorageFacade bucketStorage,
        IAuthenticationService authService,
        IOptions<BucketDefaultsSettings> bucketDefaultsOptions,
        ILogger<S3BucketsController> logger
    )
    {
        _bucketStorage = bucketStorage;
        _authService = authService;
        _bucketDefaults = bucketDefaultsOptions.Value;
        _logger = logger;
    }

    [HttpPut("{bucketName}")]
    [S3Authorize(S3Operations.Write, S3ResourceType.Bucket)]
    public async Task<IActionResult> CreateBucket(string bucketName, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Creating bucket: {BucketName}", bucketName);

        if (!IsValidBucketName(bucketName))
        {
            _logger.LogWarning("Invalid bucket name: {BucketName}", bucketName);
            return S3Error("InvalidBucketName",
                "The specified bucket is not valid. Bucket names must be between 3-63 characters, contain only lowercase letters, numbers, dots, and hyphens, and follow S3 naming conventions.",
                bucketName,
                400);
        }

        // Parse bucket configuration from request body or headers
        var createRequest = ParseCreateBucketRequest();

        // Get authenticated user from claims
        var authenticatedUser = GetS3UserFromClaims();
        createRequest.OwnerId = authenticatedUser?.AccessKeyId ?? "anonymous";
        createRequest.OwnerDisplayName = authenticatedUser?.Name ?? "anonymous";

        var bucket = await _bucketStorage.CreateBucketAsync(bucketName, createRequest, cancellationToken);
        if (bucket == null)
        {
            _logger.LogWarning("Bucket already exists: {BucketName}", bucketName);
            return S3Error("BucketAlreadyExists",
                "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",
                bucketName,
                409);
        }

        _logger.LogInformation("Bucket created successfully: {BucketName}", bucketName);
        Response.Headers.Append("Location", $"/{bucketName}");
        return Ok();
    }


    private CreateBucketRequest ParseCreateBucketRequest()
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
    [S3Authorize(S3Operations.List, S3ResourceType.Bucket)]
    public async Task<IActionResult> ListBuckets(CancellationToken cancellationToken = default)
    {
        var buckets = await _bucketStorage.ListBucketsAsync(cancellationToken);

        // Filter buckets based on user permissions
        var filteredBuckets = buckets.Buckets;

        if (_authService.IsAuthenticationEnabled())
        {
            var user = GetS3UserFromClaims();
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
    [RequireQueryParameter("location")]
    [S3Authorize(S3Operations.Read, S3ResourceType.Bucket)]
    public async Task<IActionResult> GetBucketLocation(string bucketName, CancellationToken cancellationToken = default)
    {
        // Check if bucket exists first
        var exists = await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken);
        if (!exists)
        {
            return S3Error("NoSuchBucket", "The specified bucket does not exist", bucketName, 404);
        }

        // According to S3 API specification:
        // - For us-east-1 (default region), GetBucketLocation returns null/empty
        // - Lamina operates as a single-region system (documented in README)
        // - This implementation returns empty LocationConstraint to simulate us-east-1 behavior
        var result = new LocationConstraintResult
        {
            Region = null // Empty for us-east-1 compatibility
        };

        Response.ContentType = "application/xml";
        return Ok(result);
    }

    [HttpDelete("{bucketName}")]
    [S3Authorize(S3Operations.Delete, S3ResourceType.Bucket)]
    public async Task<IActionResult> DeleteBucket(string bucketName, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Deleting bucket: {BucketName}", bucketName);

        var deleted = await _bucketStorage.DeleteBucketAsync(bucketName, force: true, cancellationToken);
        if (!deleted)
        {
            _logger.LogWarning("Bucket not found for deletion: {BucketName}", bucketName);
            return S3Error("NoSuchBucket", "The specified bucket does not exist", bucketName, 404);
        }

        _logger.LogInformation("Bucket deleted successfully: {BucketName}", bucketName);
        return NoContent();
    }

    [HttpHead("{bucketName}")]
    [S3Authorize(S3Operations.Read, S3ResourceType.Bucket)]
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