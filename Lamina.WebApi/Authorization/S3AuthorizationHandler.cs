using System.Security.Claims;
using System.Text.Json;
using Lamina.Core.Models;
using Lamina.WebApi.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Options;

namespace Lamina.WebApi.Authorization
{
    /// <summary>
    /// Authorization handler for S3 bucket and object access requirements.
    /// </summary>
    public class S3AuthorizationHandler : IAuthorizationHandler
    {
        private readonly IAuthenticationService _authService;
        private readonly AuthenticationSettings _authSettings;
        private readonly ILogger<S3AuthorizationHandler> _logger;

        public S3AuthorizationHandler(
            IAuthenticationService authService,
            IOptions<AuthenticationSettings> authSettings,
            ILogger<S3AuthorizationHandler> logger)
        {
            _authService = authService;
            _authSettings = authSettings.Value;
            _logger = logger;
        }

        /// <summary>
        /// Handles authorization for all S3 requirements.
        /// </summary>
        public Task HandleAsync(AuthorizationHandlerContext context)
        {
            foreach (var requirement in context.Requirements)
            {
                if (requirement is S3BucketAccessRequirement bucketRequirement)
                {
                    HandleBucketRequirement(context, bucketRequirement);
                }
                else if (requirement is S3ObjectAccessRequirement objectRequirement)
                {
                    HandleObjectRequirement(context, objectRequirement);
                }
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Handles bucket access authorization.
        /// </summary>
        private void HandleBucketRequirement(
            AuthorizationHandlerContext context,
            S3BucketAccessRequirement requirement)
        {
            // If authentication is disabled, always authorize
            if (!_authSettings.Enabled)
            {
                _logger.LogDebug("Authorization succeeded - authentication disabled");
                context.Succeed(requirement);
                return;
            }

            // Check if user is anonymous (when authentication is disabled)
            if (context.User.HasClaim(ClaimTypes.Anonymous, "true"))
            {
                _logger.LogDebug("Authorization succeeded - anonymous user");
                context.Succeed(requirement);
                return;
            }

            // Extract bucket name from the HTTP context
            var httpContext = context.Resource as HttpContext;
            if (httpContext == null)
            {
                _logger.LogWarning("Authorization failed - no HTTP context available");
                context.Fail();
                return;
            }

            var bucketName = ExtractBucketName(httpContext);
            if (string.IsNullOrEmpty(bucketName))
            {
                // For list buckets operation, we need any bucket permission
                if (HasAnyBucketPermission(context.User, requirement.Operation))
                {
                    _logger.LogDebug("Authorization succeeded for list buckets operation");
                    context.Succeed(requirement);
                }
                else
                {
                    _logger.LogWarning("Authorization failed - no bucket permissions for list operation");
                    context.Fail();
                }
                return;
            }

            // Get the S3 user from claims
            var user = GetS3UserFromClaims(context.User);
            if (user == null)
            {
                _logger.LogWarning("Authorization failed - no S3 user in claims");
                context.Fail();
                return;
            }

            // Check bucket permissions
            if (_authService.UserHasAccessToBucket(user, bucketName, requirement.Operation))
            {
                _logger.LogDebug("Authorization succeeded for bucket {BucketName} with operation {Operation}", 
                    bucketName, requirement.Operation);
                context.Succeed(requirement);
            }
            else
            {
                _logger.LogWarning("Authorization failed for bucket {BucketName} with operation {Operation}", 
                    bucketName, requirement.Operation);
                context.Fail();
            }
        }

        /// <summary>
        /// Handles object access authorization.
        /// </summary>
        private void HandleObjectRequirement(
            AuthorizationHandlerContext context,
            S3ObjectAccessRequirement requirement)
        {
            // If authentication is disabled, always authorize
            if (!_authSettings.Enabled)
            {
                _logger.LogDebug("Authorization succeeded - authentication disabled");
                context.Succeed(requirement);
                return;
            }

            // Check if user is anonymous (when authentication is disabled)
            if (context.User.HasClaim(ClaimTypes.Anonymous, "true"))
            {
                _logger.LogDebug("Authorization succeeded - anonymous user");
                context.Succeed(requirement);
                return;
            }

            // Extract bucket name from the HTTP context
            var httpContext = context.Resource as HttpContext;
            if (httpContext == null)
            {
                _logger.LogWarning("Authorization failed - no HTTP context available");
                context.Fail();
                return;
            }

            var bucketName = ExtractBucketName(httpContext);
            if (string.IsNullOrEmpty(bucketName))
            {
                _logger.LogWarning("Authorization failed - no bucket name for object operation");
                context.Fail();
                return;
            }

            // Get the S3 user from claims
            var user = GetS3UserFromClaims(context.User);
            if (user == null)
            {
                _logger.LogWarning("Authorization failed - no S3 user in claims");
                context.Fail();
                return;
            }

            // For object operations, check bucket permissions (objects inherit bucket permissions)
            if (_authService.UserHasAccessToBucket(user, bucketName, requirement.Operation))
            {
                _logger.LogDebug("Authorization succeeded for object in bucket {BucketName} with operation {Operation}", 
                    bucketName, requirement.Operation);
                context.Succeed(requirement);
            }
            else
            {
                _logger.LogWarning("Authorization failed for object in bucket {BucketName} with operation {Operation}", 
                    bucketName, requirement.Operation);
                context.Fail();
            }
        }

        /// <summary>
        /// Extracts the bucket name from the HTTP context.
        /// </summary>
        private string ExtractBucketName(HttpContext httpContext)
        {
            var path = httpContext.Request.Path.Value ?? "/";
            var pathSegments = path.Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries);

            return pathSegments.Length > 0 ? pathSegments[0] : "";
        }

        /// <summary>
        /// Gets the S3 user from the claims principal.
        /// </summary>
        private S3User? GetS3UserFromClaims(ClaimsPrincipal user)
        {
            var userClaim = user.FindFirst("s3_user");
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

        /// <summary>
        /// Checks if the user has any bucket permission for the specified operation.
        /// </summary>
        private bool HasAnyBucketPermission(ClaimsPrincipal user, string operation)
        {
            var s3User = GetS3UserFromClaims(user);
            if (s3User == null)
            {
                return false;
            }

            return s3User.BucketPermissions?.Any(bp => 
                bp.Permissions.Contains("*") || 
                bp.Permissions.Any(p => p.Equals(operation, StringComparison.OrdinalIgnoreCase))) ?? false;
        }
    }
}