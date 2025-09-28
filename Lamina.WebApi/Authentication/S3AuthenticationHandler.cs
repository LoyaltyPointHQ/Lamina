using System.Security.Claims;
using System.Text.Encodings.Web;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.WebApi.Streaming;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;
using S3AuthService = Lamina.WebApi.Services.IAuthenticationService;

namespace Lamina.WebApi.Authentication
{
    /// <summary>
    /// Authentication handler for S3 AWS Signature V4 authentication.
    /// </summary>
    public class S3AuthenticationHandler : AuthenticationHandler<S3AuthenticationOptions>
    {
        private readonly S3AuthService _authService;
        private readonly IStreamingAuthenticationService _streamingAuthService;
        private readonly AuthenticationSettings _authSettings;

        public S3AuthenticationHandler(
            IOptionsMonitor<S3AuthenticationOptions> options,
            ILoggerFactory logger,
            UrlEncoder encoder,
            S3AuthService authService,
            IStreamingAuthenticationService streamingAuthService,
            IOptions<AuthenticationSettings> authSettings)
            : base(options, logger, encoder)
        {
            _authService = authService;
            _streamingAuthService = streamingAuthService;
            _authSettings = authSettings.Value;
        }

        /// <summary>
        /// Handles the authentication for S3 requests.
        /// </summary>
        protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            // Skip authentication for health endpoint
            if (Context.Request.Path.StartsWithSegments(Options.HealthCheckPath))
            {
                return AuthenticateResult.NoResult();
            }

            // If authentication is disabled, create anonymous identity
            if (!_authSettings.Enabled)
            {
                var anonymousIdentity = new ClaimsIdentity(S3AuthenticationDefaults.AuthenticationScheme);
                anonymousIdentity.AddClaim(new Claim(ClaimTypes.Anonymous, "true"));
                var anonymousPrincipal = new ClaimsPrincipal(anonymousIdentity);
                var anonymousTicket = new AuthenticationTicket(anonymousPrincipal, Scheme.Name);
                
                Logger.LogDebug("Authentication disabled - creating anonymous principal");
                return AuthenticateResult.Success(anonymousTicket);
            }

            try
            {
                // Extract bucket name and object key from path
                var (bucketName, objectKey) = ExtractBucketAndKey();

                // Check if this is a streaming request
                var contentSha256 = Context.Request.Headers[S3AuthenticationDefaults.ContentSha256HeaderName].FirstOrDefault();
                var isStreamingPayload = contentSha256 == S3AuthenticationDefaults.StreamingPayload;

                // Validate the request using the authentication service
                var (isValid, user, error) = await _authService.ValidateRequestAsync(
                    Context.Request,
                    bucketName,
                    objectKey,
                    Context.Request.Method);

                if (!isValid)
                {
                    Logger.LogWarning("S3 authentication failed: {Error}", error);
                    return AuthenticateResult.Fail(error ?? "Authentication failed");
                }

                // Create authenticated identity with user information
                var identity = new ClaimsIdentity(S3AuthenticationDefaults.AuthenticationScheme);
                
                if (user != null)
                {
                    identity.AddClaim(new Claim(ClaimTypes.NameIdentifier, user.AccessKeyId));
                    identity.AddClaim(new Claim(ClaimTypes.Name, user.Name));
                    
                    // Add bucket permissions as claims
                    foreach (var bucketPermission in user.BucketPermissions)
                    {
                        foreach (var permission in bucketPermission.Permissions)
                        {
                            identity.AddClaim(new Claim("bucket_permission", bucketPermission.BucketName + ":" + permission));
                        }
                    }

                    // Store user object for easier access in authorization handlers
                    identity.AddClaim(new Claim("s3_user", System.Text.Json.JsonSerializer.Serialize(user)));

                    // For streaming requests, set up chunk validation
                    if (isStreamingPayload)
                    {
                        var chunkValidator = _streamingAuthService.CreateChunkValidator(Context.Request, user);
                        if (chunkValidator != null)
                        {
                            Context.Items["ChunkValidator"] = chunkValidator;
                        }
                    }
                }

                var principal = new ClaimsPrincipal(identity);
                var ticket = new AuthenticationTicket(principal, Scheme.Name);

                Logger.LogDebug("S3 authentication successful for user: {AccessKeyId}", user?.AccessKeyId ?? "anonymous");
                return AuthenticateResult.Success(ticket);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Error during S3 authentication");
                return AuthenticateResult.Fail("Authentication error");
            }
        }

        /// <summary>
        /// Handles authentication challenges by returning S3-compatible error responses.
        /// </summary>
        protected override async Task HandleChallengeAsync(AuthenticationProperties properties)
        {
            Context.Response.StatusCode = 403;
            Context.Response.ContentType = "application/xml";

            var errorResponse = CreateS3ErrorResponse("AccessDenied", "Access Denied");
            await Context.Response.WriteAsync(errorResponse);
        }

        /// <summary>
        /// Handles forbidden responses with S3-compatible error format.
        /// </summary>
        protected override async Task HandleForbiddenAsync(AuthenticationProperties properties)
        {
            Context.Response.StatusCode = 403;
            Context.Response.ContentType = "application/xml";

            var errorResponse = CreateS3ErrorResponse("AccessDenied", "Access Denied");
            await Context.Response.WriteAsync(errorResponse);
        }

        /// <summary>
        /// Extracts bucket name and object key from the request path.
        /// </summary>
        private (string bucketName, string? objectKey) ExtractBucketAndKey()
        {
            var path = Context.Request.Path.Value ?? "/";
            var pathSegments = path.Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries);

            if (pathSegments.Length == 0)
            {
                // Root path - ListBuckets operation
                return ("", null);
            }

            var bucketName = pathSegments[0];
            var objectKey = pathSegments.Length > 1 ? string.Join("/", pathSegments.Skip(1)) : null;

            return (bucketName, objectKey);
        }

        /// <summary>
        /// Creates an S3-compatible error response.
        /// </summary>
        private string CreateS3ErrorResponse(string code, string message)
        {
            var requestId = Guid.NewGuid().ToString();
            var hostId = Environment.MachineName;

            return $@"<?xml version=""1.0"" encoding=""UTF-8""?>
<Error>
    <Code>{code}</Code>
    <Message>{message}</Message>
    <RequestId>{requestId}</RequestId>
    <HostId>{hostId}</HostId>
</Error>";
        }
    }
}