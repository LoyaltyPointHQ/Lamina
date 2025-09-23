using System.Xml.Serialization;
using Microsoft.AspNetCore.Http;
using Lamina.Models;
using Lamina.Services;
using Lamina.Streaming;

namespace Lamina.Middleware
{
    public class S3AuthenticationMiddleware
    {
        private readonly RequestDelegate _next;
        private readonly IAuthenticationService _authService;
        private readonly IStreamingAuthenticationService _streamingAuthService;
        private readonly ILogger<S3AuthenticationMiddleware> _logger;

        public S3AuthenticationMiddleware(RequestDelegate next, IAuthenticationService authService, IStreamingAuthenticationService streamingAuthService, ILogger<S3AuthenticationMiddleware> logger)
        {
            _next = next;
            _authService = authService;
            _streamingAuthService = streamingAuthService;
            _logger = logger;
        }

        public async Task InvokeAsync(HttpContext context)
        {
            // Skip authentication for health endpoint
            if (context.Request.Path.StartsWithSegments("/health"))
            {
                await _next(context);
                return;
            }

            if (!_authService.IsAuthenticationEnabled())
            {
                await _next(context);
                return;
            }

            var path = context.Request.Path.Value ?? "/";
            var pathSegments = path.Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries);

            string bucketName;
            string? objectKey = null;

            if (pathSegments.Length == 0)
            {
                // Root path - ListBuckets operation
                // For list buckets, we only need to validate the signature, not bucket permissions
                var (bucketListIsValid, bucketListUser, bucketListError) = await _authService.ValidateRequestAsync(
                    context.Request,
                    "", // Empty bucket name for signature validation only
                    null,
                    "LIST");

                if (!bucketListIsValid)
                {
                    _logger.LogWarning("Authentication failed: {Error}", bucketListError);
                    await WriteErrorResponse(context, bucketListError ?? "Access Denied");
                    return;
                }

                if (bucketListUser != null)
                {
                    context.Items["AuthenticatedUser"] = bucketListUser;
                }

                await _next(context);
                return;
            }
            else
            {
                bucketName = pathSegments[0];
                objectKey = pathSegments.Length > 1 ? string.Join("/", pathSegments.Skip(1)) : null;
            }

            // Check if this is a streaming request
            var contentSha256 = context.Request.Headers["x-amz-content-sha256"].FirstOrDefault();
            var isStreamingPayload = contentSha256 == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

            if (isStreamingPayload)
            {
                // For streaming requests, validate the initial signature and set up chunk validation
                var (isValid, user, error) = await _authService.ValidateRequestAsync(
                    context.Request,
                    bucketName,
                    objectKey,
                    context.Request.Method);

                if (!isValid)
                {
                    _logger.LogWarning("Streaming authentication failed: {Error}", error);
                    await WriteErrorResponse(context, error ?? "Access Denied");
                    return;
                }

                if (user != null)
                {
                    context.Items["AuthenticatedUser"] = user;
                    
                    // Create chunk validator for downstream processing
                    var chunkValidator = _streamingAuthService.CreateChunkValidator(context.Request, user);
                    if (chunkValidator != null)
                    {
                        context.Items["ChunkValidator"] = chunkValidator;
                    }
                }
            }
            else
            {
                // Regular non-streaming authentication
                var (isValid, user, error) = await _authService.ValidateRequestAsync(
                    context.Request,
                    bucketName,
                    objectKey,
                    context.Request.Method);

                if (!isValid)
                {
                    _logger.LogWarning("Authentication failed: {Error}", error);
                    await WriteErrorResponse(context, error ?? "Access Denied");
                    return;
                }

                if (user != null)
                {
                    context.Items["AuthenticatedUser"] = user;
                }
            }

            await _next(context);
        }

        private async Task WriteErrorResponse(HttpContext context, string message)
        {
            context.Response.StatusCode = 403;
            context.Response.ContentType = "application/xml";

            var errorResponse = new ErrorResponse
            {
                Code = "AccessDenied",
                Message = message,
                RequestId = Guid.NewGuid().ToString(),
                HostId = Environment.MachineName
            };

            var serializer = new XmlSerializer(typeof(ErrorResponse));
            var xmlSettings = new System.Xml.XmlWriterSettings
            {
                Encoding = System.Text.Encoding.UTF8,
                Indent = false,
                OmitXmlDeclaration = true
            };

            using var memoryStream = new System.IO.MemoryStream();
            using (var xmlWriter = System.Xml.XmlWriter.Create(memoryStream, xmlSettings))
            {
                serializer.Serialize(xmlWriter, errorResponse);
            }

            var xml = System.Text.Encoding.UTF8.GetString(memoryStream.ToArray());
            await context.Response.WriteAsync(xml);
        }
    }

    [XmlRoot("Error")]
    public class ErrorResponse
    {
        public string Code { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
        public string RequestId { get; set; } = string.Empty;
        public string HostId { get; set; } = string.Empty;
    }

    public static class S3AuthenticationMiddlewareExtensions
    {
        public static IApplicationBuilder UseS3Authentication(this IApplicationBuilder builder)
        {
            return builder.UseMiddleware<S3AuthenticationMiddleware>();
        }
    }
}