using System.Xml.Serialization;
using Microsoft.AspNetCore.Http;
using S3Test.Models;
using S3Test.Services;

namespace S3Test.Middleware
{
    public class S3AuthenticationMiddleware
    {
        private readonly RequestDelegate _next;
        private readonly IAuthenticationService _authService;
        private readonly ILogger<S3AuthenticationMiddleware> _logger;

        public S3AuthenticationMiddleware(RequestDelegate next, IAuthenticationService authService, ILogger<S3AuthenticationMiddleware> logger)
        {
            _next = next;
            _authService = authService;
            _logger = logger;
        }

        public async Task InvokeAsync(HttpContext context)
        {
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
                bucketName = "*"; // Use * for service-level operations
            }
            else
            {
                bucketName = pathSegments[0];
                objectKey = pathSegments.Length > 1 ? string.Join("/", pathSegments.Skip(1)) : null;
            }

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