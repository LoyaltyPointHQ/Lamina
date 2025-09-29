using System.Text;

namespace Lamina.WebApi.Middleware;

/// <summary>
/// Middleware to ensure all S3 API responses include required AWS headers
/// </summary>
public class S3ResponseHeadersMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<S3ResponseHeadersMiddleware> _logger;

    public S3ResponseHeadersMiddleware(RequestDelegate next, ILogger<S3ResponseHeadersMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        // Generate unique request ID for S3
        var requestId = GenerateRequestId();
        
        // Use ASP.NET Core's built-in request ID for extended request ID
        var extendedRequestId = context.TraceIdentifier;
        
        // Store in HttpContext for use by controllers/error handlers
        context.Items["S3RequestId"] = requestId;
        context.Items["S3ExtendedRequestId"] = extendedRequestId;

        // Hook into the response to add headers
        context.Response.OnStarting(() =>
        {
            AddS3Headers(context, requestId, extendedRequestId);
            return Task.CompletedTask;
        });

        await _next(context);
    }

    private void AddS3Headers(HttpContext context, string requestId, string extendedRequestId)
    {
        var response = context.Response;
        var headers = response.Headers;

        // Add required AWS S3 headers if not already present
        if (!headers.ContainsKey("x-amz-request-id"))
        {
            headers.Append("x-amz-request-id", requestId);
        }

        if (!headers.ContainsKey("x-amz-id-2"))
        {
            headers.Append("x-amz-id-2", extendedRequestId);
        }

        // Override Server header to match AWS S3
        headers.Remove("Server");
        headers.Append("Server", "AmazonS3");

        // Add Date header if not present
        if (!headers.ContainsKey("Date"))
        {
            headers.Append("Date", DateTimeOffset.UtcNow.ToString("R"));
        }

        _logger.LogDebug("Added S3 headers: RequestId={RequestId}, ExtendedId={ExtendedId}", 
            requestId, extendedRequestId);
    }

    /// <summary>
    /// Generates a unique request ID similar to AWS S3 format
    /// </summary>
    private static string GenerateRequestId()
    {
        // Generate a 16-character alphanumeric request ID
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        var requestId = new StringBuilder(16);
        
        for (int i = 0; i < 16; i++)
        {
            requestId.Append(chars[Random.Shared.Next(chars.Length)]);
        }
        
        return requestId.ToString();
    }


}