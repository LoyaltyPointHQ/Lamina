using Lamina.Core.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace Lamina.WebApi.Controllers.Base;

[ApiController]
[Produces("application/xml")]
public abstract class S3ControllerBase : ControllerBase
{
    protected IActionResult S3Error(string code, string message, string resource, int statusCode = 400)
    {
        var error = new S3Error
        {
            Code = code,
            Message = message,
            Resource = resource,
            RequestId = GetRequestId(),
            HostId = GetExtendedRequestId()
        };
        Response.StatusCode = statusCode;
        Response.ContentType = "application/xml";
        return new ObjectResult(error);
    }

    /// <summary>
    /// Gets the request ID from the middleware context
    /// </summary>
    protected string GetRequestId()
    {
        return HttpContext.Items["S3RequestId"] as string ?? Guid.NewGuid().ToString();
    }

    /// <summary>
    /// Gets the extended request ID from the middleware context
    /// </summary>
    protected string GetExtendedRequestId()
    {
        return HttpContext.TraceIdentifier;
    }

    protected static bool IsValidBucketName(string bucketName)
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

    /// <summary>
    /// Validates that the Content-Length header is present in the request.
    /// Required by S3 API for PUT operations.
    /// </summary>
    /// <param name="resource">The resource path for error reporting</param>
    /// <returns>An error IActionResult if validation fails, null if valid</returns>
    protected IActionResult? ValidateContentLengthHeader(string resource)
    {
        if (!Request.ContentLength.HasValue)
        {
            return S3Error("MissingContentLength", "You must provide the Content-Length HTTP header.", resource, 411);
        }
        return null;
    }

    /// <summary>
    /// Validates that the x-amz-content-sha256 header is present and valid when using AWS Signature V4.
    /// </summary>
    /// <param name="resource">The resource path for error reporting</param>
    /// <returns>An error IActionResult if validation fails, null if valid</returns>
    protected IActionResult? ValidateContentSha256Header(string resource)
    {
        // Only validate if using AWS Signature V4
        if (Request.Headers.TryGetValue("Authorization", out var authHeader) &&
            authHeader.ToString().Contains("AWS4-HMAC-SHA256"))
        {
            if (!Request.Headers.ContainsKey("x-amz-content-sha256"))
            {
                return S3Error("InvalidRequest", "Missing required header for this request: x-amz-content-sha256", resource, 400);
            }

            var contentSha256 = Request.Headers["x-amz-content-sha256"].ToString();
            // Validate it's one of the allowed values
            if (string.IsNullOrWhiteSpace(contentSha256) ||
                !(contentSha256 == "UNSIGNED-PAYLOAD" ||
                  contentSha256 == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" ||
                  contentSha256 == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" ||
                  System.Text.RegularExpressions.Regex.IsMatch(contentSha256, "^[a-f0-9]{64}$")))
            {
                return S3Error("InvalidRequest", "Invalid x-amz-content-sha256 header value.", resource, 400);
            }
        }
        return null;
    }

    /// <summary>
    /// Logs comprehensive request headers for upload operations to aid in diagnostics.
    /// </summary>
    /// <param name="logger">The logger instance to use</param>
    /// <param name="operation">The operation name (e.g., "PutObject", "UploadPart")</param>
    /// <param name="bucketName">The bucket name</param>
    /// <param name="key">The object key</param>
    /// <param name="additionalInfo">Additional key-value pairs to log</param>
    protected void LogUploadRequestHeaders(ILogger logger, string operation, string bucketName, string key, params (string key, object value)[] additionalInfo)
    {
        var logMessage = $"{operation} request - Bucket: {{BucketName}}, Key: {{Key}}, " +
                        "Content-Length: {ContentLength}, Content-Type: {ContentType}, " +
                        "x-amz-content-sha256: {ContentSha256}, Transfer-Encoding: {TransferEncoding}";

        var logArgs = new List<object>
        {
            bucketName,
            key,
            Request.ContentLength?.ToString() ?? "missing",
            Request.ContentType ?? "not set",
            Request.Headers.ContainsKey("x-amz-content-sha256") ? Request.Headers["x-amz-content-sha256"].ToString() : "not set",
            Request.Headers.ContainsKey("Transfer-Encoding") ? Request.Headers["Transfer-Encoding"].ToString() : "not set"
        };

        // Add additional info to log message and args
        foreach (var (infoKey, value) in additionalInfo)
        {
            logMessage += $", {infoKey}: {{{infoKey}}}";
            logArgs.Add(value);
        }

        logger.LogInformation(logMessage, logArgs.ToArray());
    }
}