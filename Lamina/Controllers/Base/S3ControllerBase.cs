using Microsoft.AspNetCore.Mvc;
using Lamina.Models;

namespace Lamina.Controllers.Base;

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
            Resource = resource
        };
        Response.StatusCode = statusCode;
        Response.ContentType = "application/xml";
        return new ObjectResult(error);
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
}