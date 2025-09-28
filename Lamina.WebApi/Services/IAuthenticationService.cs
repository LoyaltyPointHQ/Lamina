using Lamina.Core.Models;

namespace Lamina.WebApi.Services
{
    public interface IAuthenticationService
    {
        Task<(bool isValid, S3User? user, string? error)> ValidateRequestAsync(HttpRequest request, string bucketName, string? objectKey = null, string? operation = null);
        bool IsAuthenticationEnabled();
        S3User? GetUserByAccessKey(string accessKeyId);
        bool UserHasAccessToBucket(S3User user, string bucketName, string? operation = null);
    }
}