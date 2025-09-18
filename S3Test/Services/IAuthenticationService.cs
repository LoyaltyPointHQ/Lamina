using S3Test.Models;
using Microsoft.AspNetCore.Http;

namespace S3Test.Services
{
    public interface IAuthenticationService
    {
        Task<(bool isValid, S3User? user, string? error)> ValidateRequestAsync(HttpRequest request, string bucketName, string? objectKey = null, string? operation = null);
        bool IsAuthenticationEnabled();
        S3User? GetUserByAccessKey(string accessKeyId);
    }
}