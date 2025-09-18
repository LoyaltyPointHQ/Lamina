using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using S3Test.Models;

namespace S3Test.Services
{
    public class AuthenticationService : IAuthenticationService
    {
        private readonly ILogger<AuthenticationService> _logger;
        private readonly AuthenticationSettings _authSettings;
        private readonly Dictionary<string, S3User> _usersByAccessKey;

        public AuthenticationService(ILogger<AuthenticationService> logger, IOptions<AuthenticationSettings> authSettings)
        {
            _logger = logger;
            _authSettings = authSettings.Value;
            _usersByAccessKey = _authSettings.Users?.ToDictionary(u => u.AccessKeyId, u => u) ?? new();
        }

        public bool IsAuthenticationEnabled() => _authSettings.Enabled;

        public S3User? GetUserByAccessKey(string accessKeyId)
        {
            _usersByAccessKey.TryGetValue(accessKeyId, out var user);
            return user;
        }

        public async Task<(bool isValid, S3User? user, string? error)> ValidateRequestAsync(HttpRequest request, string bucketName, string? objectKey = null, string? operation = null)
        {
            if (!IsAuthenticationEnabled())
            {
                return (true, null, null);
            }

            try
            {
                var authHeader = request.Headers["Authorization"].FirstOrDefault();
                if (string.IsNullOrEmpty(authHeader))
                {
                    return (false, null, "Missing Authorization header");
                }

                if (!authHeader.StartsWith("AWS4-HMAC-SHA256"))
                {
                    return (false, null, "Unsupported authentication method");
                }

                var sigV4Request = await ParseSignatureV4Request(request, authHeader);
                if (sigV4Request == null)
                {
                    return (false, null, "Invalid authorization header format");
                }

                var user = GetUserByAccessKey(sigV4Request.AccessKeyId);
                if (user == null)
                {
                    return (false, null, "Invalid access key");
                }

                // Skip permission check for list buckets operation (empty bucket name)
                if (!string.IsNullOrEmpty(bucketName) && !CheckPermissions(user, bucketName, operation))
                {
                    return (false, user, "Access denied");
                }

                var calculatedSignature = await CalculateSignatureV4(sigV4Request, user.SecretAccessKey);
                if (calculatedSignature != sigV4Request.Signature)
                {
                    _logger.LogWarning("Signature mismatch. Expected: {Expected}, Got: {Got}", calculatedSignature, sigV4Request.Signature);
                    return (false, user, "Invalid signature");
                }

                return (true, user, null);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error validating request");
                return (false, null, "Authentication error");
            }
        }

        public bool UserHasAccessToBucket(S3User user, string bucketName, string? operation = null)
        {
            return CheckPermissions(user, bucketName, operation);
        }

        private bool CheckPermissions(S3User user, string bucketName, string? operation)
        {
            if (user.BucketPermissions == null || !user.BucketPermissions.Any())
            {
                return false;
            }

            var bucketPerms = user.BucketPermissions.FirstOrDefault(bp =>
                bp.BucketName == "*" ||
                bp.BucketName.Equals(bucketName, StringComparison.OrdinalIgnoreCase));

            if (bucketPerms == null)
            {
                return false;
            }

            if (bucketPerms.Permissions.Contains("*"))
            {
                return true;
            }

            var requiredPermission = operation?.ToLower() switch
            {
                "get" => "read",
                "head" => "read",
                "put" => "write",
                "post" => "write",
                "delete" => "delete",
                "list" => "list",
                _ => "read"
            };

            return bucketPerms.Permissions.Any(p => p.Equals(requiredPermission, StringComparison.OrdinalIgnoreCase));
        }

        private async Task<SignatureV4Request?> ParseSignatureV4Request(HttpRequest request, string authHeader)
        {
            var match = Regex.Match(authHeader, @"AWS4-HMAC-SHA256\s+Credential=([^/]+)/([^/]+)/([^/]+)/s3/aws4_request,\s*SignedHeaders=([^,]+),\s*Signature=([a-f0-9]+)");
            if (!match.Success)
            {
                return null;
            }

            var accessKeyId = match.Groups[1].Value;
            var dateStamp = match.Groups[2].Value;
            var region = match.Groups[3].Value;
            var signedHeaders = match.Groups[4].Value;
            var signature = match.Groups[5].Value;

            var xAmzDate = request.Headers["x-amz-date"].FirstOrDefault() ?? request.Headers["X-Amz-Date"].FirstOrDefault();
            if (string.IsNullOrEmpty(xAmzDate))
            {
                return null;
            }

            request.EnableBuffering();
            var payload = "";
            if (request.Body != null)
            {
                request.Body.Position = 0;
                using var reader = new StreamReader(request.Body, Encoding.UTF8, leaveOpen: true);
                payload = await reader.ReadToEndAsync();
                request.Body.Position = 0;
            }

            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var header in request.Headers)
            {
                headers[header.Key.ToLower()] = string.Join(",", header.Value.ToArray());
            }

            // Add Host header if not present
            if (!headers.ContainsKey("host"))
            {
                headers["host"] = request.Host.HasValue ? request.Host.Value : "localhost";
            }

            var canonicalUri = request.Path.Value ?? "/";
            var canonicalQueryString = GetCanonicalQueryString(request.Query);

            return new SignatureV4Request
            {
                Method = request.Method,
                CanonicalUri = canonicalUri,
                CanonicalQueryString = canonicalQueryString,
                Headers = headers,
                Payload = payload,
                Region = region,
                Service = "s3",
                RequestDateTime = DateTime.ParseExact(xAmzDate, "yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal),
                AccessKeyId = accessKeyId,
                Signature = signature,
                SignedHeaders = signedHeaders
            };
        }

        private string GetCanonicalQueryString(IQueryCollection query)
        {
            var parameters = new SortedDictionary<string, string>(StringComparer.Ordinal);
            foreach (var kvp in query)
            {
                var key = Uri.EscapeDataString(kvp.Key);
                var value = kvp.Value.FirstOrDefault() ?? "";
                parameters[key] = Uri.EscapeDataString(value);
            }
            return string.Join("&", parameters.Select(p => $"{p.Key}={p.Value}"));
        }

        public Task<string> CalculateSignatureV4(SignatureV4Request request, string secretAccessKey)
        {
            var dateStamp = request.RequestDateTime.ToUniversalTime().ToString("yyyyMMdd");
            var amzDate = request.RequestDateTime.ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'");

            var canonicalHeaders = GetCanonicalHeaders(request.Headers, request.SignedHeaders);

            // Check for unsigned payload
            var payloadHash = request.Headers.ContainsKey("x-amz-content-sha256") &&
                              request.Headers["x-amz-content-sha256"] == "UNSIGNED-PAYLOAD"
                ? "UNSIGNED-PAYLOAD"
                : GetHash(request.Payload);

            // Encode the URI path segments for the canonical request
            var encodedUri = EncodeUri(request.CanonicalUri);

            var canonicalRequest = $"{request.Method}\n{encodedUri}\n{request.CanonicalQueryString}\n{canonicalHeaders}\n{request.SignedHeaders}\n{payloadHash}";

            var algorithm = "AWS4-HMAC-SHA256";
            var credentialScope = $"{dateStamp}/{request.Region}/s3/aws4_request";

            var stringToSign = $"{algorithm}\n{amzDate}\n{credentialScope}\n{GetHash(canonicalRequest)}";

            _logger.LogDebug("Canonical Request: {CanonicalRequest}", canonicalRequest);
            _logger.LogDebug("String to Sign: {StringToSign}", stringToSign);

            var signingKey = GetSigningKey(secretAccessKey, dateStamp, request.Region, "s3");
            var signature = GetHmacSha256Hex(signingKey, stringToSign);

            return Task.FromResult(signature);
        }

        private string GetCanonicalHeaders(Dictionary<string, string> headers, string signedHeaders)
        {
            var signedHeadersList = signedHeaders.Split(';').Select(h => h.Trim().ToLower()).ToList();
            var canonicalHeaders = new SortedDictionary<string, string>(StringComparer.Ordinal);

            foreach (var headerName in signedHeadersList)
            {
                if (headers.TryGetValue(headerName, out var value))
                {
                    canonicalHeaders[headerName] = value.Trim();
                }
            }

            return string.Join("\n", canonicalHeaders.Select(h => $"{h.Key}:{h.Value}")) + "\n";
        }

        private byte[] GetSigningKey(string secretAccessKey, string dateStamp, string region, string service)
        {
            var kSecret = Encoding.UTF8.GetBytes("AWS4" + secretAccessKey);
            var kDate = GetHmacSha256(kSecret, dateStamp);
            var kRegion = GetHmacSha256(kDate, region);
            var kService = GetHmacSha256(kRegion, service);
            var kSigning = GetHmacSha256(kService, "aws4_request");
            return kSigning;
        }

        private byte[] GetHmacSha256(byte[] key, string data)
        {
            using var hmac = new HMACSHA256(key);
            return hmac.ComputeHash(Encoding.UTF8.GetBytes(data));
        }

        private string GetHmacSha256Hex(byte[] key, string data)
        {
            using var hmac = new HMACSHA256(key);
            var hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(data));
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }

        private string GetHash(string text)
        {
            using var sha256 = SHA256.Create();
            var hash = sha256.ComputeHash(Encoding.UTF8.GetBytes(text));
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }

        private string EncodeUri(string uri)
        {
            // For AWS Signature V4, we need to encode each path segment separately
            // Empty path becomes "/"
            if (string.IsNullOrEmpty(uri))
                return "/";

            // Split the path into segments
            var segments = uri.TrimStart('/').Split('/');
            var encodedSegments = new string[segments.Length];

            for (int i = 0; i < segments.Length; i++)
            {
                // Encode each segment using Uri.EscapeDataString
                // This handles special characters and spaces correctly
                encodedSegments[i] = Uri.EscapeDataString(segments[i]);
            }

            // Rejoin with "/" and ensure it starts with "/"
            return "/" + string.Join("/", encodedSegments);
        }
    }
}