using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Lamina.Core.Models;
using Microsoft.Extensions.Options;

namespace Lamina.WebApi.Services
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
                SignatureV4Request? sigV4Request = null;
                var authHeader = request.Headers["Authorization"].FirstOrDefault();

                // Try header-based authentication first
                if (!string.IsNullOrEmpty(authHeader))
                {
                    if (!authHeader.StartsWith("AWS4-HMAC-SHA256"))
                    {
                        return (false, null, "Unsupported authentication method");
                    }

                    sigV4Request = ParseSignatureV4Request(request, authHeader);
                    if (sigV4Request == null)
                    {
                        return (false, null, "Invalid authorization header format");
                    }
                }
                else
                {
                    // Try presigned URL authentication
                    var algorithm = request.Query["X-Amz-Algorithm"].FirstOrDefault();
                    if (!string.IsNullOrEmpty(algorithm))
                    {
                        sigV4Request = ParsePresignedUrlRequest(request);
                        if (sigV4Request == null)
                        {
                            return (false, null, "Invalid presigned URL format");
                        }

                        // Validate expiration for presigned URLs
                        var (isValidExpiration, expirationError) = ValidatePresignedUrlExpiration(request, sigV4Request.RequestDateTime);
                        if (!isValidExpiration)
                        {
                            return (false, null, expirationError);
                        }
                    }
                    else
                    {
                        return (false, null, "Missing Authorization header");
                    }
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

        private SignatureV4Request? ParseSignatureV4Request(HttpRequest request, string authHeader)
        {
            var match = Regex.Match(authHeader, @"AWS4-HMAC-SHA256\s+Credential=([^/]+)/([^/]+)/([^/]+)/s3/aws4_request,\s*SignedHeaders=([^,]+),\s*Signature=([a-fA-F0-9]+)");
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

            // We don't read the request body here - follow AWS/MinIO specification
            // For streaming payloads, we leave payload empty and handle validation differently

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
                Payload = [],
                Region = region,
                Service = "s3",
                RequestDateTime = DateTime.ParseExact(xAmzDate, "yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal),
                AccessKeyId = accessKeyId,
                Signature = signature,
                SignedHeaders = signedHeaders
            };
        }

        private SignatureV4Request? ParsePresignedUrlRequest(HttpRequest request)
        {
            // Extract authentication parameters from query string
            var algorithm = request.Query["X-Amz-Algorithm"].FirstOrDefault();
            var credential = request.Query["X-Amz-Credential"].FirstOrDefault();
            var xAmzDate = request.Query["X-Amz-Date"].FirstOrDefault();
            var signedHeaders = request.Query["X-Amz-SignedHeaders"].FirstOrDefault();
            var signature = request.Query["X-Amz-Signature"].FirstOrDefault();

            if (string.IsNullOrEmpty(algorithm) || string.IsNullOrEmpty(credential) ||
                string.IsNullOrEmpty(xAmzDate) || string.IsNullOrEmpty(signedHeaders) ||
                string.IsNullOrEmpty(signature))
            {
                return null;
            }

            if (algorithm != "AWS4-HMAC-SHA256")
            {
                return null;
            }

            // Parse credential: accessKeyId/date/region/service/aws4_request
            var credentialParts = credential.Split('/');
            if (credentialParts.Length != 5)
            {
                return null;
            }

            var accessKeyId = credentialParts[0];
            var dateStamp = credentialParts[1];
            var region = credentialParts[2];

            // Build headers dictionary from request headers
            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var header in request.Headers)
            {
                headers[header.Key.ToLower()] = string.Join(",", header.Value.ToArray());
            }

            // Add Host header if not present (required for presigned URLs)
            if (!headers.ContainsKey("host"))
            {
                headers["host"] = request.Host.HasValue ? request.Host.Value : "localhost";
            }

            // For presigned URLs, x-amz-content-sha256 is always UNSIGNED-PAYLOAD
            headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD";

            var canonicalUri = request.Path.Value ?? "/";
            // Exclude X-Amz-Signature from canonical query string
            var canonicalQueryString = GetCanonicalQueryString(request.Query, excludeSignature: true);

            return new SignatureV4Request
            {
                Method = request.Method,
                CanonicalUri = canonicalUri,
                CanonicalQueryString = canonicalQueryString,
                Headers = headers,
                Payload = [],
                Region = region,
                Service = "s3",
                RequestDateTime = DateTime.ParseExact(xAmzDate, "yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal),
                AccessKeyId = accessKeyId,
                Signature = signature,
                SignedHeaders = signedHeaders
            };
        }

        private (bool isValid, string? error) ValidatePresignedUrlExpiration(HttpRequest request, DateTime requestDateTime)
        {
            var expiresParam = request.Query["X-Amz-Expires"].FirstOrDefault();
            if (string.IsNullOrEmpty(expiresParam))
            {
                // Expires is optional but recommended
                return (true, null);
            }

            if (!int.TryParse(expiresParam, out var expiresSeconds))
            {
                return (false, "Invalid X-Amz-Expires value");
            }

            // AWS S3 maximum expiration is 7 days (604800 seconds)
            if (expiresSeconds > 604800)
            {
                return (false, "X-Amz-Expires exceeds maximum of 7 days");
            }

            if (expiresSeconds < 0)
            {
                return (false, "X-Amz-Expires must be positive");
            }

            // Ensure both DateTimes are in UTC for correct comparison
            var requestTimeUtc = requestDateTime.ToUniversalTime();
            var expirationTime = requestTimeUtc.AddSeconds(expiresSeconds);
            var currentTime = DateTime.UtcNow;

            if (currentTime > expirationTime)
            {
                return (false, "Presigned URL has expired");
            }

            return (true, null);
        }

        private string GetCanonicalQueryString(IQueryCollection query, bool excludeSignature = false)
        {
            var parameters = new SortedDictionary<string, string>(StringComparer.Ordinal);
            foreach (var kvp in query)
            {
                // Skip X-Amz-Signature when building canonical query string for presigned URL validation
                if (excludeSignature && kvp.Key.Equals("X-Amz-Signature", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var key = Uri.EscapeDataString(kvp.Key);
                var value = kvp.Value.FirstOrDefault() ?? "";
                parameters[key] = Uri.EscapeDataString(value);
            }
            // S3 API specification: always include equals sign for query parameters
            // This matches real AWS client behavior (AWS CLI, rclone send ?uploads=)
            return string.Join("&", parameters.Select(p => $"{p.Key}={p.Value}"));
        }

        public Task<string> CalculateSignatureV4(SignatureV4Request request, string secretAccessKey)
        {
            var dateStamp = request.RequestDateTime.ToUniversalTime().ToString("yyyyMMdd");
            var amzDate = request.RequestDateTime.ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'");

            var canonicalHeaders = GetCanonicalHeaders(request.Headers, request.SignedHeaders);

            // Check for special payload hash values
            var payloadHash = request.Headers.ContainsKey("x-amz-content-sha256") switch
            {
                true when request.Headers["x-amz-content-sha256"] == "UNSIGNED-PAYLOAD" => "UNSIGNED-PAYLOAD",
                true when request.Headers["x-amz-content-sha256"] == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" => "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                true when request.Headers["x-amz-content-sha256"] == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" => "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
                true => request.Headers["x-amz-content-sha256"], // Use provided hash
                // Use empty SHA256 when header is missing (AWS/MinIO specification) - no need to read body
                false => "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            };

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

        private string GetHash(byte[] data)
        {
            using var sha256 = SHA256.Create();
            var hash = sha256.ComputeHash(data);
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }

        private string GetHash(string text)
        {
            return GetHash(Encoding.UTF8.GetBytes(text));
        }

        private string EncodeUri(string uri)
        {
            // For AWS Signature V4, we need to encode each path segment separately
            // Empty path becomes "/"
            if (string.IsNullOrEmpty(uri))
                return "/";

            // AWS Signature V4 canonical URI encoding rules:
            // - Don't encode forward slashes (/)
            // - Encode unreserved characters according to RFC 3986
            // - Double-encode already encoded characters
            
            // Split the path into segments, but preserve empty segments for consecutive slashes
            var segments = uri.TrimStart('/').Split('/', StringSplitOptions.None);
            var encodedSegments = new string[segments.Length];

            for (int i = 0; i < segments.Length; i++)
            {
                // AWS Signature V4 specific encoding for each segment
                encodedSegments[i] = AwsUriEncode(segments[i]);
            }

            // Rejoin with "/" and ensure it starts with "/"
            return "/" + string.Join("/", encodedSegments);
        }

        private string AwsUriEncode(string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;

            var result = new StringBuilder();
            foreach (char c in value)
            {
                if (IsUnreservedCharacter(c))
                {
                    result.Append(c);
                }
                else
                {
                    // Convert to bytes and encode each byte as %XX
                    var bytes = Encoding.UTF8.GetBytes(c.ToString());
                    foreach (byte b in bytes)
                    {
                        result.Append($"%{b:X2}");
                    }
                }
            }
            return result.ToString();
        }

        private bool IsUnreservedCharacter(char c)
        {
            // RFC 3986 unreserved characters: A-Z a-z 0-9 - . _ ~
            return (c >= 'A' && c <= 'Z') ||
                   (c >= 'a' && c <= 'z') ||
                   (c >= '0' && c <= '9') ||
                   c == '-' || c == '.' || c == '_' || c == '~';
        }
    }
}