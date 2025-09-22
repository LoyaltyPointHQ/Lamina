using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Http;
using Lamina.Models;

namespace Lamina.Services
{
    public class StreamingAuthenticationService : IStreamingAuthenticationService
    {
        private readonly ILogger<StreamingAuthenticationService> _logger;
        private readonly IAuthenticationService _authService;

        public StreamingAuthenticationService(ILogger<StreamingAuthenticationService> logger, IAuthenticationService authService)
        {
            _logger = logger;
            _authService = authService;
        }

        public async Task<IChunkSignatureValidator?> CreateChunkValidatorAsync(HttpRequest request, S3User user)
        {
            // Extract the authorization header details
            var authHeader = request.Headers["Authorization"].FirstOrDefault();
            if (string.IsNullOrEmpty(authHeader) || !authHeader.StartsWith("AWS4-HMAC-SHA256"))
            {
                return null;
            }

            // Check if this is a streaming request
            var contentSha256 = request.Headers["x-amz-content-sha256"].FirstOrDefault();
            if (contentSha256 != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
            {
                return null;
            }

            // Parse x-amz-decoded-content-length
            var decodedLengthStr = request.Headers["x-amz-decoded-content-length"].FirstOrDefault();
            if (!long.TryParse(decodedLengthStr, out var decodedLength))
            {
                _logger.LogWarning("Invalid or missing x-amz-decoded-content-length header");
                return null;
            }

            // Parse authorization header
            var match = Regex.Match(authHeader, @"AWS4-HMAC-SHA256\s+Credential=([^/]+)/([^/]+)/([^/]+)/s3/aws4_request,\s*SignedHeaders=([^,]+),\s*Signature=([a-f0-9]+)");
            if (!match.Success)
            {
                return null;
            }

            var dateStamp = match.Groups[2].Value;
            var region = match.Groups[3].Value;
            var signedHeaders = match.Groups[4].Value;
            var seedSignature = match.Groups[5].Value;

            _logger.LogDebug("Extracted seed signature from auth header: {SeedSignature}", seedSignature);

            var xAmzDate = request.Headers["x-amz-date"].FirstOrDefault();
            if (string.IsNullOrEmpty(xAmzDate))
            {
                return null;
            }

            var requestDateTime = DateTime.ParseExact(xAmzDate, "yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);

            // Create signing key for chunk validation
            var signingKey = GetSigningKey(user.SecretAccessKey, dateStamp, region, "s3");

            return new ChunkSignatureValidator(
                signingKey,
                requestDateTime,
                region,
                decodedLength,
                request.Path.Value ?? "/",
                GetCanonicalQueryString(request.Query),
                GetHeaders(request.Headers),
                signedHeaders,
                seedSignature,
                _logger);
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

        private string GetCanonicalQueryString(IQueryCollection query)
        {
            var parameters = new SortedDictionary<string, string>(StringComparer.Ordinal);
            foreach (var kvp in query)
            {
                var key = Uri.EscapeDataString(kvp.Key);
                var value = kvp.Value.FirstOrDefault() ?? "";
                parameters[key] = Uri.EscapeDataString(value);
            }
            // S3 API specification: empty query parameters should not have equals sign
            return string.Join("&", parameters.Select(p =>
                string.IsNullOrEmpty(p.Value) ? p.Key : $"{p.Key}={p.Value}"));
        }

        private Dictionary<string, string> GetHeaders(IHeaderDictionary headers)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var header in headers)
            {
                result[header.Key.ToLower()] = string.Join(",", header.Value.ToArray());
            }
            return result;
        }
    }

    internal class ChunkSignatureValidator : IChunkSignatureValidator
    {
        private readonly byte[] _signingKey;
        private readonly DateTime _requestDateTime;
        private readonly string _region;
        private readonly long _expectedDecodedLength;
        private readonly string _canonicalUri;
        private readonly string _canonicalQueryString;
        private readonly Dictionary<string, string> _headers;
        private readonly string _signedHeaders;
        private readonly string _seedSignature;
        private readonly ILogger _logger;
        private int _chunkIndex;
        private string _previousSignature;

        public ChunkSignatureValidator(
            byte[] signingKey,
            DateTime requestDateTime,
            string region,
            long expectedDecodedLength,
            string canonicalUri,
            string canonicalQueryString,
            Dictionary<string, string> headers,
            string signedHeaders,
            string seedSignature,
            ILogger logger)
        {
            _signingKey = signingKey;
            _requestDateTime = requestDateTime;
            _region = region;
            _expectedDecodedLength = expectedDecodedLength;
            _canonicalUri = canonicalUri;
            _canonicalQueryString = canonicalQueryString;
            _headers = headers;
            _signedHeaders = signedHeaders;
            _seedSignature = seedSignature;
            _logger = logger;
            _chunkIndex = 0;

            // Use the provided seed signature from initial request
            _previousSignature = seedSignature;
        }

        public long ExpectedDecodedLength => _expectedDecodedLength;
        public int ChunkIndex => _chunkIndex;

        public async Task<bool> ValidateChunkAsync(ReadOnlyMemory<byte> chunkData, string chunkSignature, bool isLastChunk)
        {
            try
            {
                var expectedSignature = CalculateChunkSignature(chunkData, isLastChunk);
                var isValid = string.Equals(expectedSignature, chunkSignature, StringComparison.OrdinalIgnoreCase);

                if (isValid)
                {
                    _previousSignature = chunkSignature;
                    _chunkIndex++;
                }
                else
                {
                    _logger.LogWarning("Chunk signature validation failed at chunk index {Index}. Expected: {Expected}, Got: {Got}. Previous signature: {Previous}",
                        _chunkIndex, expectedSignature, chunkSignature, _previousSignature);
                }

                return isValid;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error validating chunk signature");
                return false;
            }
        }

        public async Task<bool> ValidateChunkStreamAsync(Stream chunkStream, long chunkSize, string chunkSignature, bool isLastChunk)
        {
            try
            {
                var expectedSignature = await CalculateChunkSignatureStreamAsync(chunkStream, chunkSize, isLastChunk);
                var isValid = string.Equals(expectedSignature, chunkSignature, StringComparison.OrdinalIgnoreCase);

                if (isValid)
                {
                    _previousSignature = chunkSignature;
                    _chunkIndex++;
                }
                else
                {
                    _logger.LogWarning("Chunk signature validation failed at chunk index {Index}. Expected: {Expected}, Got: {Got}. Previous signature: {Previous}",
                        _chunkIndex, expectedSignature, chunkSignature, _previousSignature);
                }

                return isValid;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error validating chunk signature");
                return false;
            }
        }


        private string CalculateChunkSignature(ReadOnlyMemory<byte> chunkData, bool isLastChunk)
        {
            var dateStamp = _requestDateTime.ToString("yyyyMMdd");
            var amzDate = _requestDateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            // AWS Streaming signature format:
            // AWS4-HMAC-SHA256-PAYLOAD\n
            // timestamp\n
            // credential_scope\n
            // previous_signature\n
            // hash_of_empty_string\n
            // chunk_size_in_hex\n
            // chunk_hash

            var algorithm = "AWS4-HMAC-SHA256-PAYLOAD";
            var credentialScope = $"{dateStamp}/{_region}/s3/aws4_request";
            var emptyStringHash = GetHash(Array.Empty<byte>());
            var chunkSize = isLastChunk ? "0" : chunkData.Length.ToString("x");
            var chunkBytes = chunkData.ToArray();
            var chunkHash = GetHash(chunkBytes);

            // Debug: log first few bytes of chunk data
            var debugBytes = chunkBytes.Length > 10 ? chunkBytes.Take(10).ToArray() : chunkBytes;
            var hexString = BitConverter.ToString(debugBytes).Replace("-", " ");

            var stringToSign = $"{algorithm}\n{amzDate}\n{credentialScope}\n{_previousSignature}\n{emptyStringHash}\n{chunkSize}\n{chunkHash}";

            _logger.LogDebug("Chunk signature validation - Size: {Size}, Hash: {Hash}, IsLast: {IsLast}, FirstBytes: {Bytes}",
                chunkData.Length, chunkHash, isLastChunk, hexString);
            _logger.LogDebug("Chunk signature string to sign: {StringToSign}", stringToSign.Replace("\n", "\\n"));

            return GetHmacSha256Hex(_signingKey, stringToSign);
        }

        private async Task<string> CalculateChunkSignatureStreamAsync(Stream chunkStream, long chunkSize, bool isLastChunk)
        {
            var dateStamp = _requestDateTime.ToString("yyyyMMdd");
            var amzDate = _requestDateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            var algorithm = "AWS4-HMAC-SHA256-PAYLOAD";
            var credentialScope = $"{dateStamp}/{_region}/s3/aws4_request";
            var emptyStringHash = GetHash(Array.Empty<byte>());
            var chunkSizeHex = isLastChunk ? "0" : chunkSize.ToString("x");
            var chunkHash = await GetHashFromStreamAsync(chunkStream);

            var stringToSign = $"{algorithm}\n{amzDate}\n{credentialScope}\n{_previousSignature}\n{emptyStringHash}\n{chunkSizeHex}\n{chunkHash}";

            _logger.LogDebug("Chunk signature validation (streaming) - Size: {Size}, Hash: {Hash}, IsLast: {IsLast}",
                chunkSize, chunkHash, isLastChunk);
            _logger.LogDebug("Chunk signature string to sign: {StringToSign}", stringToSign.Replace("\n", "\\n"));

            return GetHmacSha256Hex(_signingKey, stringToSign);
        }

        private string GetHash(byte[] data)
        {
            using var sha256 = SHA256.Create();
            var hash = sha256.ComputeHash(data);
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }

        private async Task<string> GetHashFromStreamAsync(Stream stream)
        {
            using var sha256 = SHA256.Create();
            var originalPosition = stream.Position;
            stream.Position = 0;
            var hash = await sha256.ComputeHashAsync(stream);
            stream.Position = originalPosition;
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }

        private string GetHmacSha256Hex(byte[] key, string data)
        {
            using var hmac = new HMACSHA256(key);
            var hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(data));
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }
    }
}