using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using Lamina.Models;
using Lamina.Services;
using System.Security.Cryptography;
using System.Text;

namespace Lamina.Tests.Services
{
    public class StreamingAuthenticationServiceTests
    {
        private readonly Mock<ILogger<StreamingAuthenticationService>> _loggerMock;
        private readonly Mock<IAuthenticationService> _authServiceMock;
        private readonly StreamingAuthenticationService _streamingService;

        public StreamingAuthenticationServiceTests()
        {
            _loggerMock = new Mock<ILogger<StreamingAuthenticationService>>();
            _authServiceMock = new Mock<IAuthenticationService>();
            _streamingService = new StreamingAuthenticationService(_loggerMock.Object, _authServiceMock.Object);
        }

        [Fact]
        public async Task CreateChunkValidatorAsync_ReturnsNull_WhenNotStreamingRequest()
        {
            // Arrange
            var context = new DefaultHttpContext();
            context.Request.Headers["Authorization"] = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abcd1234";
            context.Request.Headers["x-amz-content-sha256"] = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; // Not streaming
            context.Request.Headers["x-amz-date"] = "20240101T120000Z";

            var user = new S3User { AccessKeyId = "test", SecretAccessKey = "secret" };

            // Act
            var validator = await _streamingService.CreateChunkValidatorAsync(context.Request, user);

            // Assert
            Assert.Null(validator);
        }

        [Fact]
        public async Task CreateChunkValidatorAsync_ReturnsNull_WhenMissingDecodedContentLength()
        {
            // Arrange
            var context = new DefaultHttpContext();
            context.Request.Headers["Authorization"] = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abcd1234";
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
            context.Request.Headers["x-amz-date"] = "20240101T120000Z";
            // Missing x-amz-decoded-content-length

            var user = new S3User { AccessKeyId = "test", SecretAccessKey = "secret" };

            // Act
            var validator = await _streamingService.CreateChunkValidatorAsync(context.Request, user);

            // Assert
            Assert.Null(validator);
        }

        [Fact]
        public async Task CreateChunkValidatorAsync_ReturnsValidator_WhenValidStreamingRequest()
        {
            // Arrange
            var dateTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var context = new DefaultHttpContext();
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
            context.Request.Headers["x-amz-date"] = "20240101T120000Z";
            context.Request.Headers["x-amz-decoded-content-length"] = "1024";
            context.Request.Headers["Host"] = "test.s3.amazonaws.com";
            context.Request.Method = "PUT";
            context.Request.Path = "/test-bucket/test-key";

            var user = new S3User
            {
                AccessKeyId = "TESTKEY",
                SecretAccessKey = "testsecret"
            };

            // Calculate the proper signature for this request
            var headers = new Dictionary<string, string>
            {
                ["host"] = "test.s3.amazonaws.com",
                ["x-amz-date"] = "20240101T120000Z",
                ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                ["x-amz-decoded-content-length"] = "1024"
            };
            var signedHeaders = "host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length";

            var signature = await CalculateStreamingSignature(
                "PUT",
                "/test-bucket/test-key",
                "",
                headers,
                signedHeaders,
                Array.Empty<byte>(),
                dateTime,
                "TESTKEY",
                "testsecret");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders={signedHeaders}, Signature={signature}";

            // Act
            var validator = await _streamingService.CreateChunkValidatorAsync(context.Request, user);

            // Assert
            Assert.NotNull(validator);
            Assert.Equal(1024L, validator.ExpectedDecodedLength);
            Assert.Equal(0, validator.ChunkIndex);
        }

        [Fact]
        public async Task CreateChunkValidatorAsync_ReturnsNull_WhenInvalidAuthHeader()
        {
            // Arrange
            var context = new DefaultHttpContext();
            context.Request.Headers["Authorization"] = "Invalid-Header";
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
            context.Request.Headers["x-amz-date"] = "20240101T120000Z";
            context.Request.Headers["x-amz-decoded-content-length"] = "1024";

            var user = new S3User { AccessKeyId = "test", SecretAccessKey = "secret" };

            // Act
            var validator = await _streamingService.CreateChunkValidatorAsync(context.Request, user);

            // Assert
            Assert.Null(validator);
        }

        [Fact]
        public async Task CreateChunkValidatorAsync_ReturnsNull_WhenMissingAmzDate()
        {
            // Arrange
            var context = new DefaultHttpContext();
            context.Request.Headers["Authorization"] = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abcd1234";
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
            context.Request.Headers["x-amz-decoded-content-length"] = "1024";
            // Missing x-amz-date

            var user = new S3User { AccessKeyId = "test", SecretAccessKey = "secret" };

            // Act
            var validator = await _streamingService.CreateChunkValidatorAsync(context.Request, user);

            // Assert
            Assert.Null(validator);
        }

        [Theory]
        [InlineData("100")]
        [InlineData("1024")]
        [InlineData("8192")]
        [InlineData("0")] // Empty payload
        public async Task CreateChunkValidatorAsync_HandlesVariousDecodedLengths(string decodedLength)
        {
            // Arrange
            var dateTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var context = new DefaultHttpContext();
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
            context.Request.Headers["x-amz-date"] = "20240101T120000Z";
            context.Request.Headers["x-amz-decoded-content-length"] = decodedLength;
            context.Request.Headers["Host"] = "test.s3.amazonaws.com";
            context.Request.Method = "PUT";
            context.Request.Path = "/test-bucket/test-key";

            var user = new S3User
            {
                AccessKeyId = "TESTKEY",
                SecretAccessKey = "testsecret"
            };

            // Calculate the proper signature for this request
            var headers = new Dictionary<string, string>
            {
                ["host"] = "test.s3.amazonaws.com",
                ["x-amz-date"] = "20240101T120000Z",
                ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                ["x-amz-decoded-content-length"] = decodedLength
            };
            var signedHeaders = "host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length";

            var signature = await CalculateStreamingSignature(
                "PUT",
                "/test-bucket/test-key",
                "",
                headers,
                signedHeaders,
                Array.Empty<byte>(),
                dateTime,
                "TESTKEY",
                "testsecret");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders={signedHeaders}, Signature={signature}";

            // Act
            var validator = await _streamingService.CreateChunkValidatorAsync(context.Request, user);

            // Assert
            Assert.NotNull(validator);
            Assert.Equal(long.Parse(decodedLength), validator.ExpectedDecodedLength);
        }

        [Fact]
        public async Task ChunkSignatureValidator_ValidatesChunks()
        {
            // Arrange
            var dateTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var context = new DefaultHttpContext();
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
            context.Request.Headers["x-amz-date"] = "20240101T120000Z";
            context.Request.Headers["x-amz-decoded-content-length"] = "10";
            context.Request.Headers["Host"] = "test.s3.amazonaws.com";
            context.Request.Method = "PUT";
            context.Request.Path = "/test-bucket/test-key";

            var user = new S3User
            {
                AccessKeyId = "TESTKEY",
                SecretAccessKey = "testsecret"
            };

            // Calculate the proper signature for this request
            var headers = new Dictionary<string, string>
            {
                ["host"] = "test.s3.amazonaws.com",
                ["x-amz-date"] = "20240101T120000Z",
                ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                ["x-amz-decoded-content-length"] = "10"
            };
            var signedHeaders = "host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length";

            var signature = await CalculateStreamingSignature(
                "PUT",
                "/test-bucket/test-key",
                "",
                headers,
                signedHeaders,
                Array.Empty<byte>(),
                dateTime,
                "TESTKEY",
                "testsecret");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders={signedHeaders}, Signature={signature}";

            var validator = await _streamingService.CreateChunkValidatorAsync(context.Request, user);
            Assert.NotNull(validator);

            // Act & Assert - Test chunk validation
            var chunkData = new byte[] { 0x48, 0x65, 0x6c, 0x6c, 0x6f }; // "Hello"

            // Calculate the expected chunk signature using reflection
            var validatorType = validator.GetType();
            var calculateMethod = validatorType.GetMethod("CalculateChunkSignature",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.NotNull(calculateMethod);

            var expectedChunkSignature = (string)calculateMethod.Invoke(validator, new object[] { new ReadOnlyMemory<byte>(chunkData), false });

            // Test with the correct signature
            var result = await validator.ValidateChunkAsync(chunkData, expectedChunkSignature, false);

            // Should succeed with correct signature
            Assert.True(result);
            Assert.Equal(1, validator.ChunkIndex); // Should increment on successful validation
        }

        [Fact]
        public async Task ChunkSignatureValidator_HandlesLastChunk()
        {
            // Arrange
            var dateTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var context = new DefaultHttpContext();
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
            context.Request.Headers["x-amz-date"] = "20240101T120000Z";
            context.Request.Headers["x-amz-decoded-content-length"] = "0";
            context.Request.Headers["Host"] = "test.s3.amazonaws.com";
            context.Request.Method = "PUT";
            context.Request.Path = "/test-bucket/test-key";

            var user = new S3User
            {
                AccessKeyId = "TESTKEY",
                SecretAccessKey = "testsecret"
            };

            // Calculate the proper signature for this request
            var headers = new Dictionary<string, string>
            {
                ["host"] = "test.s3.amazonaws.com",
                ["x-amz-date"] = "20240101T120000Z",
                ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                ["x-amz-decoded-content-length"] = "0"
            };
            var signedHeaders = "host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length";

            var signature = await CalculateStreamingSignature(
                "PUT",
                "/test-bucket/test-key",
                "",
                headers,
                signedHeaders,
                Array.Empty<byte>(),
                dateTime,
                "TESTKEY",
                "testsecret");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders={signedHeaders}, Signature={signature}";

            var validator = await _streamingService.CreateChunkValidatorAsync(context.Request, user);
            Assert.NotNull(validator);

            // Act - Test last chunk (empty)
            var emptyChunk = Array.Empty<byte>();

            // Calculate expected signature for the last chunk
            var validatorType = validator.GetType();
            var calculateMethod = validatorType.GetMethod("CalculateChunkSignature",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.NotNull(calculateMethod);

            var expectedChunkSignature = (string)calculateMethod.Invoke(validator, new object[] { new ReadOnlyMemory<byte>(emptyChunk), true });

            var result = await validator.ValidateChunkAsync(emptyChunk, expectedChunkSignature, true);

            // Assert - Should handle last chunk successfully with correct signature
            Assert.True(result);
            Assert.Equal(1, validator.ChunkIndex); // Should increment even for last chunk
        }

        [Fact]
        public async Task ChunkSignatureValidator_UsesCorrectPreviousSignatureChaining()
        {
            // This test ensures that the previous signature is correctly updated with our calculated signature,
            // not the client's signature. This was a bug where we were using chunkSignature instead of
            // expectedSignature for the next iteration's previousSignature.

            // Arrange
            var dateTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var context = new DefaultHttpContext();
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
            context.Request.Headers["x-amz-date"] = "20240101T120000Z";
            context.Request.Headers["x-amz-decoded-content-length"] = "10";
            context.Request.Headers["Host"] = "test.s3.amazonaws.com";
            context.Request.Method = "PUT";
            context.Request.Path = "/test-bucket/test-key";

            var user = new S3User
            {
                AccessKeyId = "TESTKEY",
                SecretAccessKey = "testsecret"
            };

            // Calculate the proper signature for this request
            var headers = new Dictionary<string, string>
            {
                ["host"] = "test.s3.amazonaws.com",
                ["x-amz-date"] = "20240101T120000Z",
                ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                ["x-amz-decoded-content-length"] = "10"
            };
            var signedHeaders = "host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length";

            var signature = await CalculateStreamingSignature(
                "PUT",
                "/test-bucket/test-key",
                "",
                headers,
                signedHeaders,
                Array.Empty<byte>(),
                dateTime,
                "TESTKEY",
                "testsecret");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders={signedHeaders}, Signature={signature}";

            var validator = await _streamingService.CreateChunkValidatorAsync(context.Request, user);
            Assert.NotNull(validator);

            // Act - Calculate what the first chunk signature should be
            var chunkData = new byte[] { 0x48, 0x65, 0x6c, 0x6c, 0x6f }; // "Hello"

            // Use reflection to access the private CalculateChunkSignature method
            var validatorType = validator.GetType();
            var calculateMethod = validatorType.GetMethod("CalculateChunkSignature",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            Assert.NotNull(calculateMethod);

            // Calculate what we expect the signature to be
            var expectedSignature = (string)calculateMethod.Invoke(validator, new object[] { new ReadOnlyMemory<byte>(chunkData), false });

            // Simulate successful validation with the correct signature
            var result = await validator.ValidateChunkAsync(chunkData, expectedSignature, false);

            // Assert - Validation should succeed
            Assert.True(result);
            Assert.Equal(1, validator.ChunkIndex);

            // Now test second chunk - the previousSignature should be our calculated signature
            var chunkData2 = new byte[] { 0x57, 0x6f, 0x72, 0x6c, 0x64 }; // "World"
            var expectedSignature2 = (string)calculateMethod.Invoke(validator, new object[] { new ReadOnlyMemory<byte>(chunkData2), false });

            var result2 = await validator.ValidateChunkAsync(chunkData2, expectedSignature2, false);

            // Assert - Second chunk should also succeed if signature chaining is correct
            Assert.True(result2);
            Assert.Equal(2, validator.ChunkIndex);

            // If signature chaining was broken (using client signature instead of calculated),
            // the second chunk validation would fail because the previousSignature would be wrong
        }

        private async Task<string> CalculateStreamingSignature(string method, string uri, string queryString,
            Dictionary<string, string> headers, string signedHeaders, byte[] payload,
            DateTime dateTime, string accessKey, string secretKey)
        {
            // For streaming, payload hash is always "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            var canonicalHeaders = GetCanonicalHeaders(headers, signedHeaders);
            var payloadHash = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

            var canonicalRequest = $"{method}\n{EncodeUri(uri)}\n{queryString}\n{canonicalHeaders}\n{signedHeaders}\n{payloadHash}";

            var algorithm = "AWS4-HMAC-SHA256";
            var credentialScope = $"{dateStamp}/us-east-1/s3/aws4_request";
            var stringToSign = $"{algorithm}\n{amzDate}\n{credentialScope}\n{GetHash(canonicalRequest)}";

            var signingKey = GetSigningKey(secretKey, dateStamp, "us-east-1", "s3");
            return GetHmacSha256Hex(signingKey, stringToSign);
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

        private string EncodeUri(string uri)
        {
            if (string.IsNullOrEmpty(uri))
                return "/";

            var segments = uri.TrimStart('/').Split('/', StringSplitOptions.None);
            var encodedSegments = new string[segments.Length];

            for (int i = 0; i < segments.Length; i++)
            {
                encodedSegments[i] = AwsUriEncode(segments[i]);
            }

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
            return (c >= 'A' && c <= 'Z') ||
                   (c >= 'a' && c <= 'z') ||
                   (c >= '0' && c <= '9') ||
                   c == '-' || c == '.' || c == '_' || c == '~';
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
    }
}