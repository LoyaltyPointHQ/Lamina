using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using System.Security.Cryptography;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.WebApi.Services;
using Lamina.WebApi.Streaming;
using Lamina.WebApi.Streaming.Validation;

namespace Lamina.WebApi.Tests.Streaming.Trailers
{
    public class StreamingTrailerSupportTests
    {
        private readonly Mock<ILogger<StreamingAuthenticationService>> _loggerMock;
        private readonly Mock<IAuthenticationService> _authServiceMock;
        private readonly StreamingAuthenticationService _streamingService;

        public StreamingTrailerSupportTests()
        {
            _loggerMock = new Mock<ILogger<StreamingAuthenticationService>>();
            _authServiceMock = new Mock<IAuthenticationService>();
            _streamingService = new StreamingAuthenticationService(_loggerMock.Object, _authServiceMock.Object);
        }

        [Fact]
        public async Task CreateChunkValidatorAsync_DetectsTrailerStreaming()
        {
            // Arrange
            var dateTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var context = new DefaultHttpContext();
            context.Request.Headers["Host"] = "test.s3.amazonaws.com";
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
            context.Request.Headers["x-amz-date"] = "20240101T120000Z";
            context.Request.Headers["x-amz-decoded-content-length"] = "1024";
            context.Request.Headers["x-amz-trailer"] = "x-amz-checksum-crc32c,x-amz-checksum-sha256";
            context.Request.Method = "PUT";
            context.Request.Path = "/test-bucket/test-key";

            var user = new S3User
            {
                AccessKeyId = "TESTKEY",
                SecretAccessKey = "testsecret"
            };

            // Calculate proper signature for trailer streaming
            var headers = new Dictionary<string, string>
            {
                ["host"] = "test.s3.amazonaws.com",
                ["x-amz-date"] = "20240101T120000Z",
                ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
                ["x-amz-decoded-content-length"] = "1024",
                ["x-amz-trailer"] = "x-amz-checksum-crc32c,x-amz-checksum-sha256"
            };
            var signedHeaders = "host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer";

            var signature = await CalculateStreamingSignature(
                "PUT",
                "/test-bucket/test-key",
                "",
                headers,
                signedHeaders,
                Array.Empty<byte>(),
                dateTime,
                "TESTKEY",
                "testsecret",
                isTrailerStreaming: true);

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders={signedHeaders}, Signature={signature}";

            // Act
            var validator = _streamingService.CreateChunkValidator(context.Request, user);

            // Assert
            Assert.NotNull(validator);
            Assert.True(validator.ExpectsTrailers);
            Assert.Equal(2, validator.ExpectedTrailerNames.Count);
            Assert.Contains("x-amz-checksum-crc32c", validator.ExpectedTrailerNames);
            Assert.Contains("x-amz-checksum-sha256", validator.ExpectedTrailerNames);
        }

        [Fact]
        public async Task CreateChunkValidatorAsync_HandlesMissingTrailerHeader()
        {
            // Arrange
            var dateTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var context = new DefaultHttpContext();
            context.Request.Headers["Host"] = "test.s3.amazonaws.com";
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
            context.Request.Headers["x-amz-date"] = "20240101T120000Z";
            context.Request.Headers["x-amz-decoded-content-length"] = "1024";
            context.Request.Method = "PUT";
            context.Request.Path = "/test-bucket/test-key";

            var user = new S3User
            {
                AccessKeyId = "TESTKEY",
                SecretAccessKey = "testsecret"
            };

            // Calculate proper signature for trailer streaming without x-amz-trailer header
            var headers = new Dictionary<string, string>
            {
                ["host"] = "test.s3.amazonaws.com",
                ["x-amz-date"] = "20240101T120000Z",
                ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
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
                "testsecret",
                isTrailerStreaming: true);

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders={signedHeaders}, Signature={signature}";

            // Act
            var validator = _streamingService.CreateChunkValidator(context.Request, user);

            // Assert
            Assert.NotNull(validator);
            Assert.True(validator.ExpectsTrailers);
            Assert.Empty(validator.ExpectedTrailerNames);
        }

        [Fact]
        public async Task CreateChunkValidatorAsync_RegularStreamingStillWorks()
        {
            // Arrange
            var dateTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var context = new DefaultHttpContext();
            context.Request.Headers["Host"] = "test.s3.amazonaws.com";
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
            context.Request.Headers["x-amz-date"] = "20240101T120000Z";
            context.Request.Headers["x-amz-decoded-content-length"] = "1024";
            context.Request.Method = "PUT";
            context.Request.Path = "/test-bucket/test-key";

            var user = new S3User
            {
                AccessKeyId = "TESTKEY",
                SecretAccessKey = "testsecret"
            };

            // Calculate proper signature for regular streaming
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
                "testsecret",
                isTrailerStreaming: false);

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders={signedHeaders}, Signature={signature}";

            // Act
            var validator = _streamingService.CreateChunkValidator(context.Request, user);

            // Assert
            Assert.NotNull(validator);
            Assert.False(validator.ExpectsTrailers);
            Assert.Empty(validator.ExpectedTrailerNames);
        }

        [Fact]
        public async Task ValidateTrailerAsync_ValidatesCorrectlyWithValidSignature()
        {
            // Arrange
            var validator = await CreateTestTrailerValidator();

            var trailers = new List<StreamingTrailer>
            {
                new() { Name = "x-amz-checksum-crc32c", Value = "wdBDMA==" },
                new() { Name = "x-amz-checksum-sha256", Value = "abc123def456" }
            };

            // Calculate expected trailer signature
            var expectedSignature = await CalculateTrailerSignatureForTest(validator, trailers);

            // Act
            var result = validator.ValidateTrailer(trailers, expectedSignature);

            // Assert
            Assert.True(result.IsValid);
            Assert.Equal(2, result.Trailers.Count);
            Assert.Null(result.ErrorMessage);
        }

        [Fact]
        public async Task ValidateTrailerAsync_FailsWithInvalidSignature()
        {
            // Arrange
            var validator = await CreateTestTrailerValidator();

            var trailers = new List<StreamingTrailer>
            {
                new() { Name = "x-amz-checksum-crc32c", Value = "wdBDMA==" }
            };

            var invalidSignature = "invalid_signature";

            // Act
            var result = validator.ValidateTrailer(trailers, invalidSignature);

            // Assert
            Assert.False(result.IsValid);
            Assert.NotNull(result.ErrorMessage);
            Assert.Contains("Trailer signature validation failed", result.ErrorMessage);
        }

        [Fact]
        public async Task ValidateTrailerAsync_FailsWithMissingExpectedTrailers()
        {
            // Arrange
            var validator = await CreateTestTrailerValidatorWithExpectedTrailers(new[] { "x-amz-checksum-crc32c", "x-amz-checksum-sha256" });

            var trailers = new List<StreamingTrailer>
            {
                new() { Name = "x-amz-checksum-crc32c", Value = "wdBDMA==" }
                // Missing x-amz-checksum-sha256
            };

            var dummySignature = "dummy";

            // Act
            var result = validator.ValidateTrailer(trailers, dummySignature);

            // Assert
            Assert.False(result.IsValid);
            Assert.NotNull(result.ErrorMessage);
            Assert.Contains("Missing expected trailers", result.ErrorMessage);
            Assert.Contains("x-amz-checksum-sha256", result.ErrorMessage);
        }

        [Fact]
        public async Task ValidateTrailerAsync_FailsWhenNotExpectingTrailers()
        {
            // Arrange
            var validator = await CreateTestNonTrailerValidator();

            var trailers = new List<StreamingTrailer>
            {
                new() { Name = "x-amz-checksum-crc32c", Value = "wdBDMA==" }
            };

            var dummySignature = "dummy";

            // Act
            var result = validator.ValidateTrailer(trailers, dummySignature);

            // Assert
            Assert.False(result.IsValid);
            Assert.NotNull(result.ErrorMessage);
            Assert.Contains("This validator does not expect trailers", result.ErrorMessage);
        }

        [Theory]
        [InlineData("x-amz-checksum-crc32c", "wdBDMA==")]
        [InlineData("x-amz-checksum-sha256", "abc123def456")]
        [InlineData("x-amz-checksum-md5", "098f6bcd4621d373cade4e832627b4f6")]
        public async Task ValidateTrailerAsync_HandlesVariousChecksumTypes(string trailerName, string trailerValue)
        {
            // Arrange
            var validator = await CreateTestTrailerValidator();

            var trailers = new List<StreamingTrailer>
            {
                new() { Name = trailerName, Value = trailerValue }
            };

            var expectedSignature = await CalculateTrailerSignatureForTest(validator, trailers);

            // Act
            var result = validator.ValidateTrailer(trailers, expectedSignature);

            // Assert
            Assert.True(result.IsValid);
            Assert.Single(result.Trailers);
            Assert.Equal(trailerName, result.Trailers[0].Name);
            Assert.Equal(trailerValue, result.Trailers[0].Value);
        }

        private async Task<IChunkSignatureValidator> CreateTestTrailerValidator()
        {
            return await CreateTestTrailerValidatorWithExpectedTrailers(Array.Empty<string>());
        }

        private async Task<IChunkSignatureValidator> CreateTestTrailerValidatorWithExpectedTrailers(string[] expectedTrailers)
        {
            var dateTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var context = new DefaultHttpContext();
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
            context.Request.Headers["x-amz-date"] = "20240101T120000Z";
            context.Request.Headers["x-amz-decoded-content-length"] = "0";
            context.Request.Headers["host"] = "test.s3.amazonaws.com";
            if (expectedTrailers.Length > 0)
            {
                context.Request.Headers["x-amz-trailer"] = string.Join(",", expectedTrailers);
            }
            context.Request.Method = "PUT";
            context.Request.Path = "/test-bucket/test-key";

            var user = new S3User
            {
                AccessKeyId = "TESTKEY",
                SecretAccessKey = "testsecret"
            };

            var headers = new Dictionary<string, string>
            {
                ["host"] = "test.s3.amazonaws.com",
                ["x-amz-date"] = "20240101T120000Z",
                ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
                ["x-amz-decoded-content-length"] = "0"
            };

            if (expectedTrailers.Length > 0)
            {
                headers["x-amz-trailer"] = string.Join(",", expectedTrailers);
            }

            var signedHeaders = expectedTrailers.Length > 0
                ? "host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer"
                : "host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length";

            var signature = await CalculateStreamingSignature(
                "PUT",
                "/test-bucket/test-key",
                "",
                headers,
                signedHeaders,
                Array.Empty<byte>(),
                dateTime,
                "TESTKEY",
                "testsecret",
                isTrailerStreaming: true);

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders={signedHeaders}, Signature={signature}";

            var validator = _streamingService.CreateChunkValidator(context.Request, user);
            return validator!;
        }

        private async Task<IChunkSignatureValidator> CreateTestNonTrailerValidator()
        {
            var dateTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var context = new DefaultHttpContext();
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
            context.Request.Headers["x-amz-date"] = "20240101T120000Z";
            context.Request.Headers["x-amz-decoded-content-length"] = "0";
            context.Request.Headers["host"] = "test.s3.amazonaws.com";
            context.Request.Method = "PUT";
            context.Request.Path = "/test-bucket/test-key";

            var user = new S3User
            {
                AccessKeyId = "TESTKEY",
                SecretAccessKey = "testsecret"
            };

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
                "testsecret",
                isTrailerStreaming: false);

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders={signedHeaders}, Signature={signature}";

            var validator = _streamingService.CreateChunkValidator(context.Request, user);
            return validator!;
        }

        private Task<string> CalculateTrailerSignatureForTest(IChunkSignatureValidator validator, List<StreamingTrailer> trailers)
        {
            // Cast to concrete type to access internal properties
            var concreteValidator = (ChunkSignatureValidator)validator;

            // Build trailer header string and calculate signature using SignatureCalculator
            var trailerHeaderString = SignatureCalculator.BuildTrailerHeaderString(trailers);
            return Task.FromResult(SignatureCalculator.CalculateTrailerSignature(
                concreteValidator.SigningKey,
                concreteValidator.RequestDateTime,
                concreteValidator.Region,
                concreteValidator.PreviousSignature,
                trailerHeaderString));
        }

        private Task<string> CalculateStreamingSignature(string method, string uri, string queryString,
            Dictionary<string, string> headers, string signedHeaders, byte[] payload,
            DateTime dateTime, string accessKey, string secretKey, bool isTrailerStreaming = false)
        {
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            var canonicalHeaders = GetCanonicalHeaders(headers, signedHeaders);
            var payloadHash = isTrailerStreaming
                ? "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
                : "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

            var canonicalRequest = $"{method}\n{EncodeUri(uri)}\n{queryString}\n{canonicalHeaders}\n{signedHeaders}\n{payloadHash}";

            var algorithm = "AWS4-HMAC-SHA256";
            var credentialScope = $"{dateStamp}/us-east-1/s3/aws4_request";
            var stringToSign = $"{algorithm}\n{amzDate}\n{credentialScope}\n{GetHash(canonicalRequest)}";

            var signingKey = GetSigningKey(secretKey, dateStamp, "us-east-1", "s3");
            return Task.FromResult(GetHmacSha256Hex(signingKey, stringToSign));
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

        private string GetHash(string text)
        {
            using var sha256 = SHA256.Create();
            var hash = sha256.ComputeHash(Encoding.UTF8.GetBytes(text));
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }
    }
}