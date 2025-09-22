using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using Lamina.Models;
using Lamina.Services;

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
            var context = new DefaultHttpContext();
            context.Request.Headers["Authorization"] = "AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, Signature=abcd1234";
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
            var context = new DefaultHttpContext();
            context.Request.Headers["Authorization"] = "AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, Signature=abcd1234";
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
            var context = new DefaultHttpContext();
            context.Request.Headers["Authorization"] = "AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, Signature=abcd1234";
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

            var validator = await _streamingService.CreateChunkValidatorAsync(context.Request, user);
            Assert.NotNull(validator);

            // Act & Assert - Test chunk validation
            var chunkData = new byte[] { 0x48, 0x65, 0x6c, 0x6c, 0x6f }; // "Hello"
            var chunkSignature = "dummy_signature"; // This would be calculated by the client
            
            // The validation will fail with dummy signature, but we're testing the flow
            var result = await validator.ValidateChunkAsync(chunkData, chunkSignature, false);
            
            // We expect failure with dummy signature, but no exceptions
            Assert.False(result);
            Assert.Equal(0, validator.ChunkIndex); // Should not increment on failed validation
        }

        [Fact]
        public async Task ChunkSignatureValidator_HandlesLastChunk()
        {
            // Arrange
            var context = new DefaultHttpContext();
            context.Request.Headers["Authorization"] = "AWS4-HMAC-SHA256 Credential=TESTKEY/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, Signature=abcd1234";
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

            var validator = await _streamingService.CreateChunkValidatorAsync(context.Request, user);
            Assert.NotNull(validator);

            // Act - Test last chunk (empty)
            var emptyChunk = Array.Empty<byte>();
            var chunkSignature = "dummy_signature";
            
            var result = await validator.ValidateChunkAsync(emptyChunk, chunkSignature, true);
            
            // Assert - Should handle last chunk without exceptions
            Assert.False(result); // Fails due to dummy signature, but that's expected
        }
    }
}