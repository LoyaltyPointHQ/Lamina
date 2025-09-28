using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Security.Cryptography;
using System.Globalization;
using Lamina.Core.Models;
using Lamina.WebApi.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using Xunit.Abstractions;

namespace Lamina.WebApi.Tests.Services
{
    public class AuthenticationRequestParsingTests
    {
        private readonly ITestOutputHelper _output;
        private readonly Mock<ILogger<AuthenticationService>> _loggerMock;
        private readonly AuthenticationService _authService;
        private readonly S3User _testUser;

        public AuthenticationRequestParsingTests(ITestOutputHelper output)
        {
            _output = output;
            _loggerMock = new Mock<ILogger<AuthenticationService>>();

            _testUser = new S3User
            {
                AccessKeyId = "TESTKEY",
                SecretAccessKey = "testsecret",
                Name = "testuser",
                BucketPermissions = new List<BucketPermission>
                {
                    new BucketPermission
                    {
                        BucketName = "*",
                        Permissions = new List<string> { "*" }
                    }
                }
            };

            var settings = new AuthenticationSettings
            {
                Enabled = true,
                Users = new List<S3User> { _testUser }
            };

            _authService = new AuthenticationService(_loggerMock.Object, Options.Create(settings));
        }

        [Fact]
        public async Task TestActualRequestParsing_CompareWithExpected()
        {
            _output.WriteLine("=== DEBUG REQUEST PARSING ===");

            // Create a mock HTTP request that simulates what the integration test sends
            var context = new DefaultHttpContext();
            var testData = "Hello, world!"u8.ToArray();
            
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            // Set up the request exactly like the integration test
            context.Request.Method = "PUT";
            context.Request.Path = "/test-bucket/test-object.txt";
            context.Request.Headers["Host"] = "localhost";
            context.Request.Headers["x-amz-date"] = amzDate;

            // Calculate what the signature SHOULD be
            var expectedSig = await CalculateExpectedSignature(testData, dateTime);
            _output.WriteLine($"Expected signature: {expectedSig}");

            // Create authorization header with the expected signature
            context.Request.Headers["Authorization"] = 
                $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
                $"SignedHeaders=host;x-amz-date, Signature={expectedSig}";

            // Set up request body
            context.Request.Body = new MemoryStream(testData);
            context.Request.ContentLength = testData.Length;

            _output.WriteLine($"Request body length: {testData.Length}");
            _output.WriteLine($"Request body hash: {GetHash(testData)}");

            // Now test authentication - this should succeed if parsing is correct
            var (isValid, user, error) = await _authService.ValidateRequestAsync(
                context.Request, "test-bucket", "test-object.txt", "PUT");

            _output.WriteLine($"Validation result: Valid={isValid}, User={user?.Name}, Error={error}");

            // Check if the request body was consumed during validation
            _output.WriteLine($"Request body position after validation: {context.Request.Body.Position}");
            _output.WriteLine($"Request body length after validation: {context.Request.Body.Length}");

            // Try to read the body again to see if it's been consumed
            context.Request.Body.Position = 0;
            var bodyAfter = new byte[testData.Length];
            var bytesRead = await context.Request.Body.ReadAsync(bodyAfter, 0, testData.Length);
            _output.WriteLine($"Bytes read after validation: {bytesRead}");
            _output.WriteLine($"Body content after validation hash: {GetHash(bodyAfter.AsSpan(0, bytesRead).ToArray())}");

            // The issue is likely here - if authentication failed, it's because the body was consumed
            if (!isValid && error == "Invalid signature")
            {
                _output.WriteLine("LIKELY ISSUE: Request body was consumed during signature validation!");
                _output.WriteLine("This means the payload hash calculated during validation doesn't match the original.");
            }
        }

        [Fact]
        public async Task TestStreamingRequestParsing_ShouldNotBufferBody()
        {
            _output.WriteLine("=== DEBUG STREAMING REQUEST PARSING ===");

            var context = new DefaultHttpContext();
            var testData = "Hello, streaming world!"u8.ToArray();
            
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            // Set up streaming request
            context.Request.Method = "PUT";
            context.Request.Path = "/test-bucket/test-object.txt";
            context.Request.Headers["Host"] = "localhost";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
            context.Request.Headers["x-amz-decoded-content-length"] = testData.Length.ToString();
            context.Request.Headers["content-md5"] = Convert.ToBase64String(MD5.HashData(testData));

            // Calculate streaming signature
            var streamingSig = await CalculateStreamingSignature(testData, dateTime);
            _output.WriteLine($"Expected streaming signature: {streamingSig}");

            context.Request.Headers["Authorization"] = 
                $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
                $"SignedHeaders=content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, " +
                $"Signature={streamingSig}";

            // Set up request body with chunked content
            var chunkContent = CreateSimpleChunkedContent(testData);
            context.Request.Body = new MemoryStream(chunkContent);
            context.Request.ContentLength = chunkContent.Length;

            _output.WriteLine($"Original body position: {context.Request.Body.Position}");

            // Test authentication - streaming should NOT buffer the body
            var (isValid, user, error) = await _authService.ValidateRequestAsync(
                context.Request, "test-bucket", "test-object.txt", "PUT");

            _output.WriteLine($"Streaming validation result: Valid={isValid}, User={user?.Name}, Error={error}");
            _output.WriteLine($"Body position after streaming validation: {context.Request.Body.Position}");

            // For streaming, the body should not be consumed during signature validation
            if (!isValid && error == "Invalid signature")
            {
                _output.WriteLine("STREAMING ISSUE: Body position changed, indicating buffering occurred when it shouldn't!");
            }
        }

        private async Task<string> CalculateExpectedSignature(byte[] testData, DateTime dateTime)
        {
            var sigRequest = new SignatureV4Request
            {
                Method = "PUT",
                CanonicalUri = "/test-bucket/test-object.txt",
                CanonicalQueryString = "",
                Headers = new Dictionary<string, string>
                {
                    ["host"] = "localhost",
                    ["x-amz-date"] = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'")
                },
                Payload = testData,
                Region = "us-east-1",
                Service = "s3",
                RequestDateTime = dateTime,
                AccessKeyId = "TESTKEY",
                SignedHeaders = "host;x-amz-date"
            };

            return await _authService.CalculateSignatureV4(sigRequest, _testUser.SecretAccessKey);
        }

        private async Task<string> CalculateStreamingSignature(byte[] testData, DateTime dateTime)
        {
            var streamingSigRequest = new SignatureV4Request
            {
                Method = "PUT",
                CanonicalUri = "/test-bucket/test-object.txt",
                CanonicalQueryString = "",
                Headers = new Dictionary<string, string>
                {
                    ["host"] = "localhost",
                    ["x-amz-date"] = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'"),
                    ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                    ["x-amz-decoded-content-length"] = testData.Length.ToString(),
                    ["content-md5"] = Convert.ToBase64String(MD5.HashData(testData))
                },
                Payload = testData, // Should be ignored for streaming
                Region = "us-east-1",
                Service = "s3",
                RequestDateTime = dateTime,
                AccessKeyId = "TESTKEY",
                SignedHeaders = "content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length"
            };

            return await _authService.CalculateSignatureV4(streamingSigRequest, _testUser.SecretAccessKey);
        }

        private byte[] CreateSimpleChunkedContent(byte[] data)
        {
            // Simple chunked format for testing - not full AWS format
            var result = new List<byte>();
            result.AddRange(data);
            return result.ToArray();
        }

        private string GetHash(byte[] data)
        {
            using var sha256 = SHA256.Create();
            var hash = sha256.ComputeHash(data);
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }
    }
}