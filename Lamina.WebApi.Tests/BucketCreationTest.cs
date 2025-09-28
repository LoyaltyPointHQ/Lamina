using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using Lamina.WebApi;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Xunit;
using Xunit.Abstractions;

namespace Lamina.WebApi.Tests.Controllers
{
    public class BucketCreationTest : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly ITestOutputHelper _output;
        private readonly WebApplicationFactory<Program> _factory;
        private readonly HttpClient _client;

        public BucketCreationTest(ITestOutputHelper output, WebApplicationFactory<Program> factory)
        {
            _output = output;
            _factory = factory.WithWebHostBuilder(builder =>
            {
                builder.UseEnvironment("Test");
                builder.ConfigureAppConfiguration((context, config) =>
                {
                    config.Sources.Clear();
                    
                    // Use in-memory configuration with authentication enabled
                    var testConfig = new Dictionary<string, string?>
                    {
                        ["Logging:LogLevel:Default"] = "Debug", // More verbose logging
                        ["Logging:LogLevel:Microsoft.AspNetCore"] = "Information",
                        ["Logging:LogLevel:Lamina.Services.AuthenticationService"] = "Debug",
                        ["Logging:LogLevel:Lamina.Middleware.S3AuthenticationMiddleware"] = "Debug",
                        ["StorageType"] = "InMemory",
                        ["FilesystemStorage:DataDirectory"] = "/tmp/lamina-test",
                        ["FilesystemStorage:MetadataDirectory"] = "/tmp/lamina-test-metadata",
                        ["Authentication:Enabled"] = "true",
                        ["Authentication:Users:0:AccessKeyId"] = "TESTKEY",
                        ["Authentication:Users:0:SecretAccessKey"] = "testsecret",
                        ["Authentication:Users:0:Name"] = "testuser",
                        ["Authentication:Users:0:BucketPermissions:0:BucketName"] = "*",
                        ["Authentication:Users:0:BucketPermissions:0:Permissions:0"] = "*",
                        ["MultipartUploadCleanup:Enabled"] = "false"
                    };
                    
                    config.AddInMemoryCollection(testConfig);
                });
            });
            _client = _factory.CreateClient();
        }

        [Fact]
        public async Task TestBucketCreation_WithDetailedLogging()
        {
            _output.WriteLine("=== DEBUG BUCKET CREATION ===");

            var bucketName = "test-bucket-debug";
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
            
            _output.WriteLine($"Using dateTime: {dateTime}");
            _output.WriteLine($"Formatted AMZ date: {amzDate}");
            _output.WriteLine($"Date stamp: {dateStamp}");

            var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
            
            request.Headers.Add("Host", "localhost");
            request.Headers.Add("x-amz-date", amzDate);
            
            _output.WriteLine($"Request URI: {request.RequestUri}");
            _output.WriteLine($"Request method: {request.Method}");

            // Calculate signature exactly like the integration test does
            var headers = new Dictionary<string, string>
            {
                ["host"] = "localhost",
                ["x-amz-date"] = amzDate
            };

            var signature = await CalculateSignature(
                "PUT", 
                $"/{bucketName}", 
                "", 
                headers,
                "host;x-amz-date",
                Array.Empty<byte>(),
                dateTime,
                "TESTKEY",
                "testsecret");

            _output.WriteLine($"Calculated signature: {signature}");

            var authHeader = $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature={signature}";
            _output.WriteLine($"Authorization header: {authHeader}");

            request.Headers.TryAddWithoutValidation("Authorization", authHeader);

            // List all headers being sent
            _output.WriteLine("\nRequest headers:");
            foreach (var header in request.Headers)
            {
                _output.WriteLine($"  {header.Key}: {string.Join(", ", header.Value)}");
            }

            // Send the request
            var response = await _client.SendAsync(request);
            
            _output.WriteLine($"\nResponse status: {response.StatusCode}");
            var content = await response.Content.ReadAsStringAsync();
            _output.WriteLine($"Response content: {content}");

            // Check if it succeeded - S3 API specification requires 200 OK for bucket creation
            var isSuccess = response.StatusCode == HttpStatusCode.OK;
            _output.WriteLine($"Bucket creation success: {isSuccess}");

            if (!isSuccess)
            {
                _output.WriteLine("BUCKET CREATION FAILED - this explains why all subsequent tests fail!");
            }
        }

        private Task<string> CalculateSignature(string method, string uri, string queryString,
            Dictionary<string, string> headers, string signedHeaders, byte[] payload,
            DateTime dateTime, string accessKey, string secretKey)
        {
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            var canonicalHeaders = GetCanonicalHeaders(headers, signedHeaders);
            var payloadHash = GetHash(payload);
            var encodedUri = EncodeUri(uri);

            var canonicalRequest = $"{method}\n{encodedUri}\n{queryString}\n{canonicalHeaders}\n{signedHeaders}\n{payloadHash}";

            var algorithm = "AWS4-HMAC-SHA256";
            var credentialScope = $"{dateStamp}/us-east-1/s3/aws4_request";
            var stringToSign = $"{algorithm}\n{amzDate}\n{credentialScope}\n{GetHash(canonicalRequest)}";

            _output.WriteLine($"\nCanonical request:\n{canonicalRequest}");
            _output.WriteLine($"\nString to sign:\n{stringToSign}");

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
    }
}