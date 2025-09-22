using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;
using Lamina.Models;
using Lamina.Services;

namespace Lamina.Tests.Controllers
{
    public class IntegrationAuthDebugTest : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly ITestOutputHelper _output;
        private readonly WebApplicationFactory<Program> _factory;
        private readonly HttpClient _client;

        public IntegrationAuthDebugTest(ITestOutputHelper output, WebApplicationFactory<Program> factory)
        {
            _output = output;
            _factory = factory.WithWebHostBuilder(builder =>
            {
                builder.UseEnvironment("Test");
                builder.ConfigureAppConfiguration((context, config) =>
                {
                    // Use the same approach as IntegrationTestBase - load test settings first
                    config.Sources.Clear();
                    var testProjectPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..");
                    var testSettingsPath = Path.Combine(testProjectPath, "Lamina.Tests", "appsettings.Test.json");
                    config.AddJsonFile(testSettingsPath, optional: false, reloadOnChange: false);
                    
                    // Then overlay authentication settings
                    var authConfig = new Dictionary<string, string>
                    {
                        ["Logging:LogLevel:Default"] = "Debug",
                        ["Logging:LogLevel:Microsoft.AspNetCore"] = "Information", 
                        ["Logging:LogLevel:Lamina.Services.AuthenticationService"] = "Debug",
                        ["Logging:LogLevel:Lamina.Middleware.S3AuthenticationMiddleware"] = "Debug",
                        ["Authentication:Enabled"] = "true",
                        ["Authentication:Users:0:AccessKeyId"] = "TESTKEY",
                        ["Authentication:Users:0:SecretAccessKey"] = "testsecret",
                        ["Authentication:Users:0:Name"] = "testuser",
                        ["Authentication:Users:0:BucketPermissions:0:BucketName"] = "*",
                        ["Authentication:Users:0:BucketPermissions:0:Permissions:0"] = "*"
                    };
                    
                    config.AddInMemoryCollection(authConfig);
                });
            });
            _client = _factory.CreateClient();
        }

        [Fact]
        public async Task DebugIntegrationTestAuthentication()
        {
            _output.WriteLine("=== DEBUG INTEGRATION TEST AUTHENTICATION ===");

            // Step 1: Test if we can get the authentication service directly
            using var scope = _factory.Services.CreateScope();
            var authService = scope.ServiceProvider.GetRequiredService<IAuthenticationService>();
            
            _output.WriteLine($"Authentication service enabled: {authService.IsAuthenticationEnabled()}");
            
            var testUser = authService.GetUserByAccessKey("TESTKEY");
            _output.WriteLine($"Test user found: {testUser?.Name}");

            // Step 2: Test bucket creation first (simpler case)
            _output.WriteLine("\n--- Testing Bucket Creation ---");
            var bucketName = "debug-test-bucket";
            var bucketResponse = await CreateBucketWithDetailedLogging(bucketName);
            _output.WriteLine($"Bucket creation response: {bucketResponse.StatusCode}");
            
            if (bucketResponse.StatusCode != HttpStatusCode.OK && bucketResponse.StatusCode != HttpStatusCode.Created)
            {
                var bucketContent = await bucketResponse.Content.ReadAsStringAsync();
                _output.WriteLine($"Bucket creation failed: {bucketContent}");
                
                // If bucket creation fails, the issue is fundamental
                _output.WriteLine("CRITICAL: Bucket creation failed - authentication is broken at basic level");
                return;
            }

            // Step 3: Test object upload
            _output.WriteLine("\n--- Testing Object Upload ---");
            var objectKey = "test-object.txt";
            var testData = "Hello, debug world!"u8.ToArray();
            
            var objectResponse = await UploadObjectWithDetailedLogging(bucketName, objectKey, testData);
            _output.WriteLine($"Object upload response: {objectResponse.StatusCode}");
            
            if (objectResponse.StatusCode != HttpStatusCode.OK)
            {
                var objectContent = await objectResponse.Content.ReadAsStringAsync();
                _output.WriteLine($"Object upload failed: {objectContent}");
            }

            // Step 4: Compare with direct service call
            _output.WriteLine("\n--- Testing Direct Service Call ---");
            await TestDirectServiceCall(testData);
        }

        private async Task<HttpResponseMessage> CreateBucketWithDetailedLogging(string bucketName)
        {
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
            
            _output.WriteLine($"Bucket creation time: {dateTime:yyyy-MM-dd HH:mm:ss} UTC");
            _output.WriteLine($"AMZ date: {amzDate}");

            var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
            request.Headers.Add("Host", "localhost");
            request.Headers.Add("x-amz-date", amzDate);
            
            // Calculate signature manually
            var signature = await CalculateSignature(
                "PUT", 
                $"/{bucketName}", 
                "", 
                new Dictionary<string, string>
                {
                    ["host"] = "localhost",
                    ["x-amz-date"] = amzDate
                },
                "host;x-amz-date",
                Array.Empty<byte>(),
                dateTime,
                "TESTKEY",
                "testsecret");

            _output.WriteLine($"Calculated bucket signature: {signature}");

            var authHeader = $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature={signature}";
            request.Headers.TryAddWithoutValidation("Authorization", authHeader);

            _output.WriteLine($"Authorization header: {authHeader}");

            return await _client.SendAsync(request);
        }

        private async Task<HttpResponseMessage> UploadObjectWithDetailedLogging(string bucketName, string key, byte[] data)
        {
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
            
            _output.WriteLine($"Object upload time: {dateTime:yyyy-MM-dd HH:mm:ss} UTC");
            _output.WriteLine($"AMZ date: {amzDate}");
            _output.WriteLine($"Data length: {data.Length}");
            _output.WriteLine($"Data hash: {GetHash(data)}");

            var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/{key}");
            request.Headers.Add("Host", "localhost");
            request.Headers.Add("x-amz-date", amzDate);
            
            // Calculate signature manually
            var signature = await CalculateSignature(
                "PUT", 
                $"/{bucketName}/{key}", 
                "", 
                new Dictionary<string, string>
                {
                    ["host"] = "localhost",
                    ["x-amz-date"] = amzDate
                },
                "host;x-amz-date",
                data,
                dateTime,
                "TESTKEY",
                "testsecret");

            _output.WriteLine($"Calculated object signature: {signature}");

            var authHeader = $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature={signature}";
            request.Headers.TryAddWithoutValidation("Authorization", authHeader);
            request.Content = new ByteArrayContent(data);

            _output.WriteLine($"Authorization header: {authHeader}");

            return await _client.SendAsync(request);
        }

        private async Task TestDirectServiceCall(byte[] testData)
        {
            using var scope = _factory.Services.CreateScope();
            var authService = scope.ServiceProvider.GetRequiredService<IAuthenticationService>();
            
            var dateTime = DateTime.UtcNow;
            
            // We can't call CalculateSignatureV4 directly as it's not public
            // Instead, let's test if we can get the user and check authentication status
            var testUser = authService.GetUserByAccessKey("TESTKEY");
            _output.WriteLine("Direct service call results:");
            _output.WriteLine("  User found: " + (testUser?.Name ?? "null"));
            _output.WriteLine("  Authentication enabled: " + authService.IsAuthenticationEnabled());
            
            if (testUser != null)
            {
                _output.WriteLine("  User has bucket permissions: " + authService.UserHasAccessToBucket(testUser, "debug-test-bucket", "PUT"));
            }
        }

        // Helper methods for signature calculation
        private async Task<string> CalculateSignature(string method, string uri, string queryString, 
            Dictionary<string, string> headers, string signedHeaders, byte[] payload, 
            DateTime dateTime, string accessKey, string secretKey)
        {
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            var canonicalHeaders = GetCanonicalHeaders(headers, signedHeaders);
            var payloadHash = GetHash(payload);
            var encodedUri = EncodeUri(uri);

            var canonicalRequest = $"{method}\n{encodedUri}\n{queryString}\n{canonicalHeaders}\n{signedHeaders}\n{payloadHash}";
            
            _output.WriteLine($"Canonical request:\n{canonicalRequest}");

            var algorithm = "AWS4-HMAC-SHA256";
            var credentialScope = $"{dateStamp}/us-east-1/s3/aws4_request";
            var stringToSign = $"{algorithm}\n{amzDate}\n{credentialScope}\n{GetHash(canonicalRequest)}";
            
            _output.WriteLine($"String to sign:\n{stringToSign}");

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