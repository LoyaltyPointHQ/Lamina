using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System.Net.Http;
using System.Text;
using Xunit;
using Xunit.Abstractions;
using Lamina.Models;
using Lamina.Services;
using System.Security.Cryptography;

namespace Lamina.Tests.Services
{
    public class IntegrationVsUnitTestComparisonTests : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly ITestOutputHelper _output;
        private readonly WebApplicationFactory<Program> _factory;
        private readonly HttpClient _client;
        private readonly Mock<ILogger<AuthenticationService>> _loggerMock;
        private readonly AuthenticationService _authService;
        private readonly S3User _testUser;

        public IntegrationVsUnitTestComparisonTests(WebApplicationFactory<Program> factory, ITestOutputHelper output)
        {
            _output = output;
            _factory = factory.WithWebHostBuilder(builder =>
            {
                builder.UseEnvironment("Test");
                builder.ConfigureAppConfiguration((context, config) =>
                {
                    config.Sources.Clear();
                    
                    var testConfig = new Dictionary<string, string?>
                    {
                        ["Logging:LogLevel:Default"] = "Debug",
                        ["Logging:LogLevel:Microsoft.AspNetCore"] = "Information",
                        ["StorageType"] = "InMemory",
                        ["FilesystemStorage:DataDirectory"] = "/tmp/lamina-test-data",
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
        public async Task CompareIntegrationTestVsUnitTest()
        {
            _output.WriteLine("=== COMPARING INTEGRATION TEST VS UNIT TEST BEHAVIOR ===");

            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
            var testData = "Hello, world!"u8.ToArray();
            var bucketName = "test-bucket";
            var key = "test-object.txt";

            _output.WriteLine($"Using dateTime: {dateTime}");
            _output.WriteLine($"Test data: '{Encoding.UTF8.GetString(testData)}'");
            _output.WriteLine($"Test data length: {testData.Length}");

            // First, create the bucket
            await CreateBucket(bucketName, dateTime);

            // PART 1: Unit test approach (KNOWN TO WORK)
            _output.WriteLine("\n--- PART 1: UNIT TEST APPROACH ---");
            
            var unitTestContext = new DefaultHttpContext();
            unitTestContext.Request.Method = "PUT";
            unitTestContext.Request.Path = $"/{bucketName}/{key}";
            unitTestContext.Request.Headers["x-amz-date"] = amzDate;
            unitTestContext.Request.Headers["Host"] = "localhost";
            unitTestContext.Request.ContentType = "text/plain";
            unitTestContext.Request.ContentLength = testData.Length;
            unitTestContext.Request.Body = new MemoryStream(testData);

            // Add content SHA256 header for proper signature validation
            var payloadHash = GetHash(testData);
            unitTestContext.Request.Headers["x-amz-content-sha256"] = payloadHash;

            // Calculate signature for unit test
            var unitTestHeaders = new Dictionary<string, string>
            {
                {"host", "localhost"},
                {"x-amz-date", amzDate},
                {"x-amz-content-sha256", payloadHash}
            };

            var unitTestSignature = await CalculateSignature("PUT", $"/{bucketName}/{key}", "",
                unitTestHeaders, "host;x-amz-date;x-amz-content-sha256", testData, dateTime, "TESTKEY", "testsecret");

            unitTestContext.Request.Headers["Authorization"] =
                $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date;x-amz-content-sha256, Signature={unitTestSignature}";

            _output.WriteLine($"Unit test signature: {unitTestSignature}");

            // Test unit test validation
            var (unitValid, unitUser, unitError) = await _authService.ValidateRequestAsync(
                unitTestContext.Request, bucketName, key, "PUT");
            _output.WriteLine($"Unit test validation - Valid: {unitValid}, Error: {unitError}");

            // PART 2: Integration test approach (FAILING)
            _output.WriteLine("\n--- PART 2: INTEGRATION TEST APPROACH ---");

            // Calculate signature exactly like the integration test does
            var integrationHeaders = new Dictionary<string, string>
            {
                ["host"] = "localhost",
                ["x-amz-date"] = amzDate,
                ["x-amz-content-sha256"] = payloadHash
            };

            var integrationSignature = await CalculateSignature("PUT", $"/{bucketName}/{key}", "",
                integrationHeaders, "host;x-amz-date;x-amz-content-sha256", testData, dateTime, "TESTKEY", "testsecret");

            _output.WriteLine($"Integration test signature: {integrationSignature}");
            _output.WriteLine($"Signatures match: {unitTestSignature == integrationSignature}");

            // Create HttpRequestMessage like the integration test
            var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/{key}");
            request.Headers.Add("Host", "localhost");
            request.Headers.Add("x-amz-date", amzDate);
            request.Headers.Add("x-amz-content-sha256", payloadHash);
            request.Headers.TryAddWithoutValidation("Authorization",
                $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
                $"SignedHeaders=host;x-amz-date;x-amz-content-sha256, Signature={integrationSignature}");
            request.Content = new ByteArrayContent(testData);

            _output.WriteLine("\nSending integration test request...");
            var response = await _client.SendAsync(request);
            _output.WriteLine($"Integration test response status: {response.StatusCode}");
            
            if (!response.IsSuccessStatusCode)
            {
                var errorContent = await response.Content.ReadAsStringAsync();
                _output.WriteLine($"Error content: {errorContent}");
            }

            // PART 3: Deep dive into request differences
            _output.WriteLine("\n--- PART 3: REQUEST ANALYSIS ---");

            // Let's manually construct what the server should see
            _output.WriteLine("Expected canonical request components:");
            _output.WriteLine($"Method: PUT");
            _output.WriteLine($"URI: {EncodeUri($"/{bucketName}/{key}")}");
            _output.WriteLine($"Query: (empty)");
            _output.WriteLine($"Headers: {GetCanonicalHeaders(integrationHeaders, "host;x-amz-date;x-amz-content-sha256")}");
            _output.WriteLine($"Signed headers: host;x-amz-date;x-amz-content-sha256");
            _output.WriteLine($"Payload hash: {GetHash(testData)}");

            var expectedCanonicalRequest = $"PUT\n{EncodeUri($"/{bucketName}/{key}")}\n\n{GetCanonicalHeaders(integrationHeaders, "host;x-amz-date;x-amz-content-sha256")}\nhost;x-amz-date;x-amz-content-sha256\n{GetHash(testData)}";
            _output.WriteLine($"\nExpected canonical request:\n{expectedCanonicalRequest}");

            var expectedStringToSign = $"AWS4-HMAC-SHA256\n{amzDate}\n{dateStamp}/us-east-1/s3/aws4_request\n{GetHash(expectedCanonicalRequest)}";
            _output.WriteLine($"\nExpected string to sign:\n{expectedStringToSign}");

            // Assertions
            Assert.True(unitValid, "Unit test should validate successfully");
            Assert.Equal(unitTestSignature, integrationSignature);
        }

        private async Task CreateBucket(string bucketName, DateTime dateTime)
        {
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
            
            var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
            request.Headers.Add("Host", "localhost");
            request.Headers.Add("x-amz-date", amzDate);
            
            var signature = await CalculateSignature("PUT", $"/{bucketName}", "", 
                new Dictionary<string, string>
                {
                    ["host"] = "localhost",
                    ["x-amz-date"] = amzDate
                },
                "host;x-amz-date", Array.Empty<byte>(), dateTime, "TESTKEY", "testsecret");

            request.Headers.TryAddWithoutValidation("Authorization", 
                $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
                $"SignedHeaders=host;x-amz-date, Signature={signature}");

            var response = await _client.SendAsync(request);
            _output.WriteLine($"Bucket creation status: {response.StatusCode}");
        }

        private Task<string> CalculateSignature(string method, string uri, string queryString,
            Dictionary<string, string> headers, string signedHeaders, byte[] payload,
            DateTime dateTime, string accessKey, string secretKey)
        {
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            var canonicalHeaders = GetCanonicalHeaders(headers, signedHeaders);
            var payloadHash = GetHash(payload);

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