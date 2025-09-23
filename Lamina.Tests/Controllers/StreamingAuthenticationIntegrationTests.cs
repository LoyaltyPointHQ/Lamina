using System.Net;
using System.Text;
using System.Globalization;
using System.Security.Cryptography;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Lamina.Models;
using Lamina.Services;

namespace Lamina.Tests.Controllers;

public class StreamingAuthenticationIntegrationTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly WebApplicationFactory<Program> _factory;
    private readonly HttpClient _client;

    public StreamingAuthenticationIntegrationTests(WebApplicationFactory<Program> factory)
    {
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
                var authConfig = new Dictionary<string, string?>
                {
                    ["Logging:LogLevel:Default"] = "Debug",
                    ["Logging:LogLevel:Microsoft.AspNetCore"] = "Information",
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
    public async Task SimpleUpload_ValidRequest_AuthenticatesAndStoresData()
    {
        // Arrange - Create bucket first
        var bucketName = "test-bucket";
        await CreateBucketAsync(bucketName);

        var key = "test-object.txt";
        var testData = "Hello, world!"u8.ToArray();

        // Act - Use the working UploadRegularObject method
        var response = await UploadRegularObject(bucketName, key, testData);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        // Verify the object was actually stored by trying to retrieve it with authenticated GET
        var getRequest = await CreateAuthenticatedGetRequest(bucketName, key);
        var getResponse = await _client.SendAsync(getRequest);
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);

        var retrievedData = await getResponse.Content.ReadAsByteArrayAsync();
        Assert.Equal(testData, retrievedData);
    }

    [Fact]
    public async Task StreamingUpload_ValidRequest_AuthenticatesAndStoresData()
    {
        // Arrange - Create bucket first
        var bucketName = "test-bucket";
        await CreateBucketAsync(bucketName);

        var key = "test-object.txt";
        var testData = "Hello, streaming world!"u8.ToArray();
        var dateTime = DateTime.UtcNow;
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
        
        // Create streaming request with proper AWS4 signature
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/{key}");
        request.Headers.Add("Host", "localhost");
        request.Headers.Add("x-amz-date", amzDate);
        request.Headers.Add("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
        request.Headers.Add("x-amz-decoded-content-length", testData.Length.ToString());
        
        // Calculate proper signature
        var signature = await CalculateStreamingSignature(
            "PUT", 
            $"/{bucketName}/{key}", 
            "", 
            new Dictionary<string, string>
            {
                ["host"] = "localhost",
                ["x-amz-date"] = amzDate,
                ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                ["x-amz-decoded-content-length"] = testData.Length.ToString(),
                ["content-md5"] = Convert.ToBase64String(MD5.HashData(testData))
            },
            "content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length",
            testData,
            dateTime,
            "TESTKEY",
            "testsecret");

        request.Headers.TryAddWithoutValidation("Authorization", 
            $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
            $"SignedHeaders=content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, " +
            $"Signature={signature}");

        // Create streaming content with AWS chunk format
        var chunkContent = CreateAwsChunkedContent(testData, dateTime, signature, "testsecret");
        request.Content = new ByteArrayContent(chunkContent);
        request.Content.Headers.ContentLength = chunkContent.Length;
        request.Content.Headers.Add("Content-MD5", Convert.ToBase64String(MD5.HashData(testData)));

        // Act
        var response = await _client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("ETag"));

        // Verify the object was actually stored by trying to retrieve it with authenticated GET
        var getRequest = await CreateAuthenticatedGetRequest(bucketName, key);
        var getResponse = await _client.SendAsync(getRequest);
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);

        var retrievedData = await getResponse.Content.ReadAsByteArrayAsync();
        Assert.Equal(testData, retrievedData);
    }

    [Fact]
    public async Task StreamingUpload_InvalidChunkSignature_ReturnsUnauthorized()
    {
        // Arrange - Create bucket first
        var bucketName = "test-bucket";
        await CreateBucketAsync(bucketName);

        var key = "test-object.txt";
        var testData = "Hello, streaming world!"u8.ToArray();
        var dateTime = DateTime.UtcNow;
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
        
        // Create streaming request with proper initial signature but invalid chunk signature
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/{key}");
        request.Headers.Add("Host", "localhost");
        request.Headers.Add("x-amz-date", amzDate);
        request.Headers.Add("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
        request.Headers.Add("x-amz-decoded-content-length", testData.Length.ToString());
        
        // Calculate proper initial signature
        var signature = await CalculateStreamingSignature(
            "PUT", 
            $"/{bucketName}/{key}", 
            "", 
            new Dictionary<string, string>
            {
                ["host"] = "localhost",
                ["x-amz-date"] = amzDate,
                ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                ["x-amz-decoded-content-length"] = testData.Length.ToString(),
                ["content-md5"] = Convert.ToBase64String(MD5.HashData(testData))
            },
            "content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length",
            testData,
            dateTime,
            "TESTKEY",
            "testsecret");

        request.Headers.TryAddWithoutValidation("Authorization", 
            $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
            $"SignedHeaders=content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, " +
            $"Signature={signature}");

        // Create streaming content with INVALID chunk signature
        var chunkContent = CreateAwsChunkedContentWithInvalidSignature(testData);
        request.Content = new ByteArrayContent(chunkContent);
        request.Content.Headers.ContentLength = chunkContent.Length;
        request.Content.Headers.Add("Content-MD5", Convert.ToBase64String(MD5.HashData(testData)));

        // Act
        var response = await _client.SendAsync(request);

        // Assert - Should fail due to invalid chunk signature
        // Note: The exact status code might be 403 (Forbidden) or 401 (Unauthorized) 
        // depending on when validation fails
        Assert.True(response.StatusCode == HttpStatusCode.Forbidden || 
                   response.StatusCode == HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task NonStreamingUpload_WithStreamingHeader_ReturnsUnauthorized()
    {
        // Arrange - Create bucket first
        var bucketName = "test-bucket";
        await CreateBucketAsync(bucketName);

        var key = "test-object.txt";
        var testData = "Hello, world!"u8.ToArray();
        var dateTime = DateTime.UtcNow;
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
        
        // Create request with streaming header but regular content (no chunk format)
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/{key}");
        request.Headers.Add("Host", "localhost");
        request.Headers.Add("x-amz-date", amzDate);
        request.Headers.Add("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
        request.Headers.Add("x-amz-decoded-content-length", testData.Length.ToString());
        
        // Use a dummy signature (this should fail)
        request.Headers.TryAddWithoutValidation("Authorization", 
            $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
            $"SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, " +
            $"Signature=dummy");

        // Send regular (non-chunked) content
        request.Content = new ByteArrayContent(testData);

        // Act
        var response = await _client.SendAsync(request);

        // Assert - Should fail due to invalid signature
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    [Fact]
    public async Task MixedRequests_StreamingAndNonStreaming_BothWork()
    {
        // Arrange - Create bucket first
        var bucketName = "test-bucket";
        await CreateBucketAsync(bucketName);

        // First: Non-streaming upload
        var nonStreamingKey = "regular-object.txt";
        var nonStreamingData = "Regular upload"u8.ToArray();
        var nonStreamingResponse = await UploadRegularObject(bucketName, nonStreamingKey, nonStreamingData);
        Assert.Equal(HttpStatusCode.OK, nonStreamingResponse.StatusCode);

        // Second: Streaming upload  
        var streamingKey = "streaming-object.txt";
        var streamingData = "Streaming upload"u8.ToArray();
        var dateTime = DateTime.UtcNow;
        
        var streamingRequest = await CreateStreamingUploadRequest(bucketName, streamingKey, streamingData, dateTime);
        var streamingResponse = await _client.SendAsync(streamingRequest);
        Assert.Equal(HttpStatusCode.OK, streamingResponse.StatusCode);

        // Verify both objects exist with authenticated GETs
        var getRegularRequest = await CreateAuthenticatedGetRequest(bucketName, nonStreamingKey);
        var getRegularResponse = await _client.SendAsync(getRegularRequest);
        Assert.Equal(HttpStatusCode.OK, getRegularResponse.StatusCode);

        var getStreamingRequest = await CreateAuthenticatedGetRequest(bucketName, streamingKey);
        var getStreamingResponse = await _client.SendAsync(getStreamingRequest);
        Assert.Equal(HttpStatusCode.OK, getStreamingResponse.StatusCode);
    }

    private async Task<HttpResponseMessage> CreateBucketAsync(string bucketName)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
        
        // Add authentication headers for bucket creation
        var dateTime = DateTime.UtcNow;
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
        
        request.Headers.Add("Host", "localhost");
        request.Headers.Add("x-amz-date", amzDate);
        
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

        request.Headers.TryAddWithoutValidation("Authorization", 
            $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
            $"SignedHeaders=host;x-amz-date, Signature={signature}");

        return await _client.SendAsync(request);
    }

    private async Task<HttpResponseMessage> UploadRegularObject(string bucketName, string key, byte[] data)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/{key}");
        var dateTime = DateTime.UtcNow;
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
        
        request.Headers.Add("Host", "localhost");
        request.Headers.Add("x-amz-date", amzDate);

        // Add content SHA256 header for proper signature validation
        var payloadHash = GetHash(data);
        request.Headers.Add("x-amz-content-sha256", payloadHash);

        var signature = await CalculateSignature(
            "PUT",
            $"/{bucketName}/{key}",
            "",
            new Dictionary<string, string>
            {
                ["host"] = "localhost",
                ["x-amz-date"] = amzDate,
                ["x-amz-content-sha256"] = payloadHash
            },
            "host;x-amz-date;x-amz-content-sha256",
            data,
            dateTime,
            "TESTKEY",
            "testsecret");

        request.Headers.TryAddWithoutValidation("Authorization",
            $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
            $"SignedHeaders=host;x-amz-date;x-amz-content-sha256, Signature={signature}");

        request.Content = new ByteArrayContent(data);
        return await _client.SendAsync(request);
    }

    private async Task<HttpRequestMessage> CreateStreamingUploadRequest(string bucketName, string key, byte[] data, DateTime dateTime)
    {
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
        
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/{key}");
        request.Headers.Add("Host", "localhost");
        request.Headers.Add("x-amz-date", amzDate);
        request.Headers.Add("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
        request.Headers.Add("x-amz-decoded-content-length", data.Length.ToString());
        
        var signature = await CalculateStreamingSignature(
            "PUT", 
            $"/{bucketName}/{key}", 
            "", 
            new Dictionary<string, string>
            {
                ["host"] = "localhost",
                ["x-amz-date"] = amzDate,
                ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                ["x-amz-decoded-content-length"] = data.Length.ToString(),
                ["content-md5"] = Convert.ToBase64String(MD5.HashData(data))
            },
            "content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length",
            data,
            dateTime,
            "TESTKEY",
            "testsecret");

        request.Headers.TryAddWithoutValidation("Authorization", 
            $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
            $"SignedHeaders=content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, " +
            $"Signature={signature}");

        var chunkContent = CreateAwsChunkedContent(data, dateTime, signature, "testsecret");
        request.Content = new ByteArrayContent(chunkContent);
        request.Content.Headers.ContentLength = chunkContent.Length;
        request.Content.Headers.Add("Content-MD5", Convert.ToBase64String(MD5.HashData(data)));
        
        return request;
    }

    private byte[] CreateAwsChunkedContent(byte[] data, DateTime dateTime, string seedSignature, string secretKey)
    {
        // AWS chunk format: chunk-size;chunk-signature\r\nchunk-data\r\n0;final-chunk-signature\r\n\r\n
        var chunkSize = data.Length.ToString("x");
        var chunkSignature = CalculateChunkSignature(data, dateTime, seedSignature, secretKey, false);
        var finalChunkSignature = CalculateChunkSignature(Array.Empty<byte>(), dateTime, chunkSignature, secretKey, true);
        
        var result = new List<byte>();
        
        // Add chunk header
        result.AddRange(Encoding.ASCII.GetBytes($"{chunkSize};chunk-signature={chunkSignature}\r\n"));
        
        // Add chunk data
        result.AddRange(data);
        result.AddRange(Encoding.ASCII.GetBytes("\r\n"));
        
        // Add final chunk
        result.AddRange(Encoding.ASCII.GetBytes($"0;chunk-signature={finalChunkSignature}\r\n\r\n"));
        
        return result.ToArray();
    }

    private byte[] CreateAwsChunkedContentWithInvalidSignature(byte[] data)
    {
        // Create chunk content with intentionally invalid signature
        var chunkSize = data.Length.ToString("x");
        var invalidSignature = "invalid_signature_12345678901234567890123456789012345678901234567890123456789012";
        var finalInvalidSignature = "invalid_final_signature_1234567890123456789012345678901234567890123456789012";
        
        var result = new List<byte>();
        
        // Add chunk header with invalid signature
        result.AddRange(Encoding.ASCII.GetBytes($"{chunkSize};chunk-signature={invalidSignature}\r\n"));
        
        // Add chunk data
        result.AddRange(data);
        result.AddRange(Encoding.ASCII.GetBytes("\r\n"));
        
        // Add final chunk with invalid signature
        result.AddRange(Encoding.ASCII.GetBytes($"0;chunk-signature={finalInvalidSignature}\r\n\r\n"));
        
        return result.ToArray();
    }

    private string CalculateChunkSignature(byte[] chunkData, DateTime dateTime, string previousSignature, string secretKey, bool isLastChunk)
    {
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
        
        var algorithm = "AWS4-HMAC-SHA256-PAYLOAD";
        var credentialScope = $"{dateStamp}/us-east-1/s3/aws4_request";
        var emptyStringHash = GetHash(Array.Empty<byte>());
        var chunkSize = isLastChunk ? "0" : chunkData.Length.ToString("x");
        var chunkHash = GetHash(chunkData);

        var stringToSign = $"{algorithm}\n{amzDate}\n{credentialScope}\n{previousSignature}\n{emptyStringHash}\n{chunkHash}";

        var signingKey = GetSigningKey(secretKey, dateStamp, "us-east-1", "s3");
        return GetHmacSha256Hex(signingKey, stringToSign);
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

    private Task<string> CalculateStreamingSignature(string method, string uri, string queryString,
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

    private async Task<HttpRequestMessage> CreateAuthenticatedGetRequest(string bucketName, string key)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, $"/{bucketName}/{key}");
        var dateTime = DateTime.UtcNow;
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

        request.Headers.Add("Host", "localhost");
        request.Headers.Add("x-amz-date", amzDate);

        var signature = await CalculateSignature(
            "GET",
            $"/{bucketName}/{key}",
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

        request.Headers.TryAddWithoutValidation("Authorization",
            $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
            $"SignedHeaders=host;x-amz-date, Signature={signature}");

        return request;
    }

    private bool IsUnreservedCharacter(char c)
    {
        return (c >= 'A' && c <= 'Z') ||
               (c >= 'a' && c <= 'z') ||
               (c >= '0' && c <= '9') ||
               c == '-' || c == '.' || c == '_' || c == '~';
    }
}