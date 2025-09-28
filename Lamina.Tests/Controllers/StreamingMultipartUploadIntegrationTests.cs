using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Serialization;
using Lamina.Core.Models;
using Lamina.WebApi;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Lamina.Tests.Controllers;

public class StreamingMultipartUploadIntegrationTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly WebApplicationFactory<Program> _factory;
    private readonly HttpClient _client;

    public StreamingMultipartUploadIntegrationTests(WebApplicationFactory<Program> factory)
    {
        _factory = factory.WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Test");
            builder.ConfigureAppConfiguration((context, config) =>
            {
                // Use the same approach as StreamingAuthenticationIntegrationTests - load test settings first
                config.Sources.Clear();
                var testProjectPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..");
                var testSettingsPath = Path.Combine(testProjectPath, "Lamina.Tests", "appsettings.Test.json");
                config.AddJsonFile(testSettingsPath, optional: false, reloadOnChange: false);

                // Then overlay authentication settings
                var authConfig = new Dictionary<string, string?>
                {
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
    public async Task MultipartUpload_WithStreamingAuthentication_ValidSignature_Succeeds()
    {
        // Arrange
        var bucketName = $"test-bucket-{Guid.NewGuid():N}";
        await CreateBucketAsync(bucketName);

        var key = "test-multipart.bin";

        // Step 1: Initiate multipart upload
        var uploadId = await InitiateMultipartUploadAsync(bucketName, key);

        // Step 2: Upload parts with streaming authentication
        var part1Data = "Part 1 data with streaming auth"u8.ToArray();
        var part2Data = "Part 2 data with streaming auth"u8.ToArray();

        var part1ETag = await UploadPartWithStreamingAuth(bucketName, key, uploadId, 1, part1Data);
        var part2ETag = await UploadPartWithStreamingAuth(bucketName, key, uploadId, 2, part2Data);

        // Step 3: Complete multipart upload
        var completeResponse = await CompleteMultipartUploadAsync(bucketName, key, uploadId,
            new[] { (1, part1ETag), (2, part2ETag) });

        // Assert
        Assert.NotNull(completeResponse);
        Assert.Equal(bucketName, completeResponse.Bucket);
        Assert.Equal(key, completeResponse.Key);
        Assert.NotNull(completeResponse.ETag);

        // Verify the object exists and has the correct content
        var getRequest = await CreateAuthenticatedGetRequest(bucketName, key);
        var getResponse = await _client.SendAsync(getRequest);
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);

        var retrievedData = await getResponse.Content.ReadAsByteArrayAsync();
        var expectedData = part1Data.Concat(part2Data).ToArray();
        Assert.Equal(expectedData, retrievedData);
    }

    [Fact]
    public async Task MultipartUpload_WithStreamingAuthentication_InvalidChunkSignature_ReturnsForbidden()
    {
        // Arrange
        var bucketName = $"test-bucket-{Guid.NewGuid():N}";
        await CreateBucketAsync(bucketName);

        var key = "test-multipart-invalid.bin";

        // Step 1: Initiate multipart upload
        var uploadId = await InitiateMultipartUploadAsync(bucketName, key);

        // Step 2: Try to upload part with invalid chunk signature
        var partData = "Part data with invalid signature"u8.ToArray();

        var response = await UploadPartWithInvalidStreamingAuth(bucketName, key, uploadId, 1, partData);

        // Assert
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);

        // Clean up
        await AbortMultipartUploadAsync(bucketName, key, uploadId);
    }

    [Fact]
    public async Task MultipartUpload_MixedAuthentication_BothPartsSucceed()
    {
        // Arrange
        var bucketName = $"test-bucket-{Guid.NewGuid():N}";
        await CreateBucketAsync(bucketName);

        var key = "test-multipart-mixed.bin";

        // Step 1: Initiate multipart upload
        var uploadId = await InitiateMultipartUploadAsync(bucketName, key);

        // Step 2: Upload one part with streaming, one without
        var part1Data = "Part 1 with streaming"u8.ToArray();
        var part2Data = "Part 2 without streaming"u8.ToArray();

        var part1ETag = await UploadPartWithStreamingAuth(bucketName, key, uploadId, 1, part1Data);
        var part2ETag = await UploadPartWithRegularAuth(bucketName, key, uploadId, 2, part2Data);

        // Step 3: Complete multipart upload
        var completeResponse = await CompleteMultipartUploadAsync(bucketName, key, uploadId,
            new[] { (1, part1ETag), (2, part2ETag) });

        // Assert
        Assert.NotNull(completeResponse);

        // Verify the complete object
        var getRequest = await CreateAuthenticatedGetRequest(bucketName, key);
        var getResponse = await _client.SendAsync(getRequest);
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);

        var retrievedData = await getResponse.Content.ReadAsByteArrayAsync();
        var expectedData = part1Data.Concat(part2Data).ToArray();
        Assert.Equal(expectedData, retrievedData);
    }

    [Fact]
    public async Task MultipartUpload_AwsCliFormat_WithUploadsEquals_Succeeds()
    {
        // Arrange
        var bucketName = $"test-bucket-{Guid.NewGuid():N}";
        var key = "test-aws-cli-format.bin";

        await CreateBucketAsync(bucketName);

        // Act - Initiate multipart upload using AWS CLI format (?uploads=)
        var uploadId = await InitiateMultipartUploadAsync(bucketName, key);

        var testData = "Test data for AWS CLI format"u8.ToArray();
        var etag = await UploadPartWithStreamingAuth(bucketName, key, uploadId, 1, testData);

        // Complete multipart upload
        await CompleteMultipartUploadAsync(bucketName, key, uploadId, new[]
        {
            (1, etag)
        });

        // Verify the final object
        var getRequest = await CreateAuthenticatedGetRequest(bucketName, key);
        var response = await _client.SendAsync(getRequest);
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsByteArrayAsync();
        Assert.Equal(testData, content);
    }

    private async Task CreateBucketAsync(string bucketName)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
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

        var response = await _client.SendAsync(request);
        response.EnsureSuccessStatusCode();
    }

    private async Task<string> InitiateMultipartUploadAsync(string bucketName, string key)
    {
        // Use AWS CLI compatible format: ?uploads= (with equals sign)
        var request = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/{key}?uploads=");
        var dateTime = DateTime.UtcNow;
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

        request.Headers.Add("Host", "localhost");
        request.Headers.Add("x-amz-date", amzDate);

        var signature = await CalculateSignature(
            "POST",
            $"/{bucketName}/{key}",
            "uploads=",  // Query string WITH equals sign (AWS CLI compatible)
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

        var response = await _client.SendAsync(request);
        response.EnsureSuccessStatusCode();

        var responseContent = await response.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var reader = new StringReader(responseContent);
        var result = (InitiateMultipartUploadResult)serializer.Deserialize(reader)!;

        return result.UploadId;
    }


    private async Task<string> UploadPartWithStreamingAuth(string bucketName, string key, string uploadId, int partNumber, byte[] data)
    {
        var dateTime = DateTime.UtcNow;
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
        var contentMd5 = Convert.ToBase64String(MD5.HashData(data));

        // Create the request with streaming headers
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/{key}?partNumber={partNumber}&uploadId={uploadId}");
        request.Headers.Add("Host", "localhost");
        request.Headers.Add("x-amz-date", amzDate);
        request.Headers.Add("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
        request.Headers.Add("x-amz-decoded-content-length", data.Length.ToString());

        // Calculate the seed signature for streaming
        var signature = await CalculateStreamingSignature(
            "PUT",
            $"/{bucketName}/{key}",
            $"partNumber={partNumber}&uploadId={uploadId}",
            new Dictionary<string, string>
            {
                ["content-md5"] = contentMd5,
                ["host"] = "localhost",
                ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                ["x-amz-date"] = amzDate,
                ["x-amz-decoded-content-length"] = data.Length.ToString()
            },
            "content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length",
            Array.Empty<byte>(),
            dateTime,
            "TESTKEY",
            "testsecret");

        request.Headers.TryAddWithoutValidation("Authorization",
            $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
            $"SignedHeaders=content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, " +
            $"Signature={signature}");

        // Create chunked content
        var chunkedContent = CreateAwsChunkedContent(data, dateTime, signature, "testsecret");
        request.Content = new ByteArrayContent(chunkedContent);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        request.Content.Headers.ContentMD5 = MD5.HashData(data);

        // Send the request
        var response = await _client.SendAsync(request);
        response.EnsureSuccessStatusCode();

        // Extract ETag from response
        return response.Headers.GetValues("ETag").First().Trim('"');
    }

    private async Task<HttpResponseMessage> UploadPartWithInvalidStreamingAuth(string bucketName, string key, string uploadId, int partNumber, byte[] data)
    {
        var dateTime = DateTime.UtcNow;
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
        var contentMd5 = Convert.ToBase64String(MD5.HashData(data));

        // Create the request with streaming headers
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/{key}?partNumber={partNumber}&uploadId={uploadId}");
        request.Headers.Add("Host", "localhost");
        request.Headers.Add("x-amz-date", amzDate);
        request.Headers.Add("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
        request.Headers.Add("x-amz-decoded-content-length", data.Length.ToString());

        // Calculate the seed signature for streaming
        var signature = await CalculateStreamingSignature(
            "PUT",
            $"/{bucketName}/{key}",
            $"partNumber={partNumber}&uploadId={uploadId}",
            new Dictionary<string, string>
            {
                ["content-md5"] = contentMd5,
                ["host"] = "localhost",
                ["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                ["x-amz-date"] = amzDate,
                ["x-amz-decoded-content-length"] = data.Length.ToString()
            },
            "content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length",
            Array.Empty<byte>(),
            dateTime,
            "TESTKEY",
            "testsecret");

        request.Headers.TryAddWithoutValidation("Authorization",
            $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
            $"SignedHeaders=content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, " +
            $"Signature={signature}");

        // Create chunked content with INVALID signatures
        var chunkedContent = CreateAwsChunkedContentWithInvalidSignature(data);
        request.Content = new ByteArrayContent(chunkedContent);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        request.Content.Headers.ContentMD5 = MD5.HashData(data);

        // Send the request
        return await _client.SendAsync(request);
    }

    private async Task<string> UploadPartWithRegularAuth(string bucketName, string key, string uploadId, int partNumber, byte[] data)
    {
        var dateTime = DateTime.UtcNow;
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/{key}?partNumber={partNumber}&uploadId={uploadId}");
        request.Headers.Add("Host", "localhost");
        request.Headers.Add("x-amz-date", amzDate);

        // Add content SHA256 header for proper signature validation
        var payloadHash = GetHash(data);
        request.Headers.Add("x-amz-content-sha256", payloadHash);

        request.Content = new ByteArrayContent(data);

        var signature = await CalculateSignature(
            "PUT",
            $"/{bucketName}/{key}",
            $"partNumber={partNumber}&uploadId={uploadId}",
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

        var response = await _client.SendAsync(request);
        response.EnsureSuccessStatusCode();

        return response.Headers.GetValues("ETag").First().Trim('"');
    }

    private async Task<CompleteMultipartUploadResult> CompleteMultipartUploadAsync(string bucketName, string key, string uploadId, (int partNumber, string etag)[] parts)
    {
        var dateTime = DateTime.UtcNow;
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

        // Create complete multipart upload request body
        // S3 expects CompleteMultipartUpload XML with Part elements
        var requestBody = $@"<?xml version=""1.0"" encoding=""UTF-8""?>
<CompleteMultipartUpload>
{string.Join("\n", parts.Select(p => $@"    <Part>
        <PartNumber>{p.partNumber}</PartNumber>
        <ETag>{p.etag}</ETag>
    </Part>"))}
</CompleteMultipartUpload>";

        var requestBytes = Encoding.UTF8.GetBytes(requestBody);

        var request = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/{key}?uploadId={uploadId}");
        request.Headers.Add("Host", "localhost");
        request.Headers.Add("x-amz-date", amzDate);

        // Add content SHA256 header for proper signature validation
        var payloadHash = GetHash(requestBytes);
        request.Headers.Add("x-amz-content-sha256", payloadHash);

        request.Content = new ByteArrayContent(requestBytes);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/xml");

        var signature = await CalculateSignature(
            "POST",
            $"/{bucketName}/{key}",
            $"uploadId={uploadId}",
            new Dictionary<string, string>
            {
                ["host"] = "localhost",
                ["x-amz-date"] = amzDate,
                ["x-amz-content-sha256"] = payloadHash
            },
            "host;x-amz-date;x-amz-content-sha256",
            requestBytes,
            dateTime,
            "TESTKEY",
            "testsecret");

        request.Headers.TryAddWithoutValidation("Authorization",
            $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, " +
            $"SignedHeaders=host;x-amz-date;x-amz-content-sha256, Signature={signature}");

        var response = await _client.SendAsync(request);
        response.EnsureSuccessStatusCode();

        var responseContent = await response.Content.ReadAsStringAsync();
        var responseSerializer = new XmlSerializer(typeof(CompleteMultipartUploadResult));
        using var reader = new StringReader(responseContent);
        return (CompleteMultipartUploadResult)responseSerializer.Deserialize(reader)!;
    }

    private async Task AbortMultipartUploadAsync(string bucketName, string key, string uploadId)
    {
        var dateTime = DateTime.UtcNow;
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

        var request = new HttpRequestMessage(HttpMethod.Delete, $"/{bucketName}/{key}?uploadId={uploadId}");
        request.Headers.Add("Host", "localhost");
        request.Headers.Add("x-amz-date", amzDate);

        var signature = await CalculateSignature(
            "DELETE",
            $"/{bucketName}/{key}",
            $"uploadId={uploadId}",
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

        await _client.SendAsync(request);
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

    private byte[] CreateAwsChunkedContent(byte[] data, DateTime dateTime, string seedSignature, string secretKey)
    {
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

        var canonicalHeaders = string.Join("\n", headers.OrderBy(h => h.Key).Select(h => $"{h.Key}:{h.Value}"));
        var payloadHash = GetHash(payload);

        var canonicalRequest = $"{method}\n{uri}\n{queryString}\n{canonicalHeaders}\n\n{signedHeaders}\n{payloadHash}";
        var canonicalRequestHash = GetHash(Encoding.UTF8.GetBytes(canonicalRequest));

        var credentialScope = $"{dateStamp}/us-east-1/s3/aws4_request";
        var stringToSign = $"AWS4-HMAC-SHA256\n{amzDate}\n{credentialScope}\n{canonicalRequestHash}";

        var signingKey = GetSigningKey(secretKey, dateStamp, "us-east-1", "s3");
        return Task.FromResult(GetHmacSha256Hex(signingKey, stringToSign));
    }

    private Task<string> CalculateStreamingSignature(string method, string uri, string queryString,
        Dictionary<string, string> headers, string signedHeaders, byte[] payload,
        DateTime dateTime, string accessKey, string secretKey)
    {
        var dateStamp = dateTime.ToString("yyyyMMdd");
        var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

        var canonicalHeaders = string.Join("\n", headers.OrderBy(h => h.Key).Select(h => $"{h.Key}:{h.Value}"));

        var canonicalRequest = $"{method}\n{uri}\n{queryString}\n{canonicalHeaders}\n\n{signedHeaders}\nSTREAMING-AWS4-HMAC-SHA256-PAYLOAD";
        var canonicalRequestHash = GetHash(Encoding.UTF8.GetBytes(canonicalRequest));

        var credentialScope = $"{dateStamp}/us-east-1/s3/aws4_request";
        var stringToSign = $"AWS4-HMAC-SHA256\n{amzDate}\n{credentialScope}\n{canonicalRequestHash}";

        var signingKey = GetSigningKey(secretKey, dateStamp, "us-east-1", "s3");
        return Task.FromResult(GetHmacSha256Hex(signingKey, stringToSign));
    }

    private byte[] GetSigningKey(string secretKey, string dateStamp, string region, string service)
    {
        var kSecret = Encoding.UTF8.GetBytes("AWS4" + secretKey);
        var kDate = HmacSha256(kSecret, dateStamp);
        var kRegion = HmacSha256(kDate, region);
        var kService = HmacSha256(kRegion, service);
        return HmacSha256(kService, "aws4_request");
    }

    private byte[] HmacSha256(byte[] key, string data)
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
}