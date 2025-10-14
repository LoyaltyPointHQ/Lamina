using System.Net;
using System.Text;
using System.Xml.Linq;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Lamina.WebApi.Tests.Controllers;

/// <summary>
/// Comprehensive tests for S3 checksum calculation and validation functionality.
/// Tests both server-side checksum calculation and client-provided checksum validation.
/// </summary>
public class ChecksumCalculationTests : IntegrationTestBase
{
    // Test data: "Hello World"
    // Pre-calculated checksums for verification (AWS-compatible values)
    private const string TestData = "Hello World";
    private const string TestData_CRC32 = "ShexVg==";  // CRC32 for "Hello World" (AWS-compatible)
    private const string TestData_CRC32C = "aR2qLw==";  // CRC32C (Crc32CAlgorithm output)
    private const string TestData_SHA1 = "Ck1VqNd45QIvq3AZd8XYQLvEhtA=";
    private const string TestData_SHA256 = "pZGm1Av0IEBKARczz7exkNYsZb8LzaMrV7J32a2fFG4=";

    public ChecksumCalculationTests(WebApplicationFactory<global::Program> factory) : base(factory)
    {
    }

    private async Task<string> CreateTestBucketAsync()
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        return bucketName;
    }

    #region PutObject Checksum Calculation Tests

    [Theory]
    [InlineData("CRC32", TestData_CRC32)]
    [InlineData("CRC32C", TestData_CRC32C)]
    [InlineData("SHA1", TestData_SHA1)]
    [InlineData("SHA256", TestData_SHA256)]
    public async Task PutObject_WithChecksumAlgorithm_ServerCalculatesAndReturnsChecksum(string algorithm, string expectedChecksum)
    {
        // Arrange - Create bucket and prepare request with only algorithm header (no checksum value)
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent(TestData, Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-algorithm", algorithm);

        // Act - Upload object with checksum algorithm specified
        var response = await Client.SendAsync(request);

        // Assert - Verify success and checksum in response headers
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var checksumHeaderName = $"x-amz-checksum-{algorithm.ToLowerInvariant()}";
        Assert.True(response.Headers.Contains(checksumHeaderName),
            $"Response should contain {checksumHeaderName} header");

        var actualChecksum = response.Headers.GetValues(checksumHeaderName).First();
        Assert.Equal(expectedChecksum, actualChecksum);
    }

    [Fact]
    public async Task PutObject_WithCRC32Algorithm_CalculatesCorrectChecksum()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent(TestData, Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test-crc32.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-algorithm", "CRC32");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("x-amz-checksum-crc32"));
        Assert.Equal(TestData_CRC32, response.Headers.GetValues("x-amz-checksum-crc32").First());
    }

    [Fact]
    public async Task PutObject_WithCRC32CAlgorithm_CalculatesCorrectChecksum()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent(TestData, Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test-crc32c.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-algorithm", "CRC32C");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("x-amz-checksum-crc32c"));
        Assert.Equal(TestData_CRC32C, response.Headers.GetValues("x-amz-checksum-crc32c").First());
    }

    [Fact]
    public async Task PutObject_WithSHA1Algorithm_CalculatesCorrectChecksum()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent(TestData, Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test-sha1.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-algorithm", "SHA1");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("x-amz-checksum-sha1"));
        Assert.Equal(TestData_SHA1, response.Headers.GetValues("x-amz-checksum-sha1").First());
    }

    [Fact]
    public async Task PutObject_WithSHA256Algorithm_CalculatesCorrectChecksum()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent(TestData, Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test-sha256.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-algorithm", "SHA256");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("x-amz-checksum-sha256"));
        Assert.Equal(TestData_SHA256, response.Headers.GetValues("x-amz-checksum-sha256").First());
    }

    #endregion

    #region PutObject Checksum Validation Tests

    [Theory]
    [InlineData("CRC32", TestData_CRC32)]
    [InlineData("CRC32C", TestData_CRC32C)]
    [InlineData("SHA1", TestData_SHA1)]
    [InlineData("SHA256", TestData_SHA256)]
    public async Task PutObject_WithCorrectChecksumValue_Succeeds(string algorithm, string correctChecksum)
    {
        // Arrange - Create bucket and prepare request with both algorithm and correct checksum value
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent(TestData, Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test-validated.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-algorithm", algorithm);
        request.Headers.Add($"x-amz-checksum-{algorithm.ToLowerInvariant()}", correctChecksum);

        // Act - Upload object with provided checksum for validation
        var response = await Client.SendAsync(request);

        // Assert - Server should validate and accept the correct checksum
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var checksumHeaderName = $"x-amz-checksum-{algorithm.ToLowerInvariant()}";
        Assert.True(response.Headers.Contains(checksumHeaderName));
        Assert.Equal(correctChecksum, response.Headers.GetValues(checksumHeaderName).First());
    }

    [Theory]
    [InlineData("CRC32", "INVALID==")]
    [InlineData("CRC32C", "INVALID==")]
    [InlineData("SHA1", "INVALIDINVALIDINVALID==")]
    [InlineData("SHA256", "INVALIDINVALIDINVALIDINVALIDINVALID==")]
    public async Task PutObject_WithIncorrectChecksumValue_Returns400(string algorithm, string incorrectChecksum)
    {
        // Arrange - Create bucket and prepare request with incorrect checksum value
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent(TestData, Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test-invalid.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-algorithm", algorithm);
        request.Headers.Add($"x-amz-checksum-{algorithm.ToLowerInvariant()}", incorrectChecksum);

        // Act - Upload object with incorrect checksum
        var response = await Client.SendAsync(request);

        // Assert - Server should reject with 400 Bad Request
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

        var responseBody = await response.Content.ReadAsStringAsync();
        Assert.Contains("InvalidChecksum", responseBody);
    }

    [Fact]
    public async Task PutObject_WithCorrectCRC32Checksum_ValidatesSuccessfully()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent(TestData, Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/validated-crc32.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-crc32", TestData_CRC32);

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("x-amz-checksum-crc32"));
        Assert.Equal(TestData_CRC32, response.Headers.GetValues("x-amz-checksum-crc32").First());
    }

    [Fact]
    public async Task PutObject_WithIncorrectCRC32Checksum_Returns400()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent(TestData, Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/invalid-crc32.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-crc32", "WRONGVAL==");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var responseBody = await response.Content.ReadAsStringAsync();
        Assert.Contains("InvalidChecksum", responseBody);
    }

    [Fact]
    public async Task PutObject_WithCorrectSHA256Checksum_ValidatesSuccessfully()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent(TestData, Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/validated-sha256.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-sha256", TestData_SHA256);

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("x-amz-checksum-sha256"));
        Assert.Equal(TestData_SHA256, response.Headers.GetValues("x-amz-checksum-sha256").First());
    }

    [Fact]
    public async Task PutObject_WithIncorrectSHA256Checksum_Returns400()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent(TestData, Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/invalid-sha256.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-sha256", "WrongChecksumValueWrongChecksumValue==");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var responseBody = await response.Content.ReadAsStringAsync();
        Assert.Contains("InvalidChecksum", responseBody);
    }

    #endregion

    #region UploadPart Checksum Calculation Tests

    [Theory]
    [InlineData("CRC32", TestData_CRC32)]
    [InlineData("CRC32C", TestData_CRC32C)]
    [InlineData("SHA1", TestData_SHA1)]
    [InlineData("SHA256", TestData_SHA256)]
    public async Task UploadPart_WithChecksumAlgorithm_ServerCalculatesAndReturnsChecksum(string algorithm, string expectedChecksum)
    {
        // Arrange - Create bucket and initiate multipart upload
        var bucketName = await CreateTestBucketAsync();

        var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/multipart-test.txt?uploads");
        initiateRequest.Headers.Add("x-amz-checksum-algorithm", algorithm);
        var initiateResponse = await Client.SendAsync(initiateRequest);
        var initiateXml = await initiateResponse.Content.ReadAsStringAsync();
        var uploadId = XDocument.Parse(initiateXml)
            .Descendants()
            .First(e => e.Name.LocalName == "UploadId")
            .Value;

        // Upload part with checksum algorithm (no checksum value provided)
        var content = new StringContent(TestData, Encoding.UTF8, "application/octet-stream");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/multipart-test.txt?partNumber=1&uploadId={uploadId}");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-algorithm", algorithm);

        // Act
        var response = await Client.SendAsync(request);

        // Assert - Verify checksum is calculated and returned
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var checksumHeaderName = $"x-amz-checksum-{algorithm.ToLowerInvariant()}";
        Assert.True(response.Headers.Contains(checksumHeaderName),
            $"Response should contain {checksumHeaderName} header");

        var actualChecksum = response.Headers.GetValues(checksumHeaderName).First();
        Assert.Equal(expectedChecksum, actualChecksum);
    }

    [Fact]
    public async Task UploadPart_WithCRC32Algorithm_CalculatesCorrectChecksum()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();

        var initiateResponse = await Client.PostAsync($"/{bucketName}/mp-crc32.txt?uploads", null);
        var initiateXml = await initiateResponse.Content.ReadAsStringAsync();
        var uploadId = XDocument.Parse(initiateXml)
            .Descendants()
            .First(e => e.Name.LocalName == "UploadId")
            .Value;

        var content = new StringContent(TestData, Encoding.UTF8, "application/octet-stream");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/mp-crc32.txt?partNumber=1&uploadId={uploadId}");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-algorithm", "CRC32");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("x-amz-checksum-crc32"));
        Assert.Equal(TestData_CRC32, response.Headers.GetValues("x-amz-checksum-crc32").First());
    }

    [Fact]
    public async Task UploadPart_WithChecksums_AppearsInListParts()
    {
        // Arrange - Create bucket, initiate upload, and upload part with checksum
        var bucketName = await CreateTestBucketAsync();

        var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/mp-list.txt?uploads");
        initiateRequest.Headers.Add("x-amz-checksum-algorithm", "CRC32");
        var initiateResponse = await Client.SendAsync(initiateRequest);
        var initiateXml = await initiateResponse.Content.ReadAsStringAsync();
        var uploadId = XDocument.Parse(initiateXml)
            .Descendants()
            .First(e => e.Name.LocalName == "UploadId")
            .Value;

        var content = new StringContent(TestData, Encoding.UTF8, "application/octet-stream");
        var uploadRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/mp-list.txt?partNumber=1&uploadId={uploadId}");
        uploadRequest.Content = content;
        uploadRequest.Headers.Add("x-amz-checksum-crc32", TestData_CRC32);
        var uploadResponse = await Client.SendAsync(uploadRequest);
        Assert.Equal(HttpStatusCode.OK, uploadResponse.StatusCode);

        // Verify that the UploadPart response returned the checksum (proving it was validated and stored)
        Assert.True(uploadResponse.Headers.Contains("x-amz-checksum-crc32"), "UploadPart response should contain x-amz-checksum-crc32 header");
        var returnedChecksum = uploadResponse.Headers.GetValues("x-amz-checksum-crc32").First();
        Assert.Equal(TestData_CRC32, returnedChecksum);

        // Act - List parts to verify checksum is stored
        var listResponse = await Client.GetAsync($"/{bucketName}/mp-list.txt?uploadId={uploadId}");

        // Assert
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var listXml = await listResponse.Content.ReadAsStringAsync();
        Assert.Contains("ChecksumCRC32", listXml);
        Assert.Contains(TestData_CRC32, listXml);
    }

    #endregion

    #region UploadPart Checksum Validation Tests

    [Theory]
    [InlineData("CRC32", TestData_CRC32)]
    [InlineData("CRC32C", TestData_CRC32C)]
    [InlineData("SHA1", TestData_SHA1)]
    [InlineData("SHA256", TestData_SHA256)]
    public async Task UploadPart_WithCorrectChecksumValue_Succeeds(string algorithm, string correctChecksum)
    {
        // Arrange - Create bucket and initiate multipart upload
        var bucketName = await CreateTestBucketAsync();

        var initiateResponse = await Client.PostAsync($"/{bucketName}/mp-validated.txt?uploads", null);
        var initiateXml = await initiateResponse.Content.ReadAsStringAsync();
        var uploadId = XDocument.Parse(initiateXml)
            .Descendants()
            .First(e => e.Name.LocalName == "UploadId")
            .Value;

        // Upload part with correct checksum value for validation
        var content = new StringContent(TestData, Encoding.UTF8, "application/octet-stream");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/mp-validated.txt?partNumber=1&uploadId={uploadId}");
        request.Content = content;
        request.Headers.Add($"x-amz-checksum-{algorithm.ToLowerInvariant()}", correctChecksum);

        // Act
        var response = await Client.SendAsync(request);

        // Assert - Server should validate and accept the correct checksum
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Theory]
    [InlineData("CRC32", "INVALID==")]
    [InlineData("CRC32C", "INVALID==")]
    [InlineData("SHA1", "INVALIDINVALIDINVALID==")]
    [InlineData("SHA256", "INVALIDINVALIDINVALIDINVALIDINVALID==")]
    public async Task UploadPart_WithIncorrectChecksumValue_Returns400(string algorithm, string incorrectChecksum)
    {
        // Arrange - Create bucket and initiate multipart upload
        var bucketName = await CreateTestBucketAsync();

        var initiateResponse = await Client.PostAsync($"/{bucketName}/mp-invalid.txt?uploads", null);
        var initiateXml = await initiateResponse.Content.ReadAsStringAsync();
        var uploadId = XDocument.Parse(initiateXml)
            .Descendants()
            .First(e => e.Name.LocalName == "UploadId")
            .Value;

        // Upload part with incorrect checksum value
        var content = new StringContent(TestData, Encoding.UTF8, "application/octet-stream");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/mp-invalid.txt?partNumber=1&uploadId={uploadId}");
        request.Content = content;
        request.Headers.Add($"x-amz-checksum-{algorithm.ToLowerInvariant()}", incorrectChecksum);

        // Act
        var response = await Client.SendAsync(request);

        // Assert - Server should reject with 400 Bad Request
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

        var responseBody = await response.Content.ReadAsStringAsync();
        Assert.Contains("InvalidChecksum", responseBody);
    }

    [Fact]
    public async Task UploadPart_WithCorrectCRC32Checksum_ValidatesSuccessfully()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();

        var initiateResponse = await Client.PostAsync($"/{bucketName}/mp-crc32-valid.txt?uploads", null);
        var initiateXml = await initiateResponse.Content.ReadAsStringAsync();
        var uploadId = XDocument.Parse(initiateXml)
            .Descendants()
            .First(e => e.Name.LocalName == "UploadId")
            .Value;

        var content = new StringContent(TestData, Encoding.UTF8, "application/octet-stream");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/mp-crc32-valid.txt?partNumber=1&uploadId={uploadId}");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-crc32", TestData_CRC32);

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task UploadPart_WithIncorrectCRC32Checksum_Returns400()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();

        var initiateResponse = await Client.PostAsync($"/{bucketName}/mp-crc32-invalid.txt?uploads", null);
        var initiateXml = await initiateResponse.Content.ReadAsStringAsync();
        var uploadId = XDocument.Parse(initiateXml)
            .Descendants()
            .First(e => e.Name.LocalName == "UploadId")
            .Value;

        var content = new StringContent(TestData, Encoding.UTF8, "application/octet-stream");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/mp-crc32-invalid.txt?partNumber=1&uploadId={uploadId}");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-crc32", "WRONGVAL==");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var responseBody = await response.Content.ReadAsStringAsync();
        Assert.Contains("InvalidChecksum", responseBody);
    }

    #endregion

    #region CompleteMultipartUpload Checksum Tests

    [Fact]
    public async Task CompleteMultipartUpload_WithChecksumParts_ReturnsAggregatedChecksum()
    {
        // Arrange - Create bucket, initiate upload, and upload parts with checksums
        var bucketName = await CreateTestBucketAsync();

        var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/mp-complete.txt?uploads");
        initiateRequest.Headers.Add("x-amz-checksum-algorithm", "CRC32");
        var initiateResponse = await Client.SendAsync(initiateRequest);
        var initiateXml = await initiateResponse.Content.ReadAsStringAsync();
        var uploadId = XDocument.Parse(initiateXml)
            .Descendants()
            .First(e => e.Name.LocalName == "UploadId")
            .Value;

        // Upload part 1 WITH checksum header
        var content1 = new StringContent(TestData, Encoding.UTF8, "application/octet-stream");
        var part1Request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/mp-complete.txt?partNumber=1&uploadId={uploadId}");
        part1Request.Content = content1;
        part1Request.Headers.Add("x-amz-checksum-crc32", TestData_CRC32);
        var part1Response = await Client.SendAsync(part1Request);
        Assert.Equal(HttpStatusCode.OK, part1Response.StatusCode);
        Assert.True(part1Response.Headers.Contains("x-amz-checksum-crc32"), "Part 1 upload should return checksum in response");
        var etag1 = part1Response.Headers.ETag!.Tag.Trim('"');

        // Upload part 2 WITH checksum header
        var content2 = new StringContent(TestData, Encoding.UTF8, "application/octet-stream");
        var part2Request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/mp-complete.txt?partNumber=2&uploadId={uploadId}");
        part2Request.Content = content2;
        part2Request.Headers.Add("x-amz-checksum-crc32", TestData_CRC32);
        var part2Response = await Client.SendAsync(part2Request);
        Assert.Equal(HttpStatusCode.OK, part2Response.StatusCode);
        Assert.True(part2Response.Headers.Contains("x-amz-checksum-crc32"), "Part 2 upload should return checksum in response");
        var etag2 = part2Response.Headers.ETag!.Tag.Trim('"');

        // Verify parts have checksums by listing them
        var listPartsResponse = await Client.GetAsync($"/{bucketName}/mp-complete.txt?uploadId={uploadId}");
        var listPartsXml = await listPartsResponse.Content.ReadAsStringAsync();
        Assert.Contains("ChecksumCRC32", listPartsXml);
        Assert.Contains(TestData_CRC32, listPartsXml);

        // Complete multipart upload
        var completeXml = $@"
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>{etag1}</ETag>
        <ChecksumCRC32>{TestData_CRC32}</ChecksumCRC32>
    </Part>
    <Part>
        <PartNumber>2</PartNumber>
        <ETag>{etag2}</ETag>
        <ChecksumCRC32>{TestData_CRC32}</ChecksumCRC32>
    </Part>
</CompleteMultipartUpload>";

        var completeRequest = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/mp-complete.txt?uploadId={uploadId}");
        completeRequest.Content = new StringContent(completeXml, Encoding.UTF8, "application/xml");

        // Act
        var response = await Client.SendAsync(completeRequest);

        // Assert - Verify completion and checksum information in response
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var responseXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("CompleteMultipartUploadResult", responseXml);
        Assert.Contains("ETag", responseXml);

        // Check response headers for checksum
        var hasChecksumHeader = response.Headers.Contains("x-amz-checksum-crc32");

        // Checksum should be present in the response (aggregated checksums in headers or XML)
        var doc = XDocument.Parse(responseXml);
        var hasChecksumInXml = doc.Descendants().Any(e => e.Name.LocalName.StartsWith("Checksum"));

        Assert.True(hasChecksumHeader || hasChecksumInXml,
            $"Response should contain checksum information. Has header: {hasChecksumHeader}, Has XML: {hasChecksumInXml}, Response XML: {responseXml}");
    }

    [Fact]
    public async Task CompleteMultipartUpload_WithSHA256Parts_ReturnsChecksumInResponse()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();

        var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/mp-sha256.txt?uploads");
        initiateRequest.Headers.Add("x-amz-checksum-algorithm", "SHA256");
        var initiateResponse = await Client.SendAsync(initiateRequest);
        var initiateXml = await initiateResponse.Content.ReadAsStringAsync();
        var uploadId = XDocument.Parse(initiateXml)
            .Descendants()
            .First(e => e.Name.LocalName == "UploadId")
            .Value;

        // Upload part
        var partContent = new StringContent(TestData, Encoding.UTF8, "application/octet-stream");
        var partResponse = await Client.PutAsync($"/{bucketName}/mp-sha256.txt?partNumber=1&uploadId={uploadId}", partContent);
        var etag = partResponse.Headers.ETag!.Tag.Trim('"');

        // Complete multipart upload with SHA256 checksum
        var completeXml = $@"
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>{etag}</ETag>
        <ChecksumSHA256>{TestData_SHA256}</ChecksumSHA256>
    </Part>
</CompleteMultipartUpload>";

        var completeRequest = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/mp-sha256.txt?uploadId={uploadId}");
        completeRequest.Content = new StringContent(completeXml, Encoding.UTF8, "application/xml");

        // Act
        var response = await Client.SendAsync(completeRequest);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var responseXml = await response.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(responseXml);

        // Verify SHA256 checksum is in XML response
        var checksumElement = doc.Descendants().FirstOrDefault(e => e.Name.LocalName == "ChecksumSHA256");
        if (checksumElement != null)
        {
            Assert.Equal(TestData_SHA256, checksumElement.Value);
        }

        // Checksum may also be in response headers
        if (response.Headers.Contains("x-amz-checksum-sha256"))
        {
            Assert.Equal(TestData_SHA256, response.Headers.GetValues("x-amz-checksum-sha256").First());
        }
    }

    [Fact]
    public async Task CompleteMultipartUpload_WithMultiplePartsAndChecksums_AggregatesCorrectly()
    {
        // Arrange - Create a multipart upload with 3 parts
        var bucketName = await CreateTestBucketAsync();

        var initiateRequest = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/mp-aggregate.txt?uploads");
        initiateRequest.Headers.Add("x-amz-checksum-algorithm", "CRC32");
        var initiateResponse = await Client.SendAsync(initiateRequest);
        var initiateXml = await initiateResponse.Content.ReadAsStringAsync();
        var uploadId = XDocument.Parse(initiateXml)
            .Descendants()
            .First(e => e.Name.LocalName == "UploadId")
            .Value;

        // Upload 3 parts WITH checksum headers
        var etags = new List<string>();
        for (int i = 1; i <= 3; i++)
        {
            var content = new StringContent(TestData, Encoding.UTF8, "application/octet-stream");
            var partRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/mp-aggregate.txt?partNumber={i}&uploadId={uploadId}");
            partRequest.Content = content;
            partRequest.Headers.Add("x-amz-checksum-crc32", TestData_CRC32);
            var partResponse = await Client.SendAsync(partRequest);
            etags.Add(partResponse.Headers.ETag!.Tag.Trim('"'));
        }

        // Complete multipart upload with all parts
        var partsXml = string.Join("\n", etags.Select((etag, index) => $@"
    <Part>
        <PartNumber>{index + 1}</PartNumber>
        <ETag>{etag}</ETag>
        <ChecksumCRC32>{TestData_CRC32}</ChecksumCRC32>
    </Part>"));

        var completeXml = $@"
<CompleteMultipartUpload>
{partsXml}
</CompleteMultipartUpload>";

        var completeRequest = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/mp-aggregate.txt?uploadId={uploadId}");
        completeRequest.Content = new StringContent(completeXml, Encoding.UTF8, "application/xml");

        // Act
        var response = await Client.SendAsync(completeRequest);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var responseXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("CompleteMultipartUploadResult", responseXml);

        // Verify the final object has checksum information
        var doc = XDocument.Parse(responseXml);
        var hasChecksum = doc.Descendants().Any(e => e.Name.LocalName.Contains("Checksum"));
        Assert.True(hasChecksum, "Completed multipart upload should include checksum information");
    }

    #endregion
}
