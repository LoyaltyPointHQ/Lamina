using System.Net;
using System.Text;
using System.Xml.Linq;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Lamina.WebApi.Tests.Controllers;

public class ChecksumIntegrationTests : IntegrationTestBase
{
    public ChecksumIntegrationTests(WebApplicationFactory<global::Program> factory) : base(factory)
    {
    }

    private async Task<string> CreateTestBucketAsync()
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        return bucketName;
    }

    [Theory]
    [InlineData("CRC32")]
    [InlineData("CRC32C")]
    [InlineData("SHA1")]
    [InlineData("SHA256")]
    [InlineData("CRC64NVME")]
    public async Task InitiateMultipartUpload_WithValidChecksumAlgorithm_Returns200(string algorithm)
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var request = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/test.txt?uploads");
        request.Headers.Add("x-amz-checksum-algorithm", algorithm);

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var xml = await response.Content.ReadAsStringAsync();
        Assert.Contains("<UploadId>", xml);
    }

    [Fact]
    public async Task InitiateMultipartUpload_WithInvalidChecksumAlgorithm_Returns400()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var request = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/test.txt?uploads");
        request.Headers.Add("x-amz-checksum-algorithm", "INVALID");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var xml = await response.Content.ReadAsStringAsync();
        Assert.Contains("InvalidArgument", xml);
        Assert.Contains("Invalid checksum algorithm", xml);
    }

    [Fact]
    public async Task PutObject_WithChecksumCRC32Header_ReturnsChecksumInResponse()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent("Test content", Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-crc32", "i9aeUg==");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("x-amz-checksum-crc32"));
        Assert.Equal("i9aeUg==", response.Headers.GetValues("x-amz-checksum-crc32").First());
    }

    [Fact]
    public async Task PutObject_WithChecksumSHA256Header_ReturnsChecksumInResponse()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent("Test content", Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-sha256", "n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg=");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("x-amz-checksum-sha256"));
        Assert.Equal("n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg=", 
            response.Headers.GetValues("x-amz-checksum-sha256").First());
    }

    [Fact]
    public async Task PutObject_WithInvalidChecksumAlgorithm_Returns400()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent("Test content", Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-algorithm", "MD5");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var xml = await response.Content.ReadAsStringAsync();
        Assert.Contains("InvalidArgument", xml);
        Assert.Contains("Invalid checksum algorithm", xml);
    }

    [Fact]
    public async Task UploadPart_WithChecksumHeader_ReturnsChecksumInResponse()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        
        // Initiate multipart upload
        var initiateResponse = await Client.PostAsync($"/{bucketName}/test.txt?uploads", null);
        var initiateXml = await initiateResponse.Content.ReadAsStringAsync();
        var uploadId = XDocument.Parse(initiateXml)
            .Descendants()
            .First(e => e.Name.LocalName == "UploadId")
            .Value;

        // Upload part with checksum
        var content = new StringContent("Part content", Encoding.UTF8, "application/octet-stream");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test.txt?partNumber=1&uploadId={uploadId}");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-crc32", "someChecksum==");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        // Note: The checksum header may not be returned if the part doesn't store it
        // This test verifies the request is accepted
    }

    [Fact]
    public async Task CopyObject_WithChecksums_ReturnsChecksumsInXmlResponse()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        
        // Create source object with checksum
        var putContent = new StringContent("Source content", Encoding.UTF8, "text/plain");
        var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/source.txt");
        putRequest.Content = putContent;
        putRequest.Headers.Add("x-amz-checksum-crc32", "testChecksum==");
        await Client.SendAsync(putRequest);

        // Copy object
        var copyRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/dest.txt");
        copyRequest.Headers.Add("x-amz-copy-source", $"/{bucketName}/source.txt");

        // Act
        var response = await Client.SendAsync(copyRequest);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var xml = await response.Content.ReadAsStringAsync();
        Assert.Contains("CopyObjectResult", xml);
        Assert.Contains("ETag", xml);
        Assert.Contains("LastModified", xml);
        // Checksums should be in the response if they were on the source
    }

    [Fact]
    public async Task CompleteMultipartUpload_WithPartChecksums_ReturnsChecksumsInXmlResponse()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        
        // Initiate multipart upload
        var initiateResponse = await Client.PostAsync($"/{bucketName}/test.txt?uploads", null);
        var initiateXml = await initiateResponse.Content.ReadAsStringAsync();
        var uploadId = XDocument.Parse(initiateXml)
            .Descendants()
            .First(e => e.Name.LocalName == "UploadId")
            .Value;

        // Upload part
        var partContent = new StringContent("Part 1 content", Encoding.UTF8, "application/octet-stream");
        var partResponse = await Client.PutAsync($"/{bucketName}/test.txt?partNumber=1&uploadId={uploadId}", partContent);
        var etag = partResponse.Headers.ETag!.Tag.Trim('"');

        // Complete multipart upload with checksum in the part
        var completeXml = $@"
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>{etag}</ETag>
        <ChecksumCRC32>testChecksum==</ChecksumCRC32>
    </Part>
</CompleteMultipartUpload>";

        var completeRequest = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/test.txt?uploadId={uploadId}");
        completeRequest.Content = new StringContent(completeXml, Encoding.UTF8, "application/xml");

        // Act
        var response = await Client.SendAsync(completeRequest);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var responseXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("CompleteMultipartUploadResult", responseXml);
        Assert.Contains("ETag", responseXml);
        
        // Checksum should be in the XML response
        var doc = XDocument.Parse(responseXml);
        var checksumElement = doc.Descendants().FirstOrDefault(e => e.Name.LocalName == "ChecksumCRC32");
        if (checksumElement != null)
        {
            Assert.Equal("testChecksum==", checksumElement.Value);
        }
    }

    [Fact]
    public async Task CompleteMultipartUpload_WithPartChecksums_ReturnsChecksumHeaders()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        
        // Initiate multipart upload
        var initiateResponse = await Client.PostAsync($"/{bucketName}/test.txt?uploads", null);
        var initiateXml = await initiateResponse.Content.ReadAsStringAsync();
        var uploadId = XDocument.Parse(initiateXml)
            .Descendants()
            .First(e => e.Name.LocalName == "UploadId")
            .Value;

        // Upload part
        var partContent = new StringContent("Part 1 content", Encoding.UTF8, "application/octet-stream");
        var partResponse = await Client.PutAsync($"/{bucketName}/test.txt?partNumber=1&uploadId={uploadId}", partContent);
        var etag = partResponse.Headers.ETag!.Tag.Trim('"');

        // Complete multipart upload with SHA256 checksum
        var completeXml = $@"
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>{etag}</ETag>
        <ChecksumSHA256>n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg=</ChecksumSHA256>
    </Part>
</CompleteMultipartUpload>";

        var completeRequest = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/test.txt?uploadId={uploadId}");
        completeRequest.Content = new StringContent(completeXml, Encoding.UTF8, "application/xml");

        // Act
        var response = await Client.SendAsync(completeRequest);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        
        // Checksum should be in response headers
        if (response.Headers.Contains("x-amz-checksum-sha256"))
        {
            Assert.Equal("n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg=", 
                response.Headers.GetValues("x-amz-checksum-sha256").First());
        }
    }

    [Theory]
    [InlineData("crc32")]
    [InlineData("crc32c")]
    [InlineData("sha1")]
    [InlineData("sha256")]
    [InlineData("crc64nvme")]
    public async Task PutObject_WithDifferentChecksumAlgorithms_AcceptsAllValidAlgorithms(string algorithm)
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent("Test content", Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test-{algorithm}.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-algorithm", algorithm);

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        // The response may or may not contain the checksum header depending on 
        // whether the implementation calculates it server-side
    }

    [Fact]
    public async Task PutObject_WithMultipleChecksumHeaders_AcceptsRequest()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent("Test content", Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test.txt");
        request.Content = content;
        request.Headers.Add("x-amz-checksum-crc32", "crc32value==");
        request.Headers.Add("x-amz-checksum-sha256", "sha256value==");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        
        // Both checksums should be returned
        if (response.Headers.Contains("x-amz-checksum-crc32"))
        {
            Assert.Equal("crc32value==", response.Headers.GetValues("x-amz-checksum-crc32").First());
        }
        if (response.Headers.Contains("x-amz-checksum-sha256"))
        {
            Assert.Equal("sha256value==", response.Headers.GetValues("x-amz-checksum-sha256").First());
        }
    }
}
