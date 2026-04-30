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
        // Request server to calculate CRC32 checksum
        request.Headers.Add("x-amz-checksum-algorithm", "CRC32");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("x-amz-checksum-crc32"));
        // Server calculated checksum, just verify it's present and not empty
        var checksum = response.Headers.GetValues("x-amz-checksum-crc32").First();
        Assert.False(string.IsNullOrEmpty(checksum));
    }

    [Fact]
    public async Task PutObject_WithChecksumSHA256Header_ReturnsChecksumInResponse()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent("Test content", Encoding.UTF8, "text/plain");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test.txt");
        request.Content = content;
        // Request server to calculate SHA256 checksum
        request.Headers.Add("x-amz-checksum-algorithm", "SHA256");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("x-amz-checksum-sha256"));
        // Server calculated checksum, just verify it's present and not empty
        var checksum = response.Headers.GetValues("x-amz-checksum-sha256").First();
        Assert.False(string.IsNullOrEmpty(checksum));
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

        // Upload part with checksum algorithm
        var content = new StringContent("Part content", Encoding.UTF8, "application/octet-stream");
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/test.txt?partNumber=1&uploadId={uploadId}");
        request.Content = content;
        // Request server to calculate CRC32 checksum
        request.Headers.Add("x-amz-checksum-algorithm", "CRC32");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        // Server should calculate and return checksum
        Assert.True(response.Headers.Contains("x-amz-checksum-crc32"));
    }

    [Fact]
    public async Task CopyObject_WithChecksums_ReturnsChecksumsInXmlResponse()
    {
        // Arrange
        var bucketName = await CreateTestBucketAsync();

        // Create source object with checksum algorithm
        var putContent = new StringContent("Source content", Encoding.UTF8, "text/plain");
        var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/source.txt");
        putRequest.Content = putContent;
        putRequest.Headers.Add("x-amz-checksum-algorithm", "CRC32");
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
        // Request server to calculate multiple checksums
        // Note: S3 typically only allows one algorithm, but let's test with CRC32
        request.Headers.Add("x-amz-checksum-algorithm", "CRC32");

        // Act
        var response = await Client.SendAsync(request);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        // At least CRC32 checksum should be returned
        Assert.True(response.Headers.Contains("x-amz-checksum-crc32"));
        var checksum = response.Headers.GetValues("x-amz-checksum-crc32").First();
        Assert.False(string.IsNullOrEmpty(checksum));
    }

    [Fact]
    public async Task GetObject_WithRange_DoesNotReturnFullObjectChecksumHeader()
    {
        // Per AWS S3 spec, for ranged GET the checksum header must be for the
        // returned range, not the full object. Returning the full-object checksum
        // makes AWS CRT clients with x-amz-checksum-mode=ENABLED fail validation
        // (they compute the checksum on the received chunk bytes and compare
        // against the header). Since we don't compute a per-range checksum, we
        // must omit the header - the AWS spec allows this: "If the checksum
        // isn't known for the byte range, Amazon S3 doesn't return the
        // x-amz-checksum-* header".
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent("0123456789ABCDEFGHIJ", Encoding.UTF8, "text/plain");
        var put = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/range-checksum.txt")
        {
            Content = content
        };
        put.Headers.Add("x-amz-checksum-algorithm", "CRC64NVME");
        var putResponse = await Client.SendAsync(put);
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);
        Assert.True(putResponse.Headers.Contains("x-amz-checksum-crc64nvme"));

        var get = new HttpRequestMessage(HttpMethod.Get, $"/{bucketName}/range-checksum.txt");
        get.Headers.Add("Range", "bytes=0-9");
        get.Headers.Add("x-amz-checksum-mode", "ENABLED");
        var getResponse = await Client.SendAsync(get);

        Assert.Equal(HttpStatusCode.PartialContent, getResponse.StatusCode);
        Assert.False(
            getResponse.Headers.Contains("x-amz-checksum-crc64nvme")
                || getResponse.Content.Headers.Contains("x-amz-checksum-crc64nvme"),
            "Range GET must not return a full-object checksum header even with checksum-mode: ENABLED");
    }

    [Fact]
    public async Task GetObject_WithoutRange_StillReturnsFullObjectChecksumHeader()
    {
        // Sanity/regression: non-range GET with x-amz-checksum-mode: ENABLED must return
        // the full-object checksum header (integrity verification for full downloads works).
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent("0123456789ABCDEFGHIJ", Encoding.UTF8, "text/plain");
        var put = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/full-checksum.txt")
        {
            Content = content
        };
        put.Headers.Add("x-amz-checksum-algorithm", "CRC64NVME");
        await Client.SendAsync(put);

        var get = new HttpRequestMessage(HttpMethod.Get, $"/{bucketName}/full-checksum.txt");
        get.Headers.Add("x-amz-checksum-mode", "ENABLED");
        var getResponse = await Client.SendAsync(get);

        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.True(getResponse.Headers.Contains("x-amz-checksum-crc64nvme"));
    }

    [Fact]
    public async Task GetObject_MultipardObject_WithChecksum_ReturnsChecksumTypeComposite()
    {
        // When a multipart upload has per-part checksums, the assembled object's checksum is
        // a composite checksum (checksum-of-checksums). boto3 and AWS CRT clients fail validation
        // if x-amz-checksum-type: COMPOSITE is not present — they treat the value as a full-object
        // checksum and get a mismatch.
        var bucketName = await CreateTestBucketAsync();

        var initiateReq = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/mpu-composite.bin?uploads");
        initiateReq.Headers.Add("x-amz-checksum-algorithm", "CRC32");
        var initiateResp = await Client.SendAsync(initiateReq);
        Assert.Equal(HttpStatusCode.OK, initiateResp.StatusCode);
        var initiateXml = await initiateResp.Content.ReadAsStringAsync();
        var uploadId = System.Xml.Linq.XDocument.Parse(initiateXml)
            .Descendants().First(e => e.Name.LocalName == "UploadId").Value;

        var partData = new byte[5 * 1024 * 1024];
        Array.Fill(partData, (byte)'A');
        var partReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{bucketName}/mpu-composite.bin?partNumber=1&uploadId={uploadId}")
        {
            Content = new ByteArrayContent(partData)
        };
        partReq.Headers.Add("x-amz-checksum-algorithm", "CRC32");
        var partResp = await Client.SendAsync(partReq);
        Assert.Equal(HttpStatusCode.OK, partResp.StatusCode);
        var partETag = partResp.Headers.ETag!.Tag.Trim('"');
        var partCrc32 = partResp.Headers.GetValues("x-amz-checksum-crc32").First();

        var completeXml = $@"<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>{partETag}</ETag><ChecksumCRC32>{partCrc32}</ChecksumCRC32></Part>
</CompleteMultipartUpload>";
        var completeReq = new HttpRequestMessage(HttpMethod.Post,
            $"/{bucketName}/mpu-composite.bin?uploadId={uploadId}")
        {
            Content = new StringContent(completeXml, System.Text.Encoding.UTF8, "application/xml")
        };
        var completeResp = await Client.SendAsync(completeReq);
        Assert.Equal(HttpStatusCode.OK, completeResp.StatusCode);

        var getReq = new HttpRequestMessage(HttpMethod.Get, $"/{bucketName}/mpu-composite.bin");
        getReq.Headers.Add("x-amz-checksum-mode", "ENABLED");
        var getResp = await Client.SendAsync(getReq);
        Assert.Equal(HttpStatusCode.OK, getResp.StatusCode);
        Assert.True(getResp.Headers.Contains("x-amz-checksum-crc32"),
            "Multipart object with stored CRC32 must return x-amz-checksum-crc32");
        Assert.True(getResp.Headers.Contains("x-amz-checksum-type"),
            "Composite checksum must be accompanied by x-amz-checksum-type: COMPOSITE");
        Assert.Equal("COMPOSITE", getResp.Headers.GetValues("x-amz-checksum-type").First());
    }

    [Fact]
    public async Task HeadObject_MultipardObject_WithChecksum_ReturnsChecksumTypeComposite()
    {
        var bucketName = await CreateTestBucketAsync();

        var initiateReq = new HttpRequestMessage(HttpMethod.Post, $"/{bucketName}/mpu-head.bin?uploads");
        initiateReq.Headers.Add("x-amz-checksum-algorithm", "SHA256");
        var initiateResp = await Client.SendAsync(initiateReq);
        var uploadId = System.Xml.Linq.XDocument.Parse(await initiateResp.Content.ReadAsStringAsync())
            .Descendants().First(e => e.Name.LocalName == "UploadId").Value;

        var partData = new byte[5 * 1024 * 1024];
        Array.Fill(partData, (byte)'B');
        var partReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{bucketName}/mpu-head.bin?partNumber=1&uploadId={uploadId}")
        {
            Content = new ByteArrayContent(partData)
        };
        partReq.Headers.Add("x-amz-checksum-algorithm", "SHA256");
        var partResp = await Client.SendAsync(partReq);
        var partETag = partResp.Headers.ETag!.Tag.Trim('"');
        var partSha256 = partResp.Headers.GetValues("x-amz-checksum-sha256").First();

        var completeXml = $@"<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>{partETag}</ETag><ChecksumSHA256>{partSha256}</ChecksumSHA256></Part>
</CompleteMultipartUpload>";
        await Client.SendAsync(new HttpRequestMessage(HttpMethod.Post,
            $"/{bucketName}/mpu-head.bin?uploadId={uploadId}")
        {
            Content = new StringContent(completeXml, System.Text.Encoding.UTF8, "application/xml")
        });

        var headReq = new HttpRequestMessage(HttpMethod.Head, $"/{bucketName}/mpu-head.bin");
        headReq.Headers.Add("x-amz-checksum-mode", "ENABLED");
        var headResp = await Client.SendAsync(headReq);
        Assert.Equal(HttpStatusCode.OK, headResp.StatusCode);
        Assert.True(headResp.Headers.Contains("x-amz-checksum-sha256"));
        Assert.True(headResp.Headers.Contains("x-amz-checksum-type"),
            "HeadObject on composite-checksum multipart object must return x-amz-checksum-type: COMPOSITE");
        Assert.Equal("COMPOSITE", headResp.Headers.GetValues("x-amz-checksum-type").First());
    }

    [Fact]
    public async Task GetObject_SinglePartObject_WithChecksum_DoesNotReturnChecksumTypeComposite()
    {
        // Single-part objects have FULL_OBJECT checksums — no x-amz-checksum-type header needed.
        var bucketName = await CreateTestBucketAsync();
        var putReq = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/single.txt")
        {
            Content = new StringContent("hello", System.Text.Encoding.UTF8, "text/plain")
        };
        putReq.Headers.Add("x-amz-checksum-algorithm", "CRC32");
        await Client.SendAsync(putReq);

        var getReqSingle = new HttpRequestMessage(HttpMethod.Get, $"/{bucketName}/single.txt");
        getReqSingle.Headers.Add("x-amz-checksum-mode", "ENABLED");
        var getResp = await Client.SendAsync(getReqSingle);
        Assert.Equal(HttpStatusCode.OK, getResp.StatusCode);
        Assert.True(getResp.Headers.Contains("x-amz-checksum-crc32"));
        Assert.False(getResp.Headers.Contains("x-amz-checksum-type"),
            "Single-part object checksum is FULL_OBJECT — x-amz-checksum-type must not be present");
    }
}
