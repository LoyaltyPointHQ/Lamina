using System.Net;
using System.Text;
using System.Xml.Serialization;
using Lamina.Core.Models;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Lamina.WebApi.Tests.Controllers;

public class MultipartUploadS3ComplianceTests : IntegrationTestBase
{
    public MultipartUploadS3ComplianceTests(WebApplicationFactory<global::Program> factory) : base(factory)
    {
    }

    private async Task<string> CreateTestBucketAsync()
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        return bucketName;
    }

    [Fact]
    public async Task CompleteMultipartUpload_ValidParts_ReturnsProperMultipartETag()
    {
        var bucketName = await CreateTestBucketAsync();

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/s3-compliance-test.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Upload parts
        var part1Response = await Client.PutAsync(
            $"/{bucketName}/s3-compliance-test.bin?partNumber=1&uploadId={initResult.UploadId}",
            new StringContent("Test part 1 content", Encoding.UTF8));
        Assert.Equal(HttpStatusCode.OK, part1Response.StatusCode);
        var part1ETag = part1Response.Headers.GetValues("ETag").First().Trim('"');

        var part2Response = await Client.PutAsync(
            $"/{bucketName}/s3-compliance-test.bin?partNumber=2&uploadId={initResult.UploadId}",
            new StringContent("Test part 2 content", Encoding.UTF8));
        Assert.Equal(HttpStatusCode.OK, part2Response.StatusCode);
        var part2ETag = part2Response.Headers.GetValues("ETag").First().Trim('"');

        // Create complete request XML
        var completeRequestXml = $@"<?xml version=""1.0"" encoding=""UTF-8""?>
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>{part1ETag}</ETag>
    </Part>
    <Part>
        <PartNumber>2</PartNumber>
        <ETag>{part2ETag}</ETag>
    </Part>
</CompleteMultipartUpload>";

        // Complete multipart upload
        var completeResponse = await Client.PostAsync(
            $"/{bucketName}/s3-compliance-test.bin?uploadId={initResult.UploadId}",
            new StringContent(completeRequestXml, Encoding.UTF8, "application/xml"));

        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);

        // Parse response
        var completeXml = await completeResponse.Content.ReadAsStringAsync();
        var completeSerializer = new XmlSerializer(typeof(CompleteMultipartUploadResult));
        using var completeReader = new StringReader(completeXml);
        var completeResult = (CompleteMultipartUploadResult?)completeSerializer.Deserialize(completeReader);
        Assert.NotNull(completeResult);

        // Verify multipart ETag format: should be {hash}-{partCount}
        var finalETag = completeResult.ETag.Trim('"');
        Assert.Contains("-", finalETag);
        Assert.EndsWith("-2", finalETag); // Should end with "-2" for 2 parts

        // Verify ETag is not just concatenated individual ETags
        Assert.NotEqual(part1ETag + part2ETag, finalETag);

        // Verify ETag is hexadecimal before the dash
        var hashPart = finalETag.Split('-')[0];
        Assert.Equal(32, hashPart.Length); // MD5 hash should be 32 hex characters
        Assert.True(hashPart.All(c => char.IsDigit(c) || (c >= 'a' && c <= 'f')));

        // Verify response headers
        Assert.True(completeResponse.Headers.Contains("x-amz-version-id"));
        Assert.Equal("null", completeResponse.Headers.GetValues("x-amz-version-id").First());
    }

    [Fact]
    public async Task CompleteMultipartUpload_WithChecksums_ReturnsChecksumHeaders()
    {
        var bucketName = await CreateTestBucketAsync();

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/checksum-test.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Upload part
        var partResponse = await Client.PutAsync(
            $"/{bucketName}/checksum-test.bin?partNumber=1&uploadId={initResult.UploadId}",
            new StringContent("Test content for checksum", Encoding.UTF8));
        Assert.Equal(HttpStatusCode.OK, partResponse.StatusCode);
        var partETag = partResponse.Headers.GetValues("ETag").First().Trim('"');

        // Create complete request XML with checksum
        var completeRequestXml = $@"<?xml version=""1.0"" encoding=""UTF-8""?>
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>{partETag}</ETag>
        <ChecksumCRC32>example-crc32</ChecksumCRC32>
    </Part>
</CompleteMultipartUpload>";

        // Complete multipart upload
        var completeResponse = await Client.PostAsync(
            $"/{bucketName}/checksum-test.bin?uploadId={initResult.UploadId}",
            new StringContent(completeRequestXml, Encoding.UTF8, "application/xml"));

        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);

        // Verify checksum header is returned
        Assert.True(completeResponse.Headers.Contains("x-amz-checksum-crc32"));
        Assert.Equal("example-crc32", completeResponse.Headers.GetValues("x-amz-checksum-crc32").First());
    }

    [Fact]
    public async Task CompleteMultipartUpload_PartsNotInOrder_ReturnsBadRequest()
    {
        var bucketName = await CreateTestBucketAsync();

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/order-test.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Upload parts
        await Client.PutAsync(
            $"/{bucketName}/order-test.bin?partNumber=1&uploadId={initResult.UploadId}",
            new StringContent("Part 1", Encoding.UTF8));

        await Client.PutAsync(
            $"/{bucketName}/order-test.bin?partNumber=2&uploadId={initResult.UploadId}",
            new StringContent("Part 2", Encoding.UTF8));

        // Create complete request XML with parts in wrong order (2, 1 instead of 1, 2)
        var completeRequestXml = @"<?xml version=""1.0"" encoding=""UTF-8""?>
<CompleteMultipartUpload>
    <Part>
        <PartNumber>2</PartNumber>
        <ETag>dummy-etag-2</ETag>
    </Part>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>dummy-etag-1</ETag>
    </Part>
</CompleteMultipartUpload>";

        // Complete multipart upload - should fail
        var completeResponse = await Client.PostAsync(
            $"/{bucketName}/order-test.bin?uploadId={initResult.UploadId}",
            new StringContent(completeRequestXml, Encoding.UTF8, "application/xml"));

        Assert.Equal(HttpStatusCode.BadRequest, completeResponse.StatusCode);

        var errorXml = await completeResponse.Content.ReadAsStringAsync();
        Assert.Contains("InvalidPartOrder", errorXml);
    }

    [Fact]
    public async Task CompleteMultipartUpload_InvalidETag_ReturnsBadRequest()
    {
        var bucketName = await CreateTestBucketAsync();

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/etag-test.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Upload part
        await Client.PutAsync(
            $"/{bucketName}/etag-test.bin?partNumber=1&uploadId={initResult.UploadId}",
            new StringContent("Part content", Encoding.UTF8));

        // Create complete request XML with wrong ETag
        var completeRequestXml = @"<?xml version=""1.0"" encoding=""UTF-8""?>
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>wrong-etag</ETag>
    </Part>
</CompleteMultipartUpload>";

        // Complete multipart upload - should fail
        var completeResponse = await Client.PostAsync(
            $"/{bucketName}/etag-test.bin?uploadId={initResult.UploadId}",
            new StringContent(completeRequestXml, Encoding.UTF8, "application/xml"));

        Assert.Equal(HttpStatusCode.BadRequest, completeResponse.StatusCode);

        var errorXml = await completeResponse.Content.ReadAsStringAsync();
        Assert.Contains("InvalidPart", errorXml);
    }

    [Fact]
    public async Task HeadMultipartUpload_ReturnsPartMetadata()
    {
        var bucketName = await CreateTestBucketAsync();

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/head-test.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Upload parts
        var part1Response = await Client.PutAsync(
            $"/{bucketName}/head-test.bin?partNumber=1&uploadId={initResult.UploadId}",
            new StringContent("Part 1 content with 50 bytes of data here!!!!", Encoding.UTF8));
        Assert.Equal(HttpStatusCode.OK, part1Response.StatusCode);

        var part2Response = await Client.PutAsync(
            $"/{bucketName}/head-test.bin?partNumber=2&uploadId={initResult.UploadId}",
            new StringContent("Part 2 content", Encoding.UTF8));
        Assert.Equal(HttpStatusCode.OK, part2Response.StatusCode);

        // HEAD request to get metadata
        var headRequest = new HttpRequestMessage(HttpMethod.Head,
            $"/{bucketName}/head-test.bin?uploadId={initResult.UploadId}");
        var headResponse = await Client.SendAsync(headRequest);

        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);

        // Verify metadata headers
        Assert.True(headResponse.Headers.Contains("x-amz-parts-count"));
        Assert.Equal("2", headResponse.Headers.GetValues("x-amz-parts-count").First());

        Assert.True(headResponse.Headers.Contains("x-amz-last-part-number"));
        Assert.Equal("2", headResponse.Headers.GetValues("x-amz-last-part-number").First());

        Assert.True(headResponse.Headers.Contains("x-amz-total-size"));
        var totalSize = long.Parse(headResponse.Headers.GetValues("x-amz-total-size").First());
        Assert.True(totalSize > 0);
    }

    [Fact]
    public async Task HeadMultipartUpload_NonExistentUpload_Returns404()
    {
        var bucketName = await CreateTestBucketAsync();

        var headRequest = new HttpRequestMessage(HttpMethod.Head,
            $"/{bucketName}/nonexistent.bin?uploadId=fake-upload-id");
        var headResponse = await Client.SendAsync(headRequest);

        Assert.Equal(HttpStatusCode.NotFound, headResponse.StatusCode);
    }
}