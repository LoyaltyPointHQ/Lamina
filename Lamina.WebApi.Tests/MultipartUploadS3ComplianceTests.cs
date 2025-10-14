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

        // Upload part WITH checksum header
        var partRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/checksum-test.bin?partNumber=1&uploadId={initResult.UploadId}");
        partRequest.Content = new StringContent("Test content for checksum", Encoding.UTF8);
        partRequest.Headers.Add("x-amz-checksum-algorithm", "CRC32");  // Request server to calculate checksum
        var partResponse = await Client.SendAsync(partRequest);
        Assert.Equal(HttpStatusCode.OK, partResponse.StatusCode);
        var partETag = partResponse.Headers.GetValues("ETag").First().Trim('"');

        // Get the calculated checksum from the response
        var calculatedChecksum = partResponse.Headers.GetValues("x-amz-checksum-crc32").FirstOrDefault();
        Assert.NotNull(calculatedChecksum);

        // Create complete request XML with checksum
        var completeRequestXml = $@"<?xml version=""1.0"" encoding=""UTF-8""?>
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>{partETag}</ETag>
        <ChecksumCRC32>{calculatedChecksum}</ChecksumCRC32>
    </Part>
</CompleteMultipartUpload>";

        // Complete multipart upload
        var completeResponse = await Client.PostAsync(
            $"/{bucketName}/checksum-test.bin?uploadId={initResult.UploadId}",
            new StringContent(completeRequestXml, Encoding.UTF8, "application/xml"));

        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);

        // Verify checksum header is returned (aggregated from stored part checksums)
        Assert.True(completeResponse.Headers.Contains("x-amz-checksum-crc32"));
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

    [Fact]
    public async Task UploadPartCopy_EntireObject_Succeeds()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create source object
        var sourceContent = "This is the source object content for copying";
        await Client.PutAsync($"/{bucketName}/source-object.txt",
            new StringContent(sourceContent, Encoding.UTF8));

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/dest-object.txt?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Upload part using UploadPartCopy
        var copyRequest = new HttpRequestMessage(HttpMethod.Put,
            $"/{bucketName}/dest-object.txt?partNumber=1&uploadId={initResult.UploadId}");
        copyRequest.Headers.Add("x-amz-copy-source", $"/{bucketName}/source-object.txt");
        var copyResponse = await Client.SendAsync(copyRequest);

        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);
        Assert.True(copyResponse.Headers.Contains("ETag"));

        // Verify response contains CopyPartResult XML
        var copyXml = await copyResponse.Content.ReadAsStringAsync();
        Assert.Contains("<CopyPartResult", copyXml);
        Assert.Contains("<ETag>", copyXml);
        Assert.Contains("<LastModified>", copyXml);
    }

    [Fact]
    public async Task UploadPartCopy_WithByteRange_CopiesOnlySpecifiedBytes()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create source object with known content
        var sourceContent = "0123456789ABCDEFGHIJ"; // 20 bytes
        await Client.PutAsync($"/{bucketName}/source.bin",
            new StringContent(sourceContent, Encoding.UTF8));

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/dest.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Copy bytes 5-14 (10 bytes: "56789ABCDE")
        var copyRequest = new HttpRequestMessage(HttpMethod.Put,
            $"/{bucketName}/dest.bin?partNumber=1&uploadId={initResult.UploadId}");
        copyRequest.Headers.Add("x-amz-copy-source", $"/{bucketName}/source.bin");
        copyRequest.Headers.Add("x-amz-copy-source-range", "bytes=5-14");
        var copyResponse = await Client.SendAsync(copyRequest);

        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);
        var partETag = copyResponse.Headers.GetValues("ETag").First().Trim('\"');

        // Complete the upload with just this one part
        var completeRequestXml = $@"<?xml version=""1.0"" encoding=""UTF-8""?>
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>{partETag}</ETag>
    </Part>
</CompleteMultipartUpload>";

        var completeResponse = await Client.PostAsync(
            $"/{bucketName}/dest.bin?uploadId={initResult.UploadId}",
            new StringContent(completeRequestXml, Encoding.UTF8, "application/xml"));

        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);

        // Verify the final object contains only the byte range
        var getResponse = await Client.GetAsync($"/{bucketName}/dest.bin");
        var resultContent = await getResponse.Content.ReadAsStringAsync();
        Assert.Equal("56789ABCDE", resultContent);
    }

    [Fact]
    public async Task UploadPartCopy_MultiplePartsFromSameSource_Succeeds()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create large source object
        var sourceContent = new string('A', 10000) + new string('B', 10000) + new string('C', 10000); // 30KB
        await Client.PutAsync($"/{bucketName}/large-source.bin",
            new StringContent(sourceContent, Encoding.UTF8));

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/large-dest.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Copy part 1: bytes 0-9999 (10KB of 'A')
        var copy1Request = new HttpRequestMessage(HttpMethod.Put,
            $"/{bucketName}/large-dest.bin?partNumber=1&uploadId={initResult.UploadId}");
        copy1Request.Headers.Add("x-amz-copy-source", $"/{bucketName}/large-source.bin");
        copy1Request.Headers.Add("x-amz-copy-source-range", "bytes=0-9999");
        var copy1Response = await Client.SendAsync(copy1Request);
        Assert.Equal(HttpStatusCode.OK, copy1Response.StatusCode);
        var part1ETag = copy1Response.Headers.GetValues("ETag").First().Trim('\"');

        // Copy part 2: bytes 10000-19999 (10KB of 'B')
        var copy2Request = new HttpRequestMessage(HttpMethod.Put,
            $"/{bucketName}/large-dest.bin?partNumber=2&uploadId={initResult.UploadId}");
        copy2Request.Headers.Add("x-amz-copy-source", $"/{bucketName}/large-source.bin");
        copy2Request.Headers.Add("x-amz-copy-source-range", "bytes=10000-19999");
        var copy2Response = await Client.SendAsync(copy2Request);
        Assert.Equal(HttpStatusCode.OK, copy2Response.StatusCode);
        var part2ETag = copy2Response.Headers.GetValues("ETag").First().Trim('\"');

        // Complete multipart upload
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

        var completeResponse = await Client.PostAsync(
            $"/{bucketName}/large-dest.bin?uploadId={initResult.UploadId}",
            new StringContent(completeRequestXml, Encoding.UTF8, "application/xml"));

        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);

        // Verify the final object
        var getResponse = await Client.GetAsync($"/{bucketName}/large-dest.bin");
        var resultContent = await getResponse.Content.ReadAsStringAsync();
        Assert.Equal(20000, resultContent.Length);
        Assert.StartsWith(new string('A', 10000), resultContent);
        Assert.StartsWith(new string('B', 10000), resultContent.Substring(10000));
    }

    [Fact]
    public async Task UploadPartCopy_AcrossBuckets_Succeeds()
    {
        var sourceBucket = await CreateTestBucketAsync();
        var destBucket = await CreateTestBucketAsync();

        // Create source object
        var sourceContent = "Content in source bucket";
        await Client.PutAsync($"/{sourceBucket}/source-object.txt",
            new StringContent(sourceContent, Encoding.UTF8));

        // Initiate multipart upload in destination bucket
        var initResponse = await Client.PostAsync($"/{destBucket}/dest-object.txt?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Copy from source bucket to dest bucket
        var copyRequest = new HttpRequestMessage(HttpMethod.Put,
            $"/{destBucket}/dest-object.txt?partNumber=1&uploadId={initResult.UploadId}");
        copyRequest.Headers.Add("x-amz-copy-source", $"/{sourceBucket}/source-object.txt");
        var copyResponse = await Client.SendAsync(copyRequest);

        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);
        Assert.True(copyResponse.Headers.Contains("ETag"));
    }

    [Fact]
    public async Task UploadPartCopy_NonExistentSource_ReturnsNoSuchKey()
    {
        var bucketName = await CreateTestBucketAsync();

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/dest.txt?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Try to copy from non-existent source
        var copyRequest = new HttpRequestMessage(HttpMethod.Put,
            $"/{bucketName}/dest.txt?partNumber=1&uploadId={initResult.UploadId}");
        copyRequest.Headers.Add("x-amz-copy-source", $"/{bucketName}/nonexistent-source.txt");
        var copyResponse = await Client.SendAsync(copyRequest);

        Assert.Equal(HttpStatusCode.NotFound, copyResponse.StatusCode);
        var errorXml = await copyResponse.Content.ReadAsStringAsync();
        Assert.Contains("NoSuchKey", errorXml);
    }

    [Fact]
    public async Task UploadPartCopy_InvalidByteRange_ReturnsInvalidRange()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create small source object (10 bytes)
        var sourceContent = "0123456789";
        await Client.PutAsync($"/{bucketName}/source.txt",
            new StringContent(sourceContent, Encoding.UTF8));

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/dest.txt?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Try to copy with invalid byte range (beyond object size)
        var copyRequest = new HttpRequestMessage(HttpMethod.Put,
            $"/{bucketName}/dest.txt?partNumber=1&uploadId={initResult.UploadId}");
        copyRequest.Headers.Add("x-amz-copy-source", $"/{bucketName}/source.txt");
        copyRequest.Headers.Add("x-amz-copy-source-range", "bytes=5-100"); // End is beyond object size
        var copyResponse = await Client.SendAsync(copyRequest);

        Assert.Equal(HttpStatusCode.RequestedRangeNotSatisfiable, copyResponse.StatusCode);
        var errorXml = await copyResponse.Content.ReadAsStringAsync();
        Assert.Contains("InvalidRange", errorXml);
    }

    [Fact]
    public async Task UploadPartCopy_MixedWithRegularUpload_Succeeds()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create source object
        var sourceContent = "Copied content from source";
        await Client.PutAsync($"/{bucketName}/source.txt",
            new StringContent(sourceContent, Encoding.UTF8));

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/mixed-upload.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Part 1: Regular upload
        var part1Response = await Client.PutAsync(
            $"/{bucketName}/mixed-upload.bin?partNumber=1&uploadId={initResult.UploadId}",
            new StringContent("Regular upload part", Encoding.UTF8));
        Assert.Equal(HttpStatusCode.OK, part1Response.StatusCode);
        var part1ETag = part1Response.Headers.GetValues("ETag").First().Trim('\"');

        // Part 2: UploadPartCopy
        var copy2Request = new HttpRequestMessage(HttpMethod.Put,
            $"/{bucketName}/mixed-upload.bin?partNumber=2&uploadId={initResult.UploadId}");
        copy2Request.Headers.Add("x-amz-copy-source", $"/{bucketName}/source.txt");
        var copy2Response = await Client.SendAsync(copy2Request);
        Assert.Equal(HttpStatusCode.OK, copy2Response.StatusCode);
        var part2ETag = copy2Response.Headers.GetValues("ETag").First().Trim('\"');

        // Complete multipart upload
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

        var completeResponse = await Client.PostAsync(
            $"/{bucketName}/mixed-upload.bin?uploadId={initResult.UploadId}",
            new StringContent(completeRequestXml, Encoding.UTF8, "application/xml"));

        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);

        // Verify the combined result
        var getResponse = await Client.GetAsync($"/{bucketName}/mixed-upload.bin");
        var resultContent = await getResponse.Content.ReadAsStringAsync();
        Assert.Equal("Regular upload part" + sourceContent, resultContent);
    }

    [Fact]
    public async Task UploadPartCopy_XmlResponse_ETagContainsQuotes()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create source object
        var sourceContent = "Test content for ETag quote validation";
        await Client.PutAsync($"/{bucketName}/source.txt",
            new StringContent(sourceContent, Encoding.UTF8));

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/dest.txt?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Upload part using UploadPartCopy
        var copyRequest = new HttpRequestMessage(HttpMethod.Put,
            $"/{bucketName}/dest.txt?partNumber=1&uploadId={initResult.UploadId}");
        copyRequest.Headers.Add("x-amz-copy-source", $"/{bucketName}/source.txt");
        var copyResponse = await Client.SendAsync(copyRequest);

        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);

        // Get XML response
        var copyXml = await copyResponse.Content.ReadAsStringAsync();

        // Deserialize to verify structure
        var copySerializer = new XmlSerializer(typeof(CopyPartResult));
        using var copyReader = new StringReader(copyXml);
        var copyResult = (CopyPartResult?)copySerializer.Deserialize(copyReader);
        Assert.NotNull(copyResult);

        // Verify ETag contains quotes in the XML (S3 specification requirement)
        Assert.StartsWith("\"", copyResult.ETag);
        Assert.EndsWith("\"", copyResult.ETag);

        // Verify the raw XML also contains quoted ETag
        Assert.Matches(@"<ETag>""[a-f0-9]+""</ETag>", copyXml);
    }

    [Fact]
    public async Task UploadPartCopy_LastModifiedFormat_MatchesS3ISO8601Specification()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create source object
        var sourceContent = "Test content for ISO8601 timestamp validation";
        await Client.PutAsync($"/{bucketName}/source.txt",
            new StringContent(sourceContent, Encoding.UTF8));

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/dest.txt?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Upload part using UploadPartCopy
        var copyRequest = new HttpRequestMessage(HttpMethod.Put,
            $"/{bucketName}/dest.txt?partNumber=1&uploadId={initResult.UploadId}");
        copyRequest.Headers.Add("x-amz-copy-source", $"/{bucketName}/source.txt");
        var copyResponse = await Client.SendAsync(copyRequest);

        Assert.Equal(HttpStatusCode.OK, copyResponse.StatusCode);

        // Get XML response
        var copyXml = await copyResponse.Content.ReadAsStringAsync();

        // Deserialize to verify structure
        var copySerializer = new XmlSerializer(typeof(CopyPartResult));
        using var copyReader = new StringReader(copyXml);
        var copyResult = (CopyPartResult?)copySerializer.Deserialize(copyReader);
        Assert.NotNull(copyResult);
        Assert.NotNull(copyResult.LastModified);

        // Verify LastModified follows S3 ISO8601 format: YYYY-MM-DDTHH:mm:ss.fffZ
        // Example from AWS docs: 2011-04-11T20:34:56.000Z
        var iso8601Pattern = @"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$";
        Assert.Matches(iso8601Pattern, copyResult.LastModified);

        // Verify the timestamp can be parsed as valid DateTime
        var parseSuccess = DateTime.TryParseExact(
            copyResult.LastModified,
            "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
            System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal,
            out var parsedDate);
        Assert.True(parseSuccess, $"Failed to parse LastModified: '{copyResult.LastModified}'");

        // Verify the parsed date is reasonable (not default, and within last few seconds)
        Assert.NotEqual(DateTime.MinValue, parsedDate);
        Assert.True(parsedDate <= DateTime.UtcNow, $"Parsed date {parsedDate:O} is after current UTC");
        Assert.True(parsedDate >= DateTime.UtcNow.AddMinutes(-1), $"Parsed date {parsedDate:O} is more than 1 minute old"); // Should be very recent

        // Verify the raw XML contains properly formatted timestamp with 'T' separator, 'Z' suffix, and exactly 3 decimal places for milliseconds
        Assert.Matches(@"<LastModified>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z</LastModified>", copyXml);
    }
}