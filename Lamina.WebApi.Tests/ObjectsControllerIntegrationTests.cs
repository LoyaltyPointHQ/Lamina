using System.Net;
using System.Text;
using System.Xml.Serialization;
using Lamina.Core.Models;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Lamina.WebApi.Tests.Controllers;

public class ObjectsControllerIntegrationTests : IntegrationTestBase
{
    public ObjectsControllerIntegrationTests(WebApplicationFactory<global::Program> factory) : base(factory)
    {
    }

    private async Task<string> CreateTestBucketAsync()
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        return bucketName;
    }

    [Fact]
    public async Task PutObject_ValidRequest_Returns200()
    {
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent("Test content", Encoding.UTF8, "text/plain");

        var response = await Client.PutAsync($"/{bucketName}/test.txt", content);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("ETag", response.Headers.Select(h => h.Key));
        Assert.Contains("x-amz-version-id", response.Headers.Select(h => h.Key));
    }

    [Fact]
    public async Task PutObject_NonExistingBucket_Returns404()
    {
        var content = new StringContent("Test content", Encoding.UTF8, "text/plain");

        var response = await Client.PutAsync($"/non-existing-{Guid.NewGuid()}/test.txt", content);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var errorXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("NoSuchBucket", errorXml);
    }

    [Fact]
    public async Task GetObject_ExistingObject_Returns200()
    {
        var bucketName = await CreateTestBucketAsync();
        var contentText = "Get test content";
        var content = new StringContent(contentText, Encoding.UTF8, "text/plain");
        await Client.PutAsync($"/{bucketName}/get-test.txt", content);

        var response = await Client.GetAsync($"/{bucketName}/get-test.txt");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var responseContent = await response.Content.ReadAsStringAsync();
        Assert.Equal(contentText, responseContent);

        var allHeaders = response.Headers.Concat(response.Content.Headers).Select(h => h.Key);
        Assert.Contains("ETag", allHeaders);
        Assert.Contains("Last-Modified", allHeaders);
        Assert.Contains("Content-Length", allHeaders);
    }

    [Fact]
    public async Task GetObject_NonExistingObject_Returns404()
    {
        var bucketName = await CreateTestBucketAsync();

        var response = await Client.GetAsync($"/{bucketName}/non-existing.txt");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var errorXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("NoSuchKey", errorXml);
    }

    [Fact]
    public async Task DeleteObject_ExistingObject_Returns204()
    {
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent("Delete test", Encoding.UTF8, "text/plain");
        await Client.PutAsync($"/{bucketName}/delete.txt", content);

        var response = await Client.DeleteAsync($"/{bucketName}/delete.txt");

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);

        var getResponse = await Client.GetAsync($"/{bucketName}/delete.txt");
        Assert.Equal(HttpStatusCode.NotFound, getResponse.StatusCode);
    }

    [Fact]
    public async Task DeleteObject_NonExistingObject_Returns204()
    {
        var bucketName = await CreateTestBucketAsync();

        var response = await Client.DeleteAsync($"/{bucketName}/non-existing.txt");

        // S3 returns 204 even for non-existing objects
        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    [Fact]
    public async Task ListObjects_ReturnsObjects()
    {
        var bucketName = await CreateTestBucketAsync();
        await Client.PutAsync($"/{bucketName}/file1.txt",
            new StringContent("Content 1", Encoding.UTF8, "text/plain"));
        await Client.PutAsync($"/{bucketName}/file2.txt",
            new StringContent("Content 2", Encoding.UTF8, "text/plain"));

        var response = await Client.GetAsync($"/{bucketName}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var xmlContent = await response.Content.ReadAsStringAsync();
        Assert.Contains("ListBucketResult", xmlContent);
        Assert.Contains("file1.txt", xmlContent);
        Assert.Contains("file2.txt", xmlContent);

        var serializer = new XmlSerializer(typeof(ListBucketResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListBucketResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);
        Assert.Equal(bucketName, result.Name);
        Assert.Equal(2, result.ContentsList.Count);
        Assert.Contains(result.ContentsList, o => o.Key == "file1.txt");
        Assert.Contains(result.ContentsList, o => o.Key == "file2.txt");
    }

    [Fact]
    public async Task ListObjects_WithPrefix_FiltersResults()
    {
        var bucketName = await CreateTestBucketAsync();
        await Client.PutAsync($"/{bucketName}/doc1.txt",
            new StringContent("Doc 1", Encoding.UTF8, "text/plain"));
        await Client.PutAsync($"/{bucketName}/doc2.txt",
            new StringContent("Doc 2", Encoding.UTF8, "text/plain"));
        await Client.PutAsync($"/{bucketName}/image.png",
            new StringContent("Image", Encoding.UTF8, "image/png"));

        var response = await Client.GetAsync($"/{bucketName}?prefix=doc");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var xmlContent = await response.Content.ReadAsStringAsync();
        Assert.Contains("ListBucketResult", xmlContent);
        Assert.Contains("doc1.txt", xmlContent);
        Assert.Contains("doc2.txt", xmlContent);
        Assert.DoesNotContain("image.png", xmlContent);

        var serializer = new XmlSerializer(typeof(ListBucketResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListBucketResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);
        Assert.Equal(2, result.ContentsList.Count);
        Assert.All(result.ContentsList, o => Assert.StartsWith("doc", o.Key));
    }

    [Fact]
    public async Task ListObjects_WithDelimiter_ReturnsHierarchicalListing()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create hierarchical structure
        await Client.PutAsync($"/{bucketName}/photos/2021/jan/1.jpg",
            new StringContent("Image 1", Encoding.UTF8, "image/jpeg"));
        await Client.PutAsync($"/{bucketName}/photos/2021/feb/2.jpg",
            new StringContent("Image 2", Encoding.UTF8, "image/jpeg"));
        await Client.PutAsync($"/{bucketName}/photos/2022/mar/3.jpg",
            new StringContent("Image 3", Encoding.UTF8, "image/jpeg"));
        await Client.PutAsync($"/{bucketName}/photos/readme.txt",
            new StringContent("Photos readme", Encoding.UTF8, "text/plain"));
        await Client.PutAsync($"/{bucketName}/docs/manual.pdf",
            new StringContent("Manual", Encoding.UTF8, "application/pdf"));

        // Test with prefix and delimiter
        var response = await Client.GetAsync($"/{bucketName}?prefix=photos/&delimiter=/");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var xmlContent = await response.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(ListBucketResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListBucketResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);

        // Should contain only direct files under photos/
        Assert.Single(result.ContentsList);
        Assert.Equal("photos/readme.txt", result.ContentsList[0].Key);

        // Should contain common prefixes for subdirectories
        Assert.Equal(2, result.CommonPrefixesList.Count);
        Assert.Contains(result.CommonPrefixesList, cp => cp.Prefix == "photos/2021/");
        Assert.Contains(result.CommonPrefixesList, cp => cp.Prefix == "photos/2022/");
    }

    [Fact]
    public async Task ListObjects_WithDelimiterNoPrefix_ReturnsTopLevelOnly()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create hierarchical structure
        await Client.PutAsync($"/{bucketName}/photos/2021/image.jpg",
            new StringContent("Image", Encoding.UTF8, "image/jpeg"));
        await Client.PutAsync($"/{bucketName}/docs/readme.txt",
            new StringContent("Readme", Encoding.UTF8, "text/plain"));
        await Client.PutAsync($"/{bucketName}/root-file.txt",
            new StringContent("Root file", Encoding.UTF8, "text/plain"));

        // Test with delimiter but no prefix
        var response = await Client.GetAsync($"/{bucketName}?delimiter=/");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var xmlContent = await response.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(ListBucketResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListBucketResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);

        // Should contain only root-level files
        Assert.Single(result.ContentsList);
        Assert.Equal("root-file.txt", result.ContentsList[0].Key);

        // Should contain common prefixes for top-level directories
        Assert.Equal(2, result.CommonPrefixesList.Count);
        Assert.Contains(result.CommonPrefixesList, cp => cp.Prefix == "photos/");
        Assert.Contains(result.CommonPrefixesList, cp => cp.Prefix == "docs/");
    }

    [Fact]
    public async Task ListObjects_WithoutDelimiter_ReturnsAllObjectsRecursively()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create hierarchical structure
        await Client.PutAsync($"/{bucketName}/photos/2021/image.jpg",
            new StringContent("Image", Encoding.UTF8, "image/jpeg"));
        await Client.PutAsync($"/{bucketName}/docs/readme.txt",
            new StringContent("Readme", Encoding.UTF8, "text/plain"));
        await Client.PutAsync($"/{bucketName}/root-file.txt",
            new StringContent("Root file", Encoding.UTF8, "text/plain"));

        // Test without delimiter (should return all objects)
        var response = await Client.GetAsync($"/{bucketName}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var xmlContent = await response.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(ListBucketResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListBucketResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);

        // Should contain all objects
        Assert.Equal(3, result.ContentsList.Count);
        Assert.Contains(result.ContentsList, o => o.Key == "photos/2021/image.jpg");
        Assert.Contains(result.ContentsList, o => o.Key == "docs/readme.txt");
        Assert.Contains(result.ContentsList, o => o.Key == "root-file.txt");

        // Should not contain any common prefixes
        Assert.Empty(result.CommonPrefixesList);
    }

    [Fact]
    public async Task ListObjects_WithPrefixAndDelimiter_ReturnsNestedLevelCorrectly()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create deep hierarchical structure
        await Client.PutAsync($"/{bucketName}/photos/2021/jan/vacation/beach1.jpg",
            new StringContent("Beach 1", Encoding.UTF8, "image/jpeg"));
        await Client.PutAsync($"/{bucketName}/photos/2021/jan/vacation/beach2.jpg",
            new StringContent("Beach 2", Encoding.UTF8, "image/jpeg"));
        await Client.PutAsync($"/{bucketName}/photos/2021/jan/work/meeting.jpg",
            new StringContent("Meeting", Encoding.UTF8, "image/jpeg"));
        await Client.PutAsync($"/{bucketName}/photos/2021/jan/selfie.jpg",
            new StringContent("Selfie", Encoding.UTF8, "image/jpeg"));

        // Test with specific prefix and delimiter
        var response = await Client.GetAsync($"/{bucketName}?prefix=photos/2021/jan/&delimiter=/");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var xmlContent = await response.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(ListBucketResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListBucketResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);

        // Should contain direct files in jan/ directory
        Assert.Single(result.ContentsList);
        Assert.Equal("photos/2021/jan/selfie.jpg", result.ContentsList[0].Key);

        // Should contain common prefixes for subdirectories
        Assert.Equal(2, result.CommonPrefixesList.Count);
        Assert.Contains(result.CommonPrefixesList, cp => cp.Prefix == "photos/2021/jan/vacation/");
        Assert.Contains(result.CommonPrefixesList, cp => cp.Prefix == "photos/2021/jan/work/");
    }

    [Fact]
    public async Task ListObjects_EmptyDelimiter_BehavesLikeNoDelimiter()
    {
        var bucketName = await CreateTestBucketAsync();

        await Client.PutAsync($"/{bucketName}/photos/image.jpg",
            new StringContent("Image", Encoding.UTF8, "image/jpeg"));
        await Client.PutAsync($"/{bucketName}/docs/readme.txt",
            new StringContent("Readme", Encoding.UTF8, "text/plain"));

        // Test with empty delimiter
        var response = await Client.GetAsync($"/{bucketName}?delimiter=");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var xmlContent = await response.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(ListBucketResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListBucketResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);

        // Should return all objects (same as no delimiter)
        Assert.Equal(2, result.ContentsList.Count);
        Assert.Empty(result.CommonPrefixesList);
    }

    [Fact]
    public async Task HeadObject_ExistingObject_Returns200()
    {
        var bucketName = await CreateTestBucketAsync();
        await Client.PutAsync($"/{bucketName}/head.txt",
            new StringContent("Head test", Encoding.UTF8, "text/plain"));

        var request = new HttpRequestMessage(HttpMethod.Head, $"/{bucketName}/head.txt");
        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var allHeaders = response.Headers.Concat(response.Content.Headers).Select(h => h.Key);
        Assert.Contains("ETag", allHeaders);
        Assert.Contains("Content-Length", allHeaders);
        Assert.Contains("Last-Modified", allHeaders);
    }

    [Fact]
    public async Task HeadObject_NonExistingObject_Returns404()
    {
        var bucketName = await CreateTestBucketAsync();

        var request = new HttpRequestMessage(HttpMethod.Head, $"/{bucketName}/non-existing.txt");
        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task HeadObject_NonExistingObject_ReturnsS3CompliantResponse()
    {
        var bucketName = await CreateTestBucketAsync();

        var request = new HttpRequestMessage(HttpMethod.Head, $"/{bucketName}/non-existing.txt");
        var response = await Client.SendAsync(request);

        // Should return 404
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);

        // Should have XML content type for S3 compatibility
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        // HEAD requests should not have a body
        var content = await response.Content.ReadAsStringAsync();
        Assert.Empty(content);
    }

    [Fact]
    public async Task InitiateMultipartUpload_ValidRequest_Returns200()
    {
        var bucketName = await CreateTestBucketAsync();

        var response = await Client.PostAsync($"/{bucketName}/multipart.bin?uploads", null);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var xmlContent = await response.Content.ReadAsStringAsync();
        Assert.Contains("InitiateMultipartUploadResult", xmlContent);
        Assert.Contains(bucketName, xmlContent);
        Assert.Contains("multipart.bin", xmlContent);

        var serializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var reader = new StringReader(xmlContent);
        var result = (InitiateMultipartUploadResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);
        Assert.NotEmpty(result.UploadId);
        Assert.Equal(bucketName, result.Bucket);
        Assert.Equal("multipart.bin", result.Key);
    }

    [Fact]
    public async Task CompleteMultipartUpload_ValidParts_CreatesObject()
    {
        var bucketName = await CreateTestBucketAsync();

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/complete.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Upload parts
        var part1Response = await Client.PutAsync(
            $"/{bucketName}/complete.bin?partNumber=1&uploadId={initResult.UploadId}",
            new StringContent("Part 1 ", Encoding.UTF8));
        Assert.Equal(HttpStatusCode.OK, part1Response.StatusCode);
        var part1ETag = part1Response.Headers.GetValues("ETag").First().Trim('"');

        var part2Response = await Client.PutAsync(
            $"/{bucketName}/complete.bin?partNumber=2&uploadId={initResult.UploadId}",
            new StringContent("Part 2", Encoding.UTF8));
        Assert.Equal(HttpStatusCode.OK, part2Response.StatusCode);
        var part2ETag = part2Response.Headers.GetValues("ETag").First().Trim('"');

        // Create complete request XML (ETags should not include quotes in XML)
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
            $"/{bucketName}/complete.bin?uploadId={initResult.UploadId}",
            new StringContent(completeRequestXml, Encoding.UTF8, "application/xml"));

        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);
        Assert.Equal("application/xml", completeResponse.Content.Headers.ContentType?.MediaType);

        var completeXml = await completeResponse.Content.ReadAsStringAsync();
        Assert.Contains("CompleteMultipartUploadResult", completeXml);
        Assert.Contains("complete.bin", completeXml);

        var getResponse = await Client.GetAsync($"/{bucketName}/complete.bin");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        var content = await getResponse.Content.ReadAsStringAsync();
        var contentLength = getResponse.Content.Headers.ContentLength;
        Assert.True(contentLength > 0, $"Content length should be greater than 0, but was {contentLength}");
        Assert.Equal("Part 1 Part 2", content);
    }

    [Fact]
    public async Task AbortMultipartUpload_ExistingUpload_Returns204()
    {
        var bucketName = await CreateTestBucketAsync();

        var initResponse = await Client.PostAsync($"/{bucketName}/abort.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var reader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)serializer.Deserialize(reader);
        Assert.NotNull(initResult);

        var response = await Client.DeleteAsync(
            $"/{bucketName}/abort.bin?uploadId={initResult.UploadId}");

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    [Fact]
    public async Task ListParts_ReturnsUploadedParts()
    {
        var bucketName = await CreateTestBucketAsync();

        var initResponse = await Client.PostAsync($"/{bucketName}/parts.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var reader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)serializer.Deserialize(reader);
        Assert.NotNull(initResult);

        await Client.PutAsync(
            $"/{bucketName}/parts.bin?partNumber=1&uploadId={initResult.UploadId}",
            new StringContent("Part 1", Encoding.UTF8));
        await Client.PutAsync(
            $"/{bucketName}/parts.bin?partNumber=2&uploadId={initResult.UploadId}",
            new StringContent("Part 2", Encoding.UTF8));

        var response = await Client.GetAsync(
            $"/{bucketName}/parts.bin?uploadId={initResult.UploadId}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var xmlContent = await response.Content.ReadAsStringAsync();
        Assert.Contains("ListPartsResult", xmlContent);
        Assert.Contains("parts.bin", xmlContent);

        var partsSerializer = new XmlSerializer(typeof(ListPartsResult));
        using var partsReader = new StringReader(xmlContent);
        var partsResult = (ListPartsResult?)partsSerializer.Deserialize(partsReader);

        Assert.NotNull(partsResult);
        Assert.Equal("parts.bin", partsResult.Key);
        Assert.Equal(2, partsResult.Parts.Count);
    }

    [Fact]
    public async Task ListMultipartUploads_ReturnsActiveUploads()
    {
        var bucketName = await CreateTestBucketAsync();

        await Client.PostAsync($"/{bucketName}/upload1.bin?uploads", null);
        await Client.PostAsync($"/{bucketName}/upload2.bin?uploads", null);

        var response = await Client.GetAsync($"/{bucketName}?uploads");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var xmlContent = await response.Content.ReadAsStringAsync();
        Assert.Contains("ListMultipartUploadsResult", xmlContent);
        Assert.Contains("upload1.bin", xmlContent);
        Assert.Contains("upload2.bin", xmlContent);

        var serializer = new XmlSerializer(typeof(ListMultipartUploadsResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListMultipartUploadsResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);
        Assert.Equal(bucketName, result.Bucket);
        Assert.True(result.Uploads.Count >= 2);
        Assert.Contains(result.Uploads, u => u.Key == "upload1.bin");
        Assert.Contains(result.Uploads, u => u.Key == "upload2.bin");
    }

    [Fact]
    public async Task ListObjects_WithMaxKeys_LimitsResults()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create 5 objects
        for (int i = 1; i <= 5; i++)
        {
            await Client.PutAsync($"/{bucketName}/file{i:D2}.txt",
                new StringContent($"Content {i}", Encoding.UTF8, "text/plain"));
        }

        // Request only 3 objects
        var response = await Client.GetAsync($"/{bucketName}?max-keys=3");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var xmlContent = await response.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(ListBucketResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListBucketResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);
        Assert.Equal(3, result.MaxKeys);
        Assert.Equal(3, result.ContentsList.Count);
        Assert.True(result.IsTruncated);
        Assert.NotNull(result.NextMarker);
        Assert.Equal("file04.txt", result.NextMarker);

        // Check keys are in lexicographic order
        Assert.Equal("file01.txt", result.ContentsList[0].Key);
        Assert.Equal("file02.txt", result.ContentsList[1].Key);
        Assert.Equal("file03.txt", result.ContentsList[2].Key);
    }

    [Fact]
    public async Task ListObjects_WithMarker_ContinuesFromSpecificKey()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create 5 objects
        for (int i = 1; i <= 5; i++)
        {
            await Client.PutAsync($"/{bucketName}/file{i:D2}.txt",
                new StringContent($"Content {i}", Encoding.UTF8, "text/plain"));
        }

        // Request objects starting after file02.txt
        var response = await Client.GetAsync($"/{bucketName}?marker=file03.txt");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var xmlContent = await response.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(ListBucketResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListBucketResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);
        Assert.Equal(3, result.ContentsList.Count);
        Assert.False(result.IsTruncated);
        Assert.Null(result.NextMarker);

        // Check we got the remaining objects
        Assert.Equal("file03.txt", result.ContentsList[0].Key);
        Assert.Equal("file04.txt", result.ContentsList[1].Key);
        Assert.Equal("file05.txt", result.ContentsList[2].Key);
    }

    [Fact]
    public async Task ListObjects_WithMaxKeysAndMarker_CombinesProperly()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create 6 objects
        for (int i = 1; i <= 6; i++)
        {
            await Client.PutAsync($"/{bucketName}/file{i:D2}.txt",
                new StringContent($"Content {i}", Encoding.UTF8, "text/plain"));
        }

        // First request: get first 2 objects
        var response1 = await Client.GetAsync($"/{bucketName}?max-keys=2");
        var xmlContent1 = await response1.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(ListBucketResult));
        using var reader1 = new StringReader(xmlContent1);
        var result1 = (ListBucketResult?)serializer.Deserialize(reader1);

        Assert.NotNull(result1);
        Assert.Equal(2, result1.ContentsList.Count);
        Assert.True(result1.IsTruncated);
        Assert.Equal("file03.txt", result1.NextMarker);

        // Second request: continue from where we left off
        var response2 = await Client.GetAsync($"/{bucketName}?max-keys=2&marker={result1.NextMarker}");
        var xmlContent2 = await response2.Content.ReadAsStringAsync();
        using var reader2 = new StringReader(xmlContent2);
        var result2 = (ListBucketResult?)serializer.Deserialize(reader2);

        Assert.NotNull(result2);
        Assert.Equal(2, result2.ContentsList.Count);
        Assert.True(result2.IsTruncated);
        Assert.Equal("file05.txt", result2.NextMarker);
        Assert.Equal("file03.txt", result2.ContentsList[0].Key);
        Assert.Equal("file04.txt", result2.ContentsList[1].Key);

        // Third request: get remaining objects
        var response3 = await Client.GetAsync($"/{bucketName}?marker={result2.NextMarker}");
        var xmlContent3 = await response3.Content.ReadAsStringAsync();
        using var reader3 = new StringReader(xmlContent3);
        var result3 = (ListBucketResult?)serializer.Deserialize(reader3);

        Assert.NotNull(result3);
        Assert.Equal(2, result3.ContentsList.Count);
        Assert.False(result3.IsTruncated);
        Assert.Null(result3.NextMarker);
        Assert.Equal("file05.txt", result3.ContentsList[0].Key);
        Assert.Equal("file06.txt", result3.ContentsList[1].Key);
    }

    [Fact]
    public async Task ListObjects_WithDelimiterAndPagination_HandlesBothKeysAndCommonPrefixes()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create objects and "directories"
        await Client.PutAsync($"/{bucketName}/a-file.txt", new StringContent("Content", Encoding.UTF8));
        await Client.PutAsync($"/{bucketName}/b-file.txt", new StringContent("Content", Encoding.UTF8));
        await Client.PutAsync($"/{bucketName}/dir1/file.txt", new StringContent("Content", Encoding.UTF8));
        await Client.PutAsync($"/{bucketName}/dir2/file.txt", new StringContent("Content", Encoding.UTF8));
        await Client.PutAsync($"/{bucketName}/z-file.txt", new StringContent("Content", Encoding.UTF8));

        // Request with max-keys=3 and delimiter
        var response = await Client.GetAsync($"/{bucketName}?delimiter=/&max-keys=3");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var xmlContent = await response.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(ListBucketResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListBucketResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);
        Assert.Equal(3, result.MaxKeys);
        Assert.True(result.IsTruncated);
        Assert.NotNull(result.NextMarker);

        // Should have a mix of files and common prefixes, limited to 3 total
        var totalItems = result.ContentsList.Count + result.CommonPrefixesList.Count;
        Assert.Equal(3, totalItems);
    }

    [Fact]
    public async Task ListObjects_ExactlyMaxKeys_DoesNotSetTruncated()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create exactly 3 objects
        for (int i = 1; i <= 3; i++)
        {
            await Client.PutAsync($"/{bucketName}/file{i}.txt",
                new StringContent($"Content {i}", Encoding.UTF8, "text/plain"));
        }

        // Request exactly 3 objects
        var response = await Client.GetAsync($"/{bucketName}?max-keys=3");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var xmlContent = await response.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(ListBucketResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListBucketResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);
        Assert.Equal(3, result.ContentsList.Count);
        Assert.False(result.IsTruncated); // Should NOT be truncated
        Assert.Null(result.NextMarker); // Should NOT have NextMarker
    }

    [Fact]
    public async Task ListObjects_WithContinuationToken_WorksLikeMarker()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create 4 objects
        for (int i = 1; i <= 4; i++)
        {
            await Client.PutAsync($"/{bucketName}/item{i:D2}.txt",
                new StringContent($"Content {i}", Encoding.UTF8, "text/plain"));
        }

        // Test continuation-token parameter (same behavior as marker)
        var response = await Client.GetAsync($"/{bucketName}?continuation-token=item03.txt");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var xmlContent = await response.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(ListBucketResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListBucketResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);
        Assert.Equal(2, result.ContentsList.Count);
        Assert.False(result.IsTruncated);

        // Should get items after item02.txt
        Assert.Equal("item03.txt", result.ContentsList[0].Key);
        Assert.Equal("item04.txt", result.ContentsList[1].Key);
    }

    [Fact]
    public async Task CopyObject_WithinSameBucket_Returns200()
    {
        var bucketName = await CreateTestBucketAsync();
        var sourceContent = "Source object content";

        // Create source object
        await Client.PutAsync($"/{bucketName}/source.txt",
            new StringContent(sourceContent, Encoding.UTF8, "text/plain"));

        // Copy to destination
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/destination.txt");
        request.Headers.Add("x-amz-copy-source", $"/{bucketName}/source.txt");

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        // Verify CopyObjectResult XML
        var xmlContent = await response.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(CopyObjectResult));
        using var reader = new StringReader(xmlContent);
        var result = (CopyObjectResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);
        Assert.NotNull(result.ETag);
        Assert.NotNull(result.LastModified);

        // Verify destination object exists and has same content
        var getResponse = await Client.GetAsync($"/{bucketName}/destination.txt");
        var destContent = await getResponse.Content.ReadAsStringAsync();
        Assert.Equal(sourceContent, destContent);
    }

    [Fact]
    public async Task CopyObject_ToDifferentBucket_Returns200()
    {
        var sourceBucket = await CreateTestBucketAsync();
        var destBucket = await CreateTestBucketAsync();
        var sourceContent = "Source object content";

        // Create source object
        await Client.PutAsync($"/{sourceBucket}/source.txt",
            new StringContent(sourceContent, Encoding.UTF8, "text/plain"));

        // Copy to different bucket
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{destBucket}/destination.txt");
        request.Headers.Add("x-amz-copy-source", $"/{sourceBucket}/source.txt");

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        // Verify destination object exists in different bucket
        var getResponse = await Client.GetAsync($"/{destBucket}/destination.txt");
        var destContent = await getResponse.Content.ReadAsStringAsync();
        Assert.Equal(sourceContent, destContent);
    }

    [Fact]
    public async Task CopyObject_NonExistentSource_Returns404()
    {
        var bucketName = await CreateTestBucketAsync();

        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/destination.txt");
        request.Headers.Add("x-amz-copy-source", $"/{bucketName}/non-existent.txt");

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);

        var errorXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("NoSuchKey", errorXml);
    }

    [Fact]
    public async Task CopyObject_WithMetadataDirectiveCopy_CopiesMetadata()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create source object with custom metadata
        var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/source.txt");
        putRequest.Content = new StringContent("Content", Encoding.UTF8, "text/plain");
        putRequest.Headers.Add("x-amz-meta-custom", "custom-value");
        await Client.SendAsync(putRequest);

        // Copy with metadata directive COPY
        var copyRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/destination.txt");
        copyRequest.Headers.Add("x-amz-copy-source", $"/{bucketName}/source.txt");
        copyRequest.Headers.Add("x-amz-metadata-directive", "COPY");

        var response = await Client.SendAsync(copyRequest);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        // Verify destination has the same metadata
        var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/{bucketName}/destination.txt");
        var headResponse = await Client.SendAsync(headRequest);
        Assert.Contains("x-amz-meta-custom", headResponse.Headers.Select(h => h.Key));
    }

    [Fact]
    public async Task CopyObject_WithMetadataDirectiveReplace_ReplacesMetadata()
    {
        var bucketName = await CreateTestBucketAsync();

        // Create source object with custom metadata
        var putRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/source.txt");
        putRequest.Content = new StringContent("Content", Encoding.UTF8, "text/plain");
        putRequest.Headers.Add("x-amz-meta-original", "original-value");
        await Client.SendAsync(putRequest);

        // Copy with metadata directive REPLACE and new metadata
        var copyRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}/destination.txt");
        copyRequest.Content = new StringContent("", Encoding.UTF8, "application/octet-stream");
        copyRequest.Headers.Add("x-amz-copy-source", $"/{bucketName}/source.txt");
        copyRequest.Headers.Add("x-amz-metadata-directive", "REPLACE");
        copyRequest.Headers.Add("x-amz-meta-new", "new-value");

        var response = await Client.SendAsync(copyRequest);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        // Verify destination has new metadata (not original)
        var headRequest = new HttpRequestMessage(HttpMethod.Head, $"/{bucketName}/destination.txt");
        var headResponse = await Client.SendAsync(headRequest);
        Assert.Contains("x-amz-meta-new", headResponse.Headers.Select(h => h.Key));
        Assert.DoesNotContain("x-amz-meta-original", headResponse.Headers.Select(h => h.Key));
    }
}