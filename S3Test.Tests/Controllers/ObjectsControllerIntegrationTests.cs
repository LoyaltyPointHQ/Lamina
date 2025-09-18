using System.Net;
using System.Text;
using System.Xml.Serialization;
using Microsoft.AspNetCore.Mvc.Testing;
using S3Test.Models;

namespace S3Test.Tests.Controllers;

public class ObjectsControllerIntegrationTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly WebApplicationFactory<Program> _factory;
    private readonly HttpClient _client;

    public ObjectsControllerIntegrationTests(WebApplicationFactory<Program> factory)
    {
        _factory = factory;
        _client = _factory.CreateClient();
    }

    private async Task<string> CreateTestBucketAsync()
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";
        await _client.PutAsync($"/{bucketName}", null);
        return bucketName;
    }

    [Fact]
    public async Task PutObject_ValidRequest_Returns200()
    {
        var bucketName = await CreateTestBucketAsync();
        var content = new StringContent("Test content", Encoding.UTF8, "text/plain");

        var response = await _client.PutAsync($"/{bucketName}/test.txt", content);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("ETag", response.Headers.Select(h => h.Key));
        Assert.Contains("x-amz-version-id", response.Headers.Select(h => h.Key));
    }

    [Fact]
    public async Task PutObject_NonExistingBucket_Returns404()
    {
        var content = new StringContent("Test content", Encoding.UTF8, "text/plain");

        var response = await _client.PutAsync($"/non-existing-{Guid.NewGuid()}/test.txt", content);

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
        await _client.PutAsync($"/{bucketName}/get-test.txt", content);

        var response = await _client.GetAsync($"/{bucketName}/get-test.txt");

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

        var response = await _client.GetAsync($"/{bucketName}/non-existing.txt");

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
        await _client.PutAsync($"/{bucketName}/delete.txt", content);

        var response = await _client.DeleteAsync($"/{bucketName}/delete.txt");

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);

        var getResponse = await _client.GetAsync($"/{bucketName}/delete.txt");
        Assert.Equal(HttpStatusCode.NotFound, getResponse.StatusCode);
    }

    [Fact]
    public async Task DeleteObject_NonExistingObject_Returns204()
    {
        var bucketName = await CreateTestBucketAsync();

        var response = await _client.DeleteAsync($"/{bucketName}/non-existing.txt");

        // S3 returns 204 even for non-existing objects
        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    [Fact]
    public async Task ListObjects_ReturnsObjects()
    {
        var bucketName = await CreateTestBucketAsync();
        await _client.PutAsync($"/{bucketName}/file1.txt",
            new StringContent("Content 1", Encoding.UTF8, "text/plain"));
        await _client.PutAsync($"/{bucketName}/file2.txt",
            new StringContent("Content 2", Encoding.UTF8, "text/plain"));

        var response = await _client.GetAsync($"/{bucketName}");

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
        await _client.PutAsync($"/{bucketName}/doc1.txt",
            new StringContent("Doc 1", Encoding.UTF8, "text/plain"));
        await _client.PutAsync($"/{bucketName}/doc2.txt",
            new StringContent("Doc 2", Encoding.UTF8, "text/plain"));
        await _client.PutAsync($"/{bucketName}/image.png",
            new StringContent("Image", Encoding.UTF8, "image/png"));

        var response = await _client.GetAsync($"/{bucketName}?prefix=doc");

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
    public async Task HeadObject_ExistingObject_Returns200()
    {
        var bucketName = await CreateTestBucketAsync();
        await _client.PutAsync($"/{bucketName}/head.txt",
            new StringContent("Head test", Encoding.UTF8, "text/plain"));

        var request = new HttpRequestMessage(HttpMethod.Head, $"/{bucketName}/head.txt");
        var response = await _client.SendAsync(request);

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
        var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task InitiateMultipartUpload_ValidRequest_Returns200()
    {
        var bucketName = await CreateTestBucketAsync();

        var response = await _client.PostAsync($"/{bucketName}/multipart.bin?uploads", null);

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
        var initResponse = await _client.PostAsync($"/{bucketName}/complete.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        // Upload parts
        var part1Response = await _client.PutAsync(
            $"/{bucketName}/complete.bin?partNumber=1&uploadId={initResult.UploadId}",
            new StringContent("Part 1 ", Encoding.UTF8));
        Assert.Equal(HttpStatusCode.OK, part1Response.StatusCode);
        var part1ETag = part1Response.Headers.GetValues("ETag").First().Trim('"');

        var part2Response = await _client.PutAsync(
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


        var completeResponse = await _client.PostAsync(
            $"/{bucketName}/complete.bin?uploadId={initResult.UploadId}",
            new StringContent(completeRequestXml, Encoding.UTF8, "application/xml"));

        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);
        Assert.Equal("application/xml", completeResponse.Content.Headers.ContentType?.MediaType);

        var completeXml = await completeResponse.Content.ReadAsStringAsync();
        Assert.Contains("CompleteMultipartUploadResult", completeXml);
        Assert.Contains("complete.bin", completeXml);

        var getResponse = await _client.GetAsync($"/{bucketName}/complete.bin");
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

        var initResponse = await _client.PostAsync($"/{bucketName}/abort.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var reader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)serializer.Deserialize(reader);
        Assert.NotNull(initResult);

        var response = await _client.DeleteAsync(
            $"/{bucketName}/abort.bin?uploadId={initResult.UploadId}");

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    [Fact]
    public async Task ListParts_ReturnsUploadedParts()
    {
        var bucketName = await CreateTestBucketAsync();

        var initResponse = await _client.PostAsync($"/{bucketName}/parts.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var reader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)serializer.Deserialize(reader);
        Assert.NotNull(initResult);

        await _client.PutAsync(
            $"/{bucketName}/parts.bin?partNumber=1&uploadId={initResult.UploadId}",
            new StringContent("Part 1", Encoding.UTF8));
        await _client.PutAsync(
            $"/{bucketName}/parts.bin?partNumber=2&uploadId={initResult.UploadId}",
            new StringContent("Part 2", Encoding.UTF8));

        var response = await _client.GetAsync(
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

        await _client.PostAsync($"/{bucketName}/upload1.bin?uploads", null);
        await _client.PostAsync($"/{bucketName}/upload2.bin?uploads", null);

        var response = await _client.GetAsync($"/{bucketName}?uploads");

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
}