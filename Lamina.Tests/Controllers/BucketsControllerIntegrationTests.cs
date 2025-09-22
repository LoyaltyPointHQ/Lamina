using System.Net;
using System.Text;
using System.Xml.Serialization;
using Microsoft.AspNetCore.Mvc.Testing;
using Lamina.Models;

namespace Lamina.Tests.Controllers;

public class BucketsControllerIntegrationTests : IntegrationTestBase
{
    public BucketsControllerIntegrationTests(WebApplicationFactory<Program> factory) : base(factory)
    {
    }

    [Fact]
    public async Task CreateBucket_ValidRequest_Returns200()
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";

        var response = await Client.PutAsync($"/{bucketName}", null);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("Location", response.Headers.Select(h => h.Key));
    }

    [Fact]
    public async Task CreateBucket_DuplicateName_Returns409()
    {
        var bucketName = $"duplicate-{Guid.NewGuid()}";

        var response1 = await Client.PutAsync($"/{bucketName}", null);
        var response2 = await Client.PutAsync($"/{bucketName}", null);

        Assert.Equal(HttpStatusCode.OK, response1.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, response2.StatusCode);

        var errorXml = await response2.Content.ReadAsStringAsync();
        Assert.Contains("BucketAlreadyExists", errorXml);
        Assert.Equal("application/xml", response2.Content.Headers.ContentType?.MediaType);
    }

    [Fact]
    public async Task ListObjects_ExistingBucket_Returns200()
    {
        var bucketName = $"get-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);

        var response = await Client.GetAsync($"/{bucketName}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var xmlContent = await response.Content.ReadAsStringAsync();
        Assert.Contains("ListBucketResult", xmlContent);
        Assert.Contains(bucketName, xmlContent);
    }

    [Fact]
    public async Task ListObjects_NonExistingBucket_Returns404()
    {
        var response = await Client.GetAsync($"/non-existing-{Guid.NewGuid()}");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var errorXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("NoSuchBucket", errorXml);
    }

    [Fact]
    public async Task ListBuckets_ReturnsAllBuckets()
    {
        var bucket1 = $"list-test-1-{Guid.NewGuid()}";
        var bucket2 = $"list-test-2-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucket1}", null);
        await Client.PutAsync($"/{bucket2}", null);

        var response = await Client.GetAsync("/");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var xmlContent = await response.Content.ReadAsStringAsync();
        Assert.Contains("ListAllMyBucketsResult", xmlContent);
        Assert.Contains(bucket1, xmlContent);
        Assert.Contains(bucket2, xmlContent);

        var serializer = new XmlSerializer(typeof(ListAllMyBucketsResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListAllMyBucketsResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);
        Assert.NotNull(result.Owner);
        Assert.Contains(result.Buckets, b => b.Name == bucket1);
        Assert.Contains(result.Buckets, b => b.Name == bucket2);
    }

    [Fact]
    public async Task DeleteBucket_ExistingBucket_Returns204()
    {
        var bucketName = $"delete-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);

        var response = await Client.DeleteAsync($"/{bucketName}");

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);

        var getResponse = await Client.GetAsync($"/{bucketName}");
        Assert.Equal(HttpStatusCode.NotFound, getResponse.StatusCode);
    }

    [Fact]
    public async Task DeleteBucket_NonExistingBucket_Returns404()
    {
        var response = await Client.DeleteAsync($"/non-existing-delete-{Guid.NewGuid()}");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var errorXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("NoSuchBucket", errorXml);
    }

    [Fact]
    public async Task DeleteBucket_BucketWithFiles_Returns204()
    {
        var bucketName = $"delete-with-files-{Guid.NewGuid()}";

        // Create bucket
        await Client.PutAsync($"/{bucketName}", null);

        // Add one simple file to the bucket
        var putResponse = await Client.PutAsync($"/{bucketName}/file1.txt", new StringContent("content1"));
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        // Delete bucket (should delete all files)
        var deleteResponse = await Client.DeleteAsync($"/{bucketName}");
        Assert.Equal(HttpStatusCode.NoContent, deleteResponse.StatusCode);

        // Verify bucket is gone
        var bucketResponse = await Client.GetAsync($"/{bucketName}");
        Assert.Equal(HttpStatusCode.NotFound, bucketResponse.StatusCode);
    }

    [Fact]
    public async Task HeadBucket_ExistingBucket_Returns200()
    {
        var bucketName = $"head-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);

        var request = new HttpRequestMessage(HttpMethod.Head, $"/{bucketName}");
        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task HeadBucket_NonExistingBucket_Returns404()
    {
        var request = new HttpRequestMessage(HttpMethod.Head, $"/non-existing-head-{Guid.NewGuid()}");
        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    // Note: Bucket tagging is not part of core S3 XML API spec
    // This test is removed as S3 controllers now follow XML response format

    // Note: Bucket tagging is not part of core S3 XML API spec
    // This test is removed as S3 controllers now follow XML response format
}