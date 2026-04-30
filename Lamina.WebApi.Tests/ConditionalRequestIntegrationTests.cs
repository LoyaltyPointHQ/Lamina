using System.Net;
using System.Text;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Lamina.WebApi.Tests.Controllers;

public class ConditionalRequestIntegrationTests : IntegrationTestBase
{
    public ConditionalRequestIntegrationTests(WebApplicationFactory<global::Program> factory) : base(factory)
    {
    }

    private async Task<(string BucketName, string Key, string ETag)> CreateTestObjectAsync(string content = "hello world")
    {
        var bucketName = $"cond-{Guid.NewGuid()}";
        var key = "test.txt";
        await Client.PutAsync($"/{bucketName}", null);

        var putContent = new StringContent(content, Encoding.UTF8, "text/plain");
        var putResponse = await Client.PutAsync($"/{bucketName}/{key}", putContent);
        var etag = putResponse.Headers.ETag?.Tag ?? "";
        return (bucketName, key, etag);
    }

    // GetObject - If-Match

    [Fact]
    public async Task GetObject_IfMatch_Matching_Returns200()
    {
        var (bucket, key, etag) = await CreateTestObjectAsync();
        var request = new HttpRequestMessage(HttpMethod.Get, $"/{bucket}/{key}");
        request.Headers.Add("If-Match", etag);

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task GetObject_IfMatch_NotMatching_Returns412()
    {
        var (bucket, key, _) = await CreateTestObjectAsync();
        var request = new HttpRequestMessage(HttpMethod.Get, $"/{bucket}/{key}");
        request.Headers.Add("If-Match", "\"nonexistentetag\"");

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.PreconditionFailed, response.StatusCode);
    }

    [Fact]
    public async Task GetObject_IfNoneMatch_Matching_Returns304()
    {
        var (bucket, key, etag) = await CreateTestObjectAsync();
        var request = new HttpRequestMessage(HttpMethod.Get, $"/{bucket}/{key}");
        request.Headers.Add("If-None-Match", etag);

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
    }

    [Fact]
    public async Task GetObject_IfNoneMatch_NotMatching_Returns200()
    {
        var (bucket, key, _) = await CreateTestObjectAsync();
        var request = new HttpRequestMessage(HttpMethod.Get, $"/{bucket}/{key}");
        request.Headers.Add("If-None-Match", "\"nonexistentetag\"");

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task GetObject_IfModifiedSince_NotModified_Returns304()
    {
        var (bucket, key, _) = await CreateTestObjectAsync();
        // Future date — object was not modified after this
        var futureDate = DateTime.UtcNow.AddDays(1).ToString("R");
        var request = new HttpRequestMessage(HttpMethod.Get, $"/{bucket}/{key}");
        request.Headers.Add("If-Modified-Since", futureDate);

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
    }

    [Fact]
    public async Task GetObject_IfUnmodifiedSince_Modified_Returns412()
    {
        var (bucket, key, _) = await CreateTestObjectAsync();
        // Past date — object was modified after this
        var pastDate = DateTime.UtcNow.AddDays(-1).ToString("R");
        var request = new HttpRequestMessage(HttpMethod.Get, $"/{bucket}/{key}");
        request.Headers.Add("If-Unmodified-Since", pastDate);

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.PreconditionFailed, response.StatusCode);
    }

    // HeadObject - conditionals

    [Fact]
    public async Task HeadObject_IfMatch_Matching_Returns200()
    {
        var (bucket, key, etag) = await CreateTestObjectAsync();
        var request = new HttpRequestMessage(HttpMethod.Head, $"/{bucket}/{key}");
        request.Headers.Add("If-Match", etag);

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task HeadObject_IfMatch_NotMatching_Returns412()
    {
        var (bucket, key, _) = await CreateTestObjectAsync();
        var request = new HttpRequestMessage(HttpMethod.Head, $"/{bucket}/{key}");
        request.Headers.Add("If-Match", "\"badtag\"");

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.PreconditionFailed, response.StatusCode);
    }

    [Fact]
    public async Task HeadObject_IfNoneMatch_Matching_Returns304()
    {
        var (bucket, key, etag) = await CreateTestObjectAsync();
        var request = new HttpRequestMessage(HttpMethod.Head, $"/{bucket}/{key}");
        request.Headers.Add("If-None-Match", etag);

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
    }

    // CopyObject - copy-source conditionals

    [Fact]
    public async Task CopyObject_CopySourceIfMatch_Matching_Returns200()
    {
        var (srcBucket, srcKey, etag) = await CreateTestObjectAsync();
        var dstBucket = $"dst-{Guid.NewGuid()}";
        await Client.PutAsync($"/{dstBucket}", null);

        var request = new HttpRequestMessage(HttpMethod.Put, $"/{dstBucket}/copy.txt");
        request.Headers.Add("x-amz-copy-source", $"/{srcBucket}/{srcKey}");
        request.Headers.Add("x-amz-copy-source-if-match", etag);

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task CopyObject_CopySourceIfMatch_NotMatching_Returns412()
    {
        var (srcBucket, srcKey, _) = await CreateTestObjectAsync();
        var dstBucket = $"dst-{Guid.NewGuid()}";
        await Client.PutAsync($"/{dstBucket}", null);

        var request = new HttpRequestMessage(HttpMethod.Put, $"/{dstBucket}/copy.txt");
        request.Headers.Add("x-amz-copy-source", $"/{srcBucket}/{srcKey}");
        request.Headers.Add("x-amz-copy-source-if-match", "\"badtag\"");

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.PreconditionFailed, response.StatusCode);
    }

    [Fact]
    public async Task CopyObject_CopySourceIfNoneMatch_Matching_Returns412()
    {
        var (srcBucket, srcKey, etag) = await CreateTestObjectAsync();
        var dstBucket = $"dst-{Guid.NewGuid()}";
        await Client.PutAsync($"/{dstBucket}", null);

        var request = new HttpRequestMessage(HttpMethod.Put, $"/{dstBucket}/copy.txt");
        request.Headers.Add("x-amz-copy-source", $"/{srcBucket}/{srcKey}");
        request.Headers.Add("x-amz-copy-source-if-none-match", etag);

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.PreconditionFailed, response.StatusCode);
    }
}
