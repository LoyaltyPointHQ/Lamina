using System.Net;
using System.Text;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace Lamina.WebApi.Tests.Controllers;

public class EmptyContentTypeIntegrationTests : IntegrationTestBase
{
    public EmptyContentTypeIntegrationTests(WebApplicationFactory<global::Program> factory) : base(factory)
    {
    }

    private async Task<string> CreateTestBucketAsync()
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        return bucketName;
    }

    // Simulates an object stored with empty ContentType (e.g. migrated from older data or
    // client that sent Content-Type: with no value). The correct response is 200 with
    // application/octet-stream, not a 500 FormatException from FileStreamResult.
    private async Task StoreObjectWithEmptyContentTypeAsync(string bucketName, string key)
    {
        // First PUT creates the data file on disk
        var content = new ByteArrayContent("hello"u8.ToArray());
        content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("text/plain");
        await Client.PutAsync($"/{bucketName}/{key}", content);

        // Overwrite the metadata entry with an empty ContentType to simulate the bad state
        var metadataStorage = Factory.Services.GetRequiredService<IObjectMetadataStorage>();
        await metadataStorage.StoreMetadataAsync(
            bucketName, key, "test-etag", 5,
            new PutObjectRequest { Key = key, ContentType = "" });
    }

    [Fact]
    public async Task GetObject_EmptyContentTypeInStorage_Returns200WithDefaultContentType()
    {
        var bucketName = await CreateTestBucketAsync();
        await StoreObjectWithEmptyContentTypeAsync(bucketName, "test.bin");

        var response = await Client.GetAsync($"/{bucketName}/test.bin");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/octet-stream", response.Content.Headers.ContentType?.MediaType);
    }

    [Fact]
    public async Task HeadObject_EmptyContentTypeInStorage_ReturnsDefaultContentType()
    {
        var bucketName = await CreateTestBucketAsync();
        await StoreObjectWithEmptyContentTypeAsync(bucketName, "test.bin");

        var request = new HttpRequestMessage(HttpMethod.Head, $"/{bucketName}/test.bin");
        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/octet-stream", response.Content.Headers.ContentType?.MediaType);
    }
}
