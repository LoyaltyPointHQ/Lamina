using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Xml.Serialization;
using Lamina.Core.Models;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Lamina.WebApi.Tests.Controllers;

public class ObjectTaggingHeaderIntegrationTests : IntegrationTestBase
{
    public ObjectTaggingHeaderIntegrationTests(WebApplicationFactory<global::Program> factory) : base(factory)
    {
    }

    private async Task<string> CreateBucketAsync()
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        return bucketName;
    }

    private static TaggingXml DeserializeTagging(string xml)
    {
        var serializer = new XmlSerializer(typeof(TaggingXml));
        using var reader = new StringReader(xml);
        return (TaggingXml)serializer.Deserialize(reader)!;
    }

    [Fact]
    public async Task PutObject_WithTaggingHeader_TagsPersist()
    {
        var bucket = await CreateBucketAsync();
        var content = new StringContent("data", Encoding.UTF8, "text/plain");
        content.Headers.Add("x-amz-tagging", "env=prod&team=core");

        var put = await Client.PutAsync($"/{bucket}/file.txt", content);
        Assert.Equal(HttpStatusCode.OK, put.StatusCode);

        var get = await Client.GetAsync($"/{bucket}/file.txt?tagging");
        Assert.Equal(HttpStatusCode.OK, get.StatusCode);
        var tagging = DeserializeTagging(await get.Content.ReadAsStringAsync());

        Assert.Equal(2, tagging.TagSet.Count);
        Assert.Contains(tagging.TagSet, t => t.Key == "env" && t.Value == "prod");
    }

    [Fact]
    public async Task HeadObject_WithTags_ReturnsTaggingCountHeader()
    {
        var bucket = await CreateBucketAsync();
        var content = new StringContent("data", Encoding.UTF8, "text/plain");
        content.Headers.Add("x-amz-tagging", "a=1&b=2&c=3");
        await Client.PutAsync($"/{bucket}/file.txt", content);

        var head = await Client.SendAsync(new HttpRequestMessage(HttpMethod.Head, $"/{bucket}/file.txt"));

        Assert.Equal(HttpStatusCode.OK, head.StatusCode);
        Assert.True(head.Headers.TryGetValues("x-amz-tagging-count", out var values));
        Assert.Equal("3", values!.First());
    }

    [Fact]
    public async Task GetObject_WithTags_ReturnsTaggingCountHeader()
    {
        var bucket = await CreateBucketAsync();
        var content = new StringContent("data", Encoding.UTF8, "text/plain");
        content.Headers.Add("x-amz-tagging", "only=one");
        await Client.PutAsync($"/{bucket}/file.txt", content);

        var get = await Client.GetAsync($"/{bucket}/file.txt");

        Assert.Equal(HttpStatusCode.OK, get.StatusCode);
        Assert.True(get.Headers.TryGetValues("x-amz-tagging-count", out var values));
        Assert.Equal("1", values!.First());
    }

    [Fact]
    public async Task HeadObject_NoTags_NoTaggingCountHeader()
    {
        var bucket = await CreateBucketAsync();
        await Client.PutAsync($"/{bucket}/file.txt", new StringContent("data", Encoding.UTF8, "text/plain"));

        var head = await Client.SendAsync(new HttpRequestMessage(HttpMethod.Head, $"/{bucket}/file.txt"));

        Assert.False(head.Headers.Contains("x-amz-tagging-count"));
    }

    [Fact]
    public async Task PutObject_TaggingHeaderTooManyTags_Returns400()
    {
        var bucket = await CreateBucketAsync();
        var content = new StringContent("data", Encoding.UTF8, "text/plain");
        var manyTags = string.Join("&", Enumerable.Range(1, 11).Select(i => $"k{i}=v{i}"));
        content.Headers.Add("x-amz-tagging", manyTags);

        var response = await Client.PutAsync($"/{bucket}/file.txt", content);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task CopyObject_DirectiveCopy_CopiesTagsFromSource()
    {
        var bucket = await CreateBucketAsync();

        // Put source with tags
        var source = new StringContent("data", Encoding.UTF8, "text/plain");
        source.Headers.Add("x-amz-tagging", "env=prod");
        await Client.PutAsync($"/{bucket}/source.txt", source);

        // Copy - default is COPY directive
        var copyReq = new HttpRequestMessage(HttpMethod.Put, $"/{bucket}/dest.txt");
        copyReq.Headers.Add("x-amz-copy-source", $"/{bucket}/source.txt");
        var copy = await Client.SendAsync(copyReq);
        Assert.Equal(HttpStatusCode.OK, copy.StatusCode);

        var get = await Client.GetAsync($"/{bucket}/dest.txt?tagging");
        var tagging = DeserializeTagging(await get.Content.ReadAsStringAsync());
        Assert.Single(tagging.TagSet);
        Assert.Equal("prod", tagging.TagSet[0].Value);
    }

    [Fact]
    public async Task CopyObject_DirectiveReplace_UsesNewTags()
    {
        var bucket = await CreateBucketAsync();

        var source = new StringContent("data", Encoding.UTF8, "text/plain");
        source.Headers.Add("x-amz-tagging", "env=prod");
        await Client.PutAsync($"/{bucket}/source.txt", source);

        var copyReq = new HttpRequestMessage(HttpMethod.Put, $"/{bucket}/dest.txt");
        copyReq.Headers.Add("x-amz-copy-source", $"/{bucket}/source.txt");
        copyReq.Headers.Add("x-amz-tagging-directive", "REPLACE");
        copyReq.Headers.Add("x-amz-tagging", "env=dev");
        var copy = await Client.SendAsync(copyReq);
        Assert.Equal(HttpStatusCode.OK, copy.StatusCode);

        var get = await Client.GetAsync($"/{bucket}/dest.txt?tagging");
        var tagging = DeserializeTagging(await get.Content.ReadAsStringAsync());
        Assert.Single(tagging.TagSet);
        Assert.Equal("dev", tagging.TagSet[0].Value);
    }

    [Fact]
    public async Task CopyObject_DirectiveReplaceNoHeader_EmptyTags()
    {
        var bucket = await CreateBucketAsync();

        var source = new StringContent("data", Encoding.UTF8, "text/plain");
        source.Headers.Add("x-amz-tagging", "env=prod");
        await Client.PutAsync($"/{bucket}/source.txt", source);

        var copyReq = new HttpRequestMessage(HttpMethod.Put, $"/{bucket}/dest.txt");
        copyReq.Headers.Add("x-amz-copy-source", $"/{bucket}/source.txt");
        copyReq.Headers.Add("x-amz-tagging-directive", "REPLACE");
        var copy = await Client.SendAsync(copyReq);
        Assert.Equal(HttpStatusCode.OK, copy.StatusCode);

        var get = await Client.GetAsync($"/{bucket}/dest.txt?tagging");
        var tagging = DeserializeTagging(await get.Content.ReadAsStringAsync());
        Assert.Empty(tagging.TagSet);
    }
}
