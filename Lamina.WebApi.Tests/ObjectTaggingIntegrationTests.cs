using System.Net;
using System.Text;
using System.Xml.Serialization;
using Lamina.Core.Models;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Lamina.WebApi.Tests.Controllers;

public class ObjectTaggingIntegrationTests : IntegrationTestBase
{
    public ObjectTaggingIntegrationTests(WebApplicationFactory<global::Program> factory) : base(factory)
    {
    }

    private async Task<string> CreateBucketWithObjectAsync(string key)
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        await Client.PutAsync($"/{bucketName}/{key}", new StringContent("data", Encoding.UTF8, "text/plain"));
        return bucketName;
    }

    private static StringContent TaggingXmlContent(params (string key, string value)[] tags)
    {
        var tagsXml = string.Join("", tags.Select(t => $"<Tag><Key>{t.key}</Key><Value>{t.value}</Value></Tag>"));
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet>{tagsXml}</TagSet></Tagging>";
        return new StringContent(body, Encoding.UTF8, "application/xml");
    }

    [Fact]
    public async Task PutObjectTagging_ValidRequest_Returns200AndGetReturnsSame()
    {
        var bucket = await CreateBucketWithObjectAsync("file.txt");

        var put = await Client.PutAsync($"/{bucket}/file.txt?tagging",
            TaggingXmlContent(("env", "prod"), ("team", "core")));
        Assert.Equal(HttpStatusCode.OK, put.StatusCode);

        var get = await Client.GetAsync($"/{bucket}/file.txt?tagging");
        Assert.Equal(HttpStatusCode.OK, get.StatusCode);

        var xml = await get.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(TaggingXml));
        using var reader = new StringReader(xml);
        var tagging = (TaggingXml)serializer.Deserialize(reader)!;

        Assert.Equal(2, tagging.TagSet.Count);
        Assert.Contains(tagging.TagSet, t => t.Key == "env" && t.Value == "prod");
        Assert.Contains(tagging.TagSet, t => t.Key == "team" && t.Value == "core");
    }

    [Fact]
    public async Task PutObjectTagging_NonExistentObject_Returns404()
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);

        var response = await Client.PutAsync($"/{bucketName}/missing.txt?tagging", TaggingXmlContent(("k", "v")));

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("NoSuchKey", body);
    }

    [Fact]
    public async Task PutObjectTagging_MalformedXml_Returns400()
    {
        var bucket = await CreateBucketWithObjectAsync("file.txt");

        var content = new StringContent("not xml", Encoding.UTF8, "application/xml");
        var response = await Client.PutAsync($"/{bucket}/file.txt?tagging", content);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("MalformedXML", body);
    }

    [Fact]
    public async Task PutObjectTagging_ElevenTags_Returns400InvalidTag()
    {
        var bucket = await CreateBucketWithObjectAsync("file.txt");
        var tags = Enumerable.Range(1, 11).Select(i => ($"k{i}", $"v{i}")).ToArray();

        var response = await Client.PutAsync($"/{bucket}/file.txt?tagging", TaggingXmlContent(tags));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("InvalidTag", body);
    }

    [Fact]
    public async Task GetObjectTagging_NonExistentObject_Returns404()
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);

        var response = await Client.GetAsync($"/{bucketName}/missing.txt?tagging");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("NoSuchKey", body);
    }

    [Fact]
    public async Task GetObjectTagging_NoTagsSet_ReturnsEmpty()
    {
        var bucket = await CreateBucketWithObjectAsync("file.txt");

        var response = await Client.GetAsync($"/{bucket}/file.txt?tagging");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var xml = await response.Content.ReadAsStringAsync();
        Assert.Contains("TagSet", xml);
    }

    [Fact]
    public async Task DeleteObjectTagging_RemovesTags()
    {
        var bucket = await CreateBucketWithObjectAsync("file.txt");
        await Client.PutAsync($"/{bucket}/file.txt?tagging", TaggingXmlContent(("a", "1")));

        var delete = await Client.DeleteAsync($"/{bucket}/file.txt?tagging");
        Assert.Equal(HttpStatusCode.NoContent, delete.StatusCode);

        var get = await Client.GetAsync($"/{bucket}/file.txt?tagging");
        var xml = await get.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(TaggingXml));
        using var reader = new StringReader(xml);
        var tagging = (TaggingXml)serializer.Deserialize(reader)!;

        Assert.Empty(tagging.TagSet);
    }

    [Fact]
    public async Task PutObjectTagging_DuplicateKeys_Returns400()
    {
        var bucket = await CreateBucketWithObjectAsync("file.txt");

        var response = await Client.PutAsync($"/{bucket}/file.txt?tagging",
            TaggingXmlContent(("same", "1"), ("same", "2")));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("InvalidTag", body);
    }
}
