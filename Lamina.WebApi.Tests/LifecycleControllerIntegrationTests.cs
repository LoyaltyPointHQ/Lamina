using System.Net;
using System.Text;
using System.Xml.Serialization;
using Lamina.Core.Models;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Lamina.WebApi.Tests.Controllers;

public class LifecycleControllerIntegrationTests : IntegrationTestBase
{
    public LifecycleControllerIntegrationTests(WebApplicationFactory<global::Program> factory) : base(factory)
    {
    }

    private async Task<string> CreateBucketAsync()
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        return bucketName;
    }

    private const string BasicLifecycleXml = @"<?xml version=""1.0"" encoding=""UTF-8""?>
<LifecycleConfiguration xmlns=""http://s3.amazonaws.com/doc/2006-03-01/"">
  <Rule>
    <ID>expire-logs</ID>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>7</Days></Expiration>
  </Rule>
</LifecycleConfiguration>";

    private static StringContent XmlBody(string xml) => new(xml, Encoding.UTF8, "application/xml");

    [Fact]
    public async Task PutLifecycle_Valid_Returns200()
    {
        var bucket = await CreateBucketAsync();

        var response = await Client.PutAsync($"/{bucket}?lifecycle", XmlBody(BasicLifecycleXml));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task GetLifecycle_AfterPut_RoundTripsXml()
    {
        var bucket = await CreateBucketAsync();
        await Client.PutAsync($"/{bucket}?lifecycle", XmlBody(BasicLifecycleXml));

        var response = await Client.GetAsync($"/{bucket}?lifecycle");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var xml = await response.Content.ReadAsStringAsync();

        var serializer = new XmlSerializer(typeof(LifecycleConfigurationXml));
        using var reader = new StringReader(xml);
        var config = (LifecycleConfigurationXml)serializer.Deserialize(reader)!;
        Assert.Single(config.Rules);
        Assert.Equal("expire-logs", config.Rules[0].Id);
        Assert.Equal("Enabled", config.Rules[0].Status);
        Assert.Equal("logs/", config.Rules[0].Filter?.Prefix);
        Assert.Equal(7, config.Rules[0].Expiration?.Days);
    }

    [Fact]
    public async Task GetLifecycle_NoConfiguration_Returns404NoSuchLifecycle()
    {
        var bucket = await CreateBucketAsync();

        var response = await Client.GetAsync($"/{bucket}?lifecycle");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("NoSuchLifecycleConfiguration", body);
    }

    [Fact]
    public async Task DeleteLifecycle_RemovesConfiguration()
    {
        var bucket = await CreateBucketAsync();
        await Client.PutAsync($"/{bucket}?lifecycle", XmlBody(BasicLifecycleXml));

        var delete = await Client.DeleteAsync($"/{bucket}?lifecycle");
        Assert.Equal(HttpStatusCode.NoContent, delete.StatusCode);

        var get = await Client.GetAsync($"/{bucket}?lifecycle");
        Assert.Equal(HttpStatusCode.NotFound, get.StatusCode);
    }

    [Fact]
    public async Task PutLifecycle_NonExistentBucket_Returns404NoSuchBucket()
    {
        var response = await Client.PutAsync($"/missing-{Guid.NewGuid()}?lifecycle", XmlBody(BasicLifecycleXml));

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("NoSuchBucket", body);
    }

    [Fact]
    public async Task PutLifecycle_MalformedXml_Returns400()
    {
        var bucket = await CreateBucketAsync();

        var response = await Client.PutAsync($"/{bucket}?lifecycle", XmlBody("not xml"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("MalformedXML", body);
    }

    [Fact]
    public async Task PutLifecycle_WithTransition_Returns501NotImplemented()
    {
        var bucket = await CreateBucketAsync();
        var xmlWithTransition = @"<?xml version=""1.0"" encoding=""UTF-8""?>
<LifecycleConfiguration xmlns=""http://s3.amazonaws.com/doc/2006-03-01/"">
  <Rule>
    <ID>r1</ID>
    <Filter><Prefix></Prefix></Filter>
    <Status>Enabled</Status>
    <Transition><Days>30</Days><StorageClass>GLACIER</StorageClass></Transition>
  </Rule>
</LifecycleConfiguration>";

        var response = await Client.PutAsync($"/{bucket}?lifecycle", XmlBody(xmlWithTransition));

        Assert.Equal(HttpStatusCode.NotImplemented, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("NotImplemented", body);
        Assert.Contains("Transition", body);
    }

    [Fact]
    public async Task PutLifecycle_MalformedXml_StillReturns400()
    {
        // Sanity check: malformed XML (not an unsupported feature) stays on 400
        var bucket = await CreateBucketAsync();

        var response = await Client.PutAsync($"/{bucket}?lifecycle", XmlBody("not xml at all"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("MalformedXML", body);
    }

    [Fact]
    public async Task PutLifecycle_WithAbortMPU_Success()
    {
        var bucket = await CreateBucketAsync();
        var xml = @"<?xml version=""1.0"" encoding=""UTF-8""?>
<LifecycleConfiguration xmlns=""http://s3.amazonaws.com/doc/2006-03-01/"">
  <Rule>
    <ID>mpu-rule</ID>
    <Filter><Prefix>incomplete/</Prefix></Filter>
    <Status>Enabled</Status>
    <AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>";

        var response = await Client.PutAsync($"/{bucket}?lifecycle", XmlBody(xml));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var get = await Client.GetAsync($"/{bucket}?lifecycle");
        var getBody = await get.Content.ReadAsStringAsync();
        Assert.Contains("AbortIncompleteMultipartUpload", getBody);
    }
}
