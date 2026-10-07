using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Lamina.WebApi.Tests.Controllers;

public class FormContentTypeIntegrationTests(WebApplicationFactory<global::Program> factory)
    : IntegrationTestBase(factory)
{
    private static readonly byte[] Payload = [.. Encoding.UTF8.GetBytes("field=a+b%25&other=value=raw\r\n"), 0, 0xff, 0x80];
    private static readonly XNamespace S3 = "http://s3.amazonaws.com/doc/2006-03-01/";

    [Theory]
    [InlineData("application/x-www-form-urlencoded")]
    [InlineData("application/x-www-form-urlencoded; charset=utf-8")]
    [InlineData("multipart/form-data; boundary=lamina-test")]
    [InlineData("application/octet-stream")]
    public async Task PutObject_FormContentType_PreservesRawBytesAndMetadata(string contentType)
    {
        var bucket = await CreateBucketAsync();
        var path = $"/{bucket}/raw.bin";
        try
        {
            using var content = RawContent(contentType);
            using var put = await Client.PutAsync(path, content);
            Assert.Equal(HttpStatusCode.OK, put.StatusCode);
            var etag = Convert.ToHexString(MD5.HashData(Payload)).ToLowerInvariant();
            Assert.Equal($"\"{etag}\"", put.Headers.ETag?.Tag);
            await AssertObjectAsync(path, contentType, etag);
        }
        finally
        {
            using var delete = await Client.DeleteAsync(path);
            using var deleteBucket = await Client.DeleteAsync($"/{bucket}");
        }
    }

    [Theory]
    [InlineData("application/x-www-form-urlencoded")]
    [InlineData("application/x-www-form-urlencoded; charset=utf-8")]
    [InlineData("multipart/form-data; boundary=lamina-test")]
    [InlineData("application/octet-stream")]
    public async Task UploadPart_FormContentType_PreservesRawBytesThroughComplete(string contentType)
    {
        var bucket = await CreateBucketAsync();
        var path = $"/{bucket}/multipart.bin";
        string? uploadId = null;
        try
        {
            using var initContent = new ByteArrayContent([]);
            initContent.Headers.ContentType = new MediaTypeHeaderValue("application/x-lamina-object");
            using var initiate = await Client.PostAsync($"{path}?uploads", initContent);
            Assert.Equal(HttpStatusCode.OK, initiate.StatusCode);
            uploadId = XDocument.Parse(await initiate.Content.ReadAsStringAsync()).Root!.Element(S3 + "UploadId")!.Value;

            using var content = RawContent(contentType);
            using var part = await Client.PutAsync($"{path}?partNumber=1&uploadId={uploadId}", content);
            Assert.Equal(HttpStatusCode.OK, part.StatusCode);
            var partMd5 = MD5.HashData(Payload);
            var partEtag = Convert.ToHexString(partMd5).ToLowerInvariant();
            Assert.Equal($"\"{partEtag}\"", part.Headers.ETag?.Tag);

            using var list = await Client.GetAsync($"{path}?uploadId={uploadId}");
            Assert.Equal(HttpStatusCode.OK, list.StatusCode);
            var listedPart = Assert.Single(XDocument.Parse(await list.Content.ReadAsStringAsync()).Root!.Elements(S3 + "Part"));
            Assert.Equal(Payload.Length, (int)listedPart.Element(S3 + "Size")!);
            Assert.Equal(partEtag, listedPart.Element(S3 + "ETag")!.Value.Trim('"'));

            using var completeContent = new StringContent(
                $"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{partEtag}\"</ETag></Part></CompleteMultipartUpload>",
                Encoding.UTF8, "application/xml");
            using var complete = await Client.PostAsync($"{path}?uploadId={uploadId}", completeContent);
            Assert.Equal(HttpStatusCode.OK, complete.StatusCode);
            var finalEtag = Convert.ToHexString(MD5.HashData(partMd5)).ToLowerInvariant() + "-1";
            var completeXml = XDocument.Parse(await complete.Content.ReadAsStringAsync());
            Assert.Equal(finalEtag, completeXml.Root!.Element(S3 + "ETag")!.Value.Trim('"'));
            await AssertObjectAsync(path, "application/x-lamina-object", finalEtag);
        }
        finally
        {
            if (uploadId is not null)
            {
                using var abort = await Client.DeleteAsync($"{path}?uploadId={uploadId}");
            }
            using var delete = await Client.DeleteAsync(path);
            using var deleteBucket = await Client.DeleteAsync($"/{bucket}");
        }
    }

    private async Task<string> CreateBucketAsync()
    {
        var bucket = $"form-content-{Guid.NewGuid():N}";
        using var response = await Client.PutAsync($"/{bucket}", null);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return bucket;
    }

    private static ByteArrayContent RawContent(string contentType)
    {
        var content = new ByteArrayContent(Payload);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse(contentType);
        content.Headers.ContentMD5 = MD5.HashData(Payload);
        return content;
    }

    private async Task AssertObjectAsync(string path, string contentType, string etag)
    {
        using var get = await Client.GetAsync(path);
        Assert.Equal(HttpStatusCode.OK, get.StatusCode);
        Assert.Equal(Payload, await get.Content.ReadAsByteArrayAsync());
        Assert.Equal(Payload.Length, get.Content.Headers.ContentLength);
        Assert.Equal(contentType, get.Content.Headers.ContentType?.ToString());
        Assert.Equal($"\"{etag}\"", get.Headers.ETag?.Tag);
    }
}
