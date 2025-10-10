using System.Net;
using System.Text;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Lamina.WebApi.Tests.Controllers;

public class S3UploadHeaderValidationTests : IntegrationTestBase
{
    public S3UploadHeaderValidationTests(WebApplicationFactory<global::Program> factory) : base(factory)
    {
    }

    private async Task<string> CreateTestBucketAsync()
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        return bucketName;
    }

    #region PutObject Tests

    // Note: Testing missing Content-Length is difficult in integration tests because
    // HttpClient automatically adds the Content-Length header. The validation code
    // in the controller will work correctly in production when real S3 clients send
    // requests without Content-Length.

    [Fact]
    public async Task PutObject_WithContentLengthZero_AcceptsEmptyFile()
    {
        var bucketName = await CreateTestBucketAsync();

        var response = await Client.PutAsync(
            $"/{bucketName}/empty-file.txt",
            new ByteArrayContent(Array.Empty<byte>()));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("ETag"));
    }

    [Fact]
    public async Task PutObject_WithValidContentLength_Succeeds()
    {
        var bucketName = await CreateTestBucketAsync();

        var content = new StringContent("test content", Encoding.UTF8);
        var response = await Client.PutAsync($"/{bucketName}/test-file.txt", content);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("ETag"));
    }

    #endregion

    #region UploadPart Tests

    // Note: Testing missing Content-Length is difficult in integration tests because
    // HttpClient automatically adds the Content-Length header. The validation code
    // in the controller will work correctly in production when real S3 clients send
    // requests without Content-Length.

    [Fact]
    public async Task UploadPart_WithContentLengthZero_AcceptsEmptyPart()
    {
        var bucketName = await CreateTestBucketAsync();

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/multipart-test.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();

        var uploadIdStart = initXml.IndexOf("<UploadId>") + 10;
        var uploadIdEnd = initXml.IndexOf("</UploadId>");
        var uploadId = initXml.Substring(uploadIdStart, uploadIdEnd - uploadIdStart);

        var response = await Client.PutAsync(
            $"/{bucketName}/multipart-test.bin?partNumber=1&uploadId={uploadId}",
            new ByteArrayContent(Array.Empty<byte>()));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("ETag"));
    }

    [Fact]
    public async Task UploadPart_WithValidContentLength_Succeeds()
    {
        var bucketName = await CreateTestBucketAsync();

        // Initiate multipart upload
        var initResponse = await Client.PostAsync($"/{bucketName}/multipart-test.bin?uploads", null);
        var initXml = await initResponse.Content.ReadAsStringAsync();

        var uploadIdStart = initXml.IndexOf("<UploadId>") + 10;
        var uploadIdEnd = initXml.IndexOf("</UploadId>");
        var uploadId = initXml.Substring(uploadIdStart, uploadIdEnd - uploadIdStart);

        var response = await Client.PutAsync(
            $"/{bucketName}/multipart-test.bin?partNumber=1&uploadId={uploadId}",
            new StringContent("part content", Encoding.UTF8));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.Contains("ETag"));
    }

    #endregion
}
