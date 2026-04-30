using System.IO.Pipelines;
using System.Net;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;

namespace Lamina.WebApi.Tests.Controllers;

public class ErrorMappingIntegrationTests : IClassFixture<WebApplicationFactory<global::Program>>
{
    private readonly WebApplicationFactory<global::Program> _factory;

    public ErrorMappingIntegrationTests(WebApplicationFactory<global::Program> factory)
    {
        _factory = factory;
    }

    private HttpClient CreateClientWithStorageThatReturns(StorageResult<S3Object> putResult)
    {
        var mockObjectFacade = new Mock<IObjectStorageFacade>();
        mockObjectFacade
            .Setup(x => x.PutObjectAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<PipeReader>(),
                It.IsAny<IChunkSignatureValidator?>(), It.IsAny<PutObjectRequest?>(),
                It.IsAny<byte[]?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(putResult);
        mockObjectFacade
            .Setup(x => x.PutObjectAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<PipeReader>(),
                It.IsAny<PutObjectRequest?>(), It.IsAny<byte[]?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(putResult);
        mockObjectFacade
            .Setup(x => x.IsValidObjectKey(It.IsAny<string>()))
            .Returns(true);

        var mockBucketFacade = new Mock<IBucketStorageFacade>();
        mockBucketFacade
            .Setup(x => x.BucketExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var testProjectPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..");
        var testSettingsPath = Path.Combine(testProjectPath, "Lamina.WebApi.Tests", "appsettings.Test.json");

        return _factory.WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Test");
            builder.ConfigureAppConfiguration((_, config) =>
            {
                config.Sources.Clear();
                config.AddJsonFile(testSettingsPath, optional: false);
            });
            builder.ConfigureServices(services =>
            {
                services.RemoveAll<IObjectStorageFacade>();
                services.AddSingleton(mockObjectFacade.Object);
                services.RemoveAll<IBucketStorageFacade>();
                services.AddSingleton(mockBucketFacade.Object);
            });
        }).CreateClient();
    }

    [Fact]
    public async Task PutObject_GenericStorageFailure_Returns500WithXmlBody()
    {
        var bucketName = $"test-{Guid.NewGuid()}";
        var client = CreateClientWithStorageThatReturns(
            StorageResult<S3Object>.Error("SomeUnexpectedError", "Something went wrong internally"));

        var content = new ByteArrayContent([1, 2, 3]);
        content.Headers.ContentLength = 3;
        var response = await client.PutAsync($"/{bucketName}/test.txt", content);

        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("<Code>InternalError</Code>", body);
    }
}
