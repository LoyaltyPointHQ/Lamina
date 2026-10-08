using System.Net;
using System.Text;
using System.Xml.Linq;
using Lamina.Core.Models;
using Lamina.Storage.Core;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.InMemory;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;

namespace Lamina.WebApi.Tests.Controllers;

public class MultipartStorageFailureTests
{
    public static IEnumerable<object[]> Failures =>
        from operation in new[] { "initiate", "update", "read", "head", "list", "delete", "complete", "copy", "copy-update" }
        from failure in new[] { "lock", "io", "invalid-operation" }
        select new object[] { operation, failure };

    [Theory]
    [MemberData(nameof(Failures))]
    public async Task InfrastructureFailure_ReturnsSafeInternalError_NotMissingUpload(string operation, string failure)
    {
        using var host = new FailureHost();
        host.FailureFactory = failure switch
        {
            "lock" => () => new LockAcquisitionException("write"),
            "io" => () => new IOException("Unavailable /private/storage/upload.metadata.json"),
            _ => () => new InvalidOperationException("Unexpected failure in /private/storage/upload.metadata.json")
        };
        using var client = host.Factory.CreateClient();
        var uploadId = await PrepareUploadAsync(client);
        host.FailingOperation = operation switch
        {
            "complete" or "copy" or "head" => "read",
            "copy-update" => "update",
            _ => operation
        };
        client.DefaultRequestHeaders.Accept.ParseAdd("application/json");

        using var response = operation switch
        {
            "initiate" => await client.PostAsync("/test-bucket/object?uploads", null),
            "update" => await client.PutAsync($"/test-bucket/object?uploadId={uploadId}&partNumber=1", new ByteArrayContent([1, 2, 3])),
            "read" => await client.GetAsync($"/test-bucket/object?uploadId={uploadId}"),
            "head" => await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, $"/test-bucket/object?uploadId={uploadId}")),
            "list" => await client.GetAsync("/test-bucket?uploads"),
            "delete" => await client.DeleteAsync($"/test-bucket/object?uploadId={uploadId}"),
            "complete" => await client.PostAsync($"/test-bucket/object?uploadId={uploadId}", CompleteBody(host.ETag!)),
            "copy" => await CopyPartAsync(client, uploadId),
            "copy-update" => await CopyPartAsync(client, uploadId, range: true),
            _ => throw new ArgumentOutOfRangeException(nameof(operation))
        };

        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        var text = await response.Content.ReadAsStringAsync();
        if (operation == "head")
        {
            Assert.Empty(text);
            Assert.True(response.Headers.Contains("x-amz-request-id"));
            Assert.True(response.Headers.Contains("x-amz-id-2"));
            return;
        }
        var xml = XDocument.Parse(text);
        Assert.Equal("Error", xml.Root!.Name.LocalName);
        Assert.Equal("InternalError", xml.Root.Element("Code")?.Value);
        Assert.DoesNotContain("/private/storage", text);
        Assert.Equal(response.Headers.GetValues("x-amz-request-id").Single(), xml.Root.Element("RequestId")?.Value);
        Assert.Equal(response.Headers.GetValues("x-amz-id-2").Single(), xml.Root.Element("HostId")?.Value);

        async Task<string> PrepareUploadAsync(HttpClient http)
        {
            (await http.PutAsync("/test-bucket", null)).EnsureSuccessStatusCode();
            (await http.PutAsync("/test-bucket/source", new ByteArrayContent([4, 5, 6]))).EnsureSuccessStatusCode();
            using var initiated = await http.PostAsync("/test-bucket/object?uploads", null);
            initiated.EnsureSuccessStatusCode();
            var id = XDocument.Parse(await initiated.Content.ReadAsStringAsync()).Descendants()
                .Single(e => e.Name.LocalName == "UploadId").Value;
            using var part = await http.PutAsync($"/test-bucket/object?uploadId={id}&partNumber=1", new ByteArrayContent([1, 2, 3]));
            part.EnsureSuccessStatusCode();
            host.ETag = part.Headers.ETag!.Tag;
            return id;
        }
    }

    [Fact]
    public async Task FailedMetadataWrite_CanRetryPartAndCompleteWithIntactContent()
    {
        using var host = new FailureHost
        {
            FailureFactory = () => new LockAcquisitionException("write")
        };
        using var client = host.Factory.CreateClient();
        (await client.PutAsync("/test-bucket", null)).EnsureSuccessStatusCode();
        using var initiated = await client.PostAsync("/test-bucket/object?uploads", null);
        initiated.EnsureSuccessStatusCode();
        var uploadId = XDocument.Parse(await initiated.Content.ReadAsStringAsync()).Descendants()
            .Single(e => e.Name.LocalName == "UploadId").Value;
        var path = $"/test-bucket/object?uploadId={uploadId}&partNumber=1";
        var bytes = Encoding.UTF8.GetBytes("The entire validated part survives metadata lock failure.");
        host.FailingOperation = "update";
        using var failed = await client.PutAsync(path, new ByteArrayContent(bytes));
        Assert.Equal(HttpStatusCode.InternalServerError, failed.StatusCode);

        host.FailingOperation = null;
        using var retried = await client.PutAsync(path, new ByteArrayContent(bytes));
        retried.EnsureSuccessStatusCode();
        using var completed = await client.PostAsync($"/test-bucket/object?uploadId={uploadId}", CompleteBody(retried.Headers.ETag!.Tag));
        completed.EnsureSuccessStatusCode();
        Assert.Contains("CompleteMultipartUploadResult", await completed.Content.ReadAsStringAsync());
        Assert.Equal(bytes, await client.GetByteArrayAsync("/test-bucket/object"));

        using var missing = await client.GetAsync($"/test-bucket/object?uploadId={uploadId}");
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
        Assert.Contains("<Code>NoSuchUpload</Code>", await missing.Content.ReadAsStringAsync());
    }

    private static async Task<HttpResponseMessage> CopyPartAsync(HttpClient client, string uploadId, bool range = false)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, $"/test-bucket/object?uploadId={uploadId}&partNumber=2");
        request.Headers.Add("x-amz-copy-source", "/test-bucket/source");
        if (range) request.Headers.Add("x-amz-copy-source-range", "bytes=0-1");
        return await client.SendAsync(request);
    }

    private static StringContent CompleteBody(string etag) => new(
        $"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>",
        Encoding.UTF8, "application/xml");

    private sealed class FailureHost : IDisposable
    {
        public string? FailingOperation { get; set; }
        public string? ETag { get; set; }
        public Func<Exception> FailureFactory { get; set; } = () => new InvalidOperationException("Unexpected storage failure");
        public WebApplicationFactory<global::Program> Factory { get; }

        public FailureHost()
        {
            var inner = new InMemoryMultipartUploadMetadataStorage();
            var storage = new Mock<IMultipartUploadMetadataStorage>();
            storage.Setup(s => s.InitiateUploadAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<InitiateMultipartUploadRequest>(), It.IsAny<CancellationToken>()))
                .Returns((string b, string k, InitiateMultipartUploadRequest r, CancellationToken ct) => Run("initiate", () => inner.InitiateUploadAsync(b, k, r, ct)));
            storage.Setup(s => s.GetUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns((string b, string k, string id, CancellationToken ct) => Run("read", () => inner.GetUploadMetadataAsync(b, k, id, ct)));
            storage.Setup(s => s.ListUploadsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns((string b, CancellationToken ct) => Run("list", () => inner.ListUploadsAsync(b, ct)));
            storage.Setup(s => s.DeleteUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns((string b, string k, string id, CancellationToken ct) => Run("delete", () => inner.DeleteUploadMetadataAsync(b, k, id, ct)));
            storage.Setup(s => s.UpdateUploadMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<MultipartUpload>(), It.IsAny<CancellationToken>()))
                .Returns((string b, string k, string id, MultipartUpload u, CancellationToken ct) =>
                    FailingOperation == "update" ? Task.FromException(FailureFactory()) : inner.UpdateUploadMetadataAsync(b, k, id, u, ct));

            Factory = new WebApplicationFactory<global::Program>().WithWebHostBuilder(builder =>
            {
                builder.UseEnvironment("Test");
                builder.ConfigureAppConfiguration((_, config) =>
                {
                    config.Sources.Clear();
                    config.AddInMemoryCollection(new Dictionary<string, string?>
                    {
                        ["StorageType"] = "InMemory",
                        ["MetadataStorageType"] = "InMemory",
                        ["Authentication:Enabled"] = "false",
                        ["AutoBucketCreation:Enabled"] = "false",
                        ["MultipartUploadCleanup:Enabled"] = "false",
                        ["MetadataCleanup:Enabled"] = "false",
                        ["TempFileCleanup:Enabled"] = "false",
                        ["LifecycleExpiration:Enabled"] = "false",
                        ["MultipartUpload:Heartbeat:Enabled"] = "false"
                    });
                });
                builder.ConfigureServices(services =>
                {
                    services.RemoveAll<IBucketDataStorage>();
                    services.AddSingleton<IBucketDataStorage, InMemoryBucketDataStorage>();
                    services.RemoveAll<IBucketMetadataStorage>();
                    services.AddSingleton<IBucketMetadataStorage, InMemoryBucketMetadataStorage>();
                    services.RemoveAll<IObjectDataStorage>();
                    services.AddSingleton<IObjectDataStorage, InMemoryObjectDataStorage>();
                    services.RemoveAll<IObjectMetadataStorage>();
                    services.AddSingleton<IObjectMetadataStorage, InMemoryObjectMetadataStorage>();
                    services.RemoveAll<IMultipartUploadDataStorage>();
                    services.AddSingleton<IMultipartUploadDataStorage, InMemoryMultipartUploadDataStorage>();
                    services.RemoveAll<IMultipartUploadMetadataStorage>();
                    services.AddSingleton(storage.Object);
                });
            });
        }

        private Task<T> Run<T>(string operation, Func<Task<T>> action) =>
            FailingOperation == operation ? Task.FromException<T>(FailureFactory()) : action();

        public void Dispose() => Factory.Dispose();
    }
}
