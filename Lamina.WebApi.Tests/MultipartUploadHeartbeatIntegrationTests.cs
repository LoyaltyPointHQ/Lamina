using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Xml.Serialization;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.InMemory;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Lamina.WebApi.Tests;

public class MultipartUploadHeartbeatIntegrationTests
    : IClassFixture<MultipartUploadHeartbeatIntegrationTests.HeartbeatEnabledFactory>,
      IClassFixture<MultipartUploadHeartbeatIntegrationTests.HeartbeatDisabledFactory>
{
    private readonly HttpClient _enabledClient;
    private readonly HttpClient _disabledClient;

    public MultipartUploadHeartbeatIntegrationTests(
        HeartbeatEnabledFactory enabledFactory,
        HeartbeatDisabledFactory disabledFactory)
    {
        _enabledClient = enabledFactory.CreateClient();
        _disabledClient = disabledFactory.CreateClient();
    }

    [Fact]
    public async Task CompleteMultipartUpload_SlowStorage_HeartbeatDisabled_NoLeadingWhitespace()
    {
        var (status, bytes) = await RunCompleteMultipartFlowAsync(_disabledClient);

        Assert.Equal(HttpStatusCode.OK, status);

        var leadingSpaces = bytes.TakeWhile(b => b == 0x20).Count();
        Assert.Equal(0, leadingSpaces);

        var bodyText = Encoding.UTF8.GetString(bytes).TrimStart('\uFEFF');
        Assert.StartsWith("<?xml", bodyText);
        Assert.Contains("<CompleteMultipartUploadResult", bodyText);
    }

    private static async Task<(HttpStatusCode Status, byte[] Body)> RunCompleteMultipartFlowAsync(HttpClient client)
    {
        var bucketName = $"hb-test-{Guid.NewGuid()}";
        var key = "object.bin";

        var bucketResp = await client.PutAsync($"/{bucketName}", null);
        Assert.Equal(HttpStatusCode.OK, bucketResp.StatusCode);

        var initResp = await client.PostAsync($"/{bucketName}/{key}?uploads", null);
        Assert.Equal(HttpStatusCode.OK, initResp.StatusCode);
        var initXml = await initResp.Content.ReadAsStringAsync();
        var initSerializer = new XmlSerializer(typeof(InitiateMultipartUploadResult));
        using var initReader = new StringReader(initXml);
        var initResult = (InitiateMultipartUploadResult?)initSerializer.Deserialize(initReader);
        Assert.NotNull(initResult);

        var partResp = await client.PutAsync(
            $"/{bucketName}/{key}?partNumber=1&uploadId={initResult.UploadId}",
            new StringContent("hello world", Encoding.UTF8));
        Assert.Equal(HttpStatusCode.OK, partResp.StatusCode);
        var etag = partResp.Headers.GetValues("ETag").First().Trim('"');

        var completeXml = $@"<?xml version=""1.0"" encoding=""UTF-8""?>
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>{etag}</ETag>
    </Part>
</CompleteMultipartUpload>";

        var completeResp = await client.PostAsync(
            $"/{bucketName}/{key}?uploadId={initResult.UploadId}",
            new StringContent(completeXml, Encoding.UTF8, "application/xml"));

        var bytes = await completeResp.Content.ReadAsByteArrayAsync();
        return (completeResp.StatusCode, bytes);
    }

    [Fact]
    public async Task CompleteMultipartUpload_SlowStorage_HeartbeatEnabled_ResponseHasXmlHeaderThenWhitespaceThenBody()
    {
        var (status, bytes) = await RunCompleteMultipartFlowAsync(_enabledClient);

        Assert.Equal(HttpStatusCode.OK, status);

        var bodyText = Encoding.UTF8.GetString(bytes);

        // Format wzorowany na minio sendWhiteSpace: XML declaration najpierw (boto3/expat
        // wymagają żeby <?xml było pierwsze), potem spacje (heartbeat ticks między prologiem
        // a root elementem - legalne XML 1.0 Misc*), potem root element bez powtórzenia XML decl.
        Assert.StartsWith("<?xml", bodyText);

        var endOfDecl = bodyText.IndexOf("?>", StringComparison.Ordinal);
        Assert.True(endOfDecl > 0, "Expected closing '?>' of XML declaration");

        var afterDecl = bodyText[(endOfDecl + 2)..];
        var whitespaceCount = afterDecl.TakeWhile(c => c == ' ' || c == '\n' || c == '\r').Count();
        Assert.True(whitespaceCount >= 1,
            $"Expected ≥1 whitespace byte between XML declaration and root element (heartbeat tick), got {whitespaceCount}. After decl: '{afterDecl[..Math.Min(50, afterDecl.Length)]}'");

        var bodyStart = afterDecl.TrimStart(' ', '\n', '\r');
        Assert.StartsWith("<CompleteMultipartUploadResult", bodyStart);
    }

    public class HeartbeatEnabledFactory : SlowStorageFactoryBase
    {
        protected override bool HeartbeatEnabled => true;
    }

    public class HeartbeatDisabledFactory : SlowStorageFactoryBase
    {
        protected override bool HeartbeatEnabled => false;
    }

    public abstract class SlowStorageFactoryBase : WebApplicationFactory<global::Program>
    {
        protected abstract bool HeartbeatEnabled { get; }

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            var testProjectPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..");
            var testSettingsPath = Path.Combine(testProjectPath, "Lamina.WebApi.Tests", "appsettings.Test.json");

            builder.UseEnvironment("Test");
            builder.ConfigureAppConfiguration((_, config) =>
            {
                config.Sources.Clear();
                config.AddJsonFile(testSettingsPath, optional: false, reloadOnChange: false);
                config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["MultipartUpload:Heartbeat:Enabled"] = HeartbeatEnabled ? "true" : "false",
                    ["MultipartUpload:Heartbeat:IntervalSeconds"] = "1"
                });
            });
            builder.ConfigureServices(services =>
            {
                services.AddSingleton<IObjectMetadataStorage, InMemoryObjectMetadataStorage>();
                services.AddSingleton<IBucketMetadataStorage, InMemoryBucketMetadataStorage>();
                services.AddSingleton<IMultipartUploadMetadataStorage, InMemoryMultipartUploadMetadataStorage>();

                services.RemoveAll<IMultipartUploadStorageFacade>();
                services.AddScoped<IMultipartUploadStorageFacade>(provider =>
                {
                    var inner = ActivatorUtilities.CreateInstance<Lamina.Storage.Core.MultipartUploadStorageFacade>(provider);
                    return new DelayingCompleteFacade(inner, TimeSpan.FromMilliseconds(2500));
                });
            });
        }
    }

    private class DelayingCompleteFacade : IMultipartUploadStorageFacade
    {
        private readonly IMultipartUploadStorageFacade _inner;
        private readonly TimeSpan _completeDelay;

        public DelayingCompleteFacade(IMultipartUploadStorageFacade inner, TimeSpan completeDelay)
        {
            _inner = inner;
            _completeDelay = completeDelay;
        }

        public Task<MultipartUpload> InitiateMultipartUploadAsync(string bucketName, string key, InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
            => _inner.InitiateMultipartUploadAsync(bucketName, key, request, cancellationToken);

        public Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, CancellationToken cancellationToken = default)
            => _inner.UploadPartAsync(bucketName, key, uploadId, partNumber, dataReader, cancellationToken);

        public Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, CancellationToken cancellationToken = default)
            => _inner.UploadPartAsync(bucketName, key, uploadId, partNumber, dataReader, chunkValidator, cancellationToken);

        public Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, ChecksumRequest? checksumRequest, CancellationToken cancellationToken = default)
            => _inner.UploadPartAsync(bucketName, key, uploadId, partNumber, dataReader, checksumRequest, cancellationToken);

        public Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, ChecksumRequest? checksumRequest, CancellationToken cancellationToken = default)
            => _inner.UploadPartAsync(bucketName, key, uploadId, partNumber, dataReader, chunkValidator, checksumRequest, cancellationToken);

        public async Task<StorageResult<CompleteMultipartUploadResponse>> CompleteMultipartUploadAsync(string bucketName, string key, CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
        {
            await Task.Delay(_completeDelay, cancellationToken);
            return await _inner.CompleteMultipartUploadAsync(bucketName, key, request, cancellationToken);
        }

        public Task<bool> AbortMultipartUploadAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
            => _inner.AbortMultipartUploadAsync(bucketName, key, uploadId, cancellationToken);

        public Task<List<UploadPart>> ListPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
            => _inner.ListPartsAsync(bucketName, key, uploadId, cancellationToken);

        public Task<List<MultipartUpload>> ListMultipartUploadsAsync(string bucketName, CancellationToken cancellationToken = default)
            => _inner.ListMultipartUploadsAsync(bucketName, cancellationToken);
    }
}
