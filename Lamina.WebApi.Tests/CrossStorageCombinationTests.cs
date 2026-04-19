using System.Net;
using System.Text;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.InMemory;
using Lamina.Core.Streaming;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace Lamina.WebApi.Tests.Controllers;

/// <summary>
/// End-to-end proof that filesystem metadata storage is genuinely independent of the data
/// storage backend. The composition here - data in memory, metadata JSON on disk - was the
/// whole point of decoupling <see cref="IObjectDataStorage"/> from
/// <see cref="FilesystemJsonObjectMetadataStorageBase"/>. If these tests pass, the metadata
/// storage never touches the data backend's filesystem layout directly.
/// </summary>
public class CrossStorageCombinationTests : IClassFixture<WebApplicationFactory<global::Program>>, IDisposable
{
    private readonly WebApplicationFactory<global::Program> _factory;
    private readonly HttpClient _client;
    private readonly string _metadataDirectory;

    public CrossStorageCombinationTests(WebApplicationFactory<global::Program> factory)
    {
        _metadataDirectory = Path.Combine(Path.GetTempPath(), $"lamina-crossstorage-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_metadataDirectory);

        _factory = factory.WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Test");
            builder.ConfigureAppConfiguration((context, config) =>
            {
                config.Sources.Clear();
                config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["Logging:LogLevel:Default"] = "Warning",
                    ["StorageType"] = "InMemory",
                    ["FilesystemStorage:DataDirectory"] = _metadataDirectory, // unused for data, satisfies options
                    ["FilesystemStorage:MetadataDirectory"] = _metadataDirectory,
                    ["FilesystemStorage:MetadataMode"] = "SeparateDirectory",
                    ["MultipartUploadCleanup:Enabled"] = "false",
                    ["MetadataCleanup:Enabled"] = "false",
                    ["TempFileCleanup:Enabled"] = "false",
                    ["LifecycleExpiration:Enabled"] = "false",
                    ["MetadataCache:Enabled"] = "false"
                });
            });
            builder.ConfigureServices(services =>
            {
                // Replace metadata storages registered by Program.cs (it reads StorageType from the
                // original appsettings.json which may say Filesystem) with explicit combinations we
                // want to exercise: InMemory data × Filesystem SeparateDirectory metadata.
                RemoveByServiceType(services, typeof(IObjectMetadataStorage));
                RemoveByServiceType(services, typeof(IBucketMetadataStorage));
                RemoveByServiceType(services, typeof(IMultipartUploadMetadataStorage));

                // Filesystem storage needs its options registered.
                services.Configure<FilesystemStorageSettings>(opts =>
                {
                    opts.DataDirectory = _metadataDirectory;
                    opts.MetadataDirectory = _metadataDirectory;
                    opts.MetadataMode = MetadataStorageMode.SeparateDirectory;
                });
                services.AddSingleton<NetworkFileSystemHelper>();
                services.AddSingleton<IFileSystemLockManager, InMemoryLockManager>();
                services.AddMemoryCache();

                services.AddScoped<IObjectMetadataStorage, SeparateDirectoryObjectMetadataStorage>();
                services.AddScoped<IBucketMetadataStorage, FilesystemBucketMetadataStorage>();
                services.AddSingleton<IMultipartUploadMetadataStorage, InMemoryMultipartUploadMetadataStorage>();
            });
        });
        _client = _factory.CreateClient();
    }

    private static void RemoveByServiceType(IServiceCollection services, Type serviceType)
    {
        var descriptor = services.FirstOrDefault(d => d.ServiceType == serviceType);
        if (descriptor != null)
        {
            services.Remove(descriptor);
        }
    }

    [Fact]
    public async Task PutGetDelete_WithInMemoryDataAndFilesystemMetadata_PreservesCustomMetadata()
    {
        const string bucket = "xstorage-bucket";
        const string key = "greeting.txt";
        var payload = Encoding.UTF8.GetBytes("ala ma kota");

        // Create bucket
        var createBucket = await _client.SendAsync(new HttpRequestMessage(HttpMethod.Put, $"/{bucket}"));
        Assert.Equal(HttpStatusCode.OK, createBucket.StatusCode);

        // PUT with a non-default content type and a user-metadata header - both force metadata
        // to be persisted to the filesystem metadata store.
        var put = new HttpRequestMessage(HttpMethod.Put, $"/{bucket}/{key}")
        {
            Content = new ByteArrayContent(payload)
        };
        put.Content.Headers.TryAddWithoutValidation("Content-Type", "text/plain; charset=utf-8");
        put.Headers.TryAddWithoutValidation("x-amz-meta-lang", "pl");
        var putResp = await _client.SendAsync(put);
        Assert.Equal(HttpStatusCode.OK, putResp.StatusCode);

        // The metadata JSON must exist on disk even though data lives in memory.
        var expectedMetadataPath = Path.Combine(_metadataDirectory, bucket, $"{key}.json");
        Assert.True(File.Exists(expectedMetadataPath),
            $"Expected metadata file at {expectedMetadataPath} but it does not exist.");

        // HEAD should report back the custom metadata from the filesystem store.
        var head = await _client.SendAsync(new HttpRequestMessage(HttpMethod.Head, $"/{bucket}/{key}"));
        Assert.Equal(HttpStatusCode.OK, head.StatusCode);
        Assert.Contains("text/plain", head.Content.Headers.ContentType?.ToString() ?? "");
        Assert.True(head.Headers.TryGetValues("x-amz-meta-lang", out var langValues));
        Assert.Equal("pl", langValues!.Single());

        // GET returns the in-memory data.
        var get = await _client.SendAsync(new HttpRequestMessage(HttpMethod.Get, $"/{bucket}/{key}"));
        Assert.Equal(HttpStatusCode.OK, get.StatusCode);
        var received = await get.Content.ReadAsByteArrayAsync();
        Assert.Equal(payload, received);

        // DELETE removes both data and metadata.
        var del = await _client.SendAsync(new HttpRequestMessage(HttpMethod.Delete, $"/{bucket}/{key}"));
        Assert.Equal(HttpStatusCode.NoContent, del.StatusCode);
        Assert.False(File.Exists(expectedMetadataPath));
    }

    public void Dispose()
    {
        _client.Dispose();
        _factory.Dispose();
        try { Directory.Delete(_metadataDirectory, true); } catch { }
    }
}
