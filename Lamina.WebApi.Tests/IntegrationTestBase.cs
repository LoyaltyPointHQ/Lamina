using Lamina.Storage.Core.Abstract;
using Lamina.Storage.InMemory;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Lamina.WebApi.Tests.Controllers;

public abstract class IntegrationTestBase : IClassFixture<WebApplicationFactory<global::Program>>
{
    protected readonly WebApplicationFactory<global::Program> Factory;
    protected readonly HttpClient Client;

    protected IntegrationTestBase(WebApplicationFactory<global::Program> factory)
    {
        var testProjectPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..");
        var testSettingsPath = Path.Combine(testProjectPath, "Lamina.WebApi.Tests", "appsettings.Test.json");

        Factory = factory.WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Test");
            builder.ConfigureAppConfiguration((context, config) =>
            {
                config.Sources.Clear();
                config.AddJsonFile(testSettingsPath, optional: false, reloadOnChange: false);
            });
            // Override metadata storage to Singleton InMemory instances.
            // Program.cs reads StorageType from config which may register Filesystem.
            // Note: Data storage (IObjectDataStorage, IBucketDataStorage, IMultipartUploadDataStorage)
            // is NOT overridden - tests use whatever Program.cs configures (Filesystem by default).
            builder.ConfigureServices(services =>
            {
                services.RemoveAll<IObjectMetadataStorage>();
                services.AddSingleton<IObjectMetadataStorage, InMemoryObjectMetadataStorage>();
                services.RemoveAll<IBucketMetadataStorage>();
                services.AddSingleton<IBucketMetadataStorage, InMemoryBucketMetadataStorage>();
                services.RemoveAll<IMultipartUploadMetadataStorage>();
                services.AddSingleton<IMultipartUploadMetadataStorage, InMemoryMultipartUploadMetadataStorage>();
            });
        });
        Client = Factory.CreateClient();
    }
}