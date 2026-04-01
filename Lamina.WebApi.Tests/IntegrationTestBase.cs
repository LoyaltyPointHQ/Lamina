using Lamina.Storage.Core.Abstract;
using Lamina.Storage.InMemory;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

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
            // Program.cs reads StorageType from config before ConfigureAppConfiguration runs,
            // so it may register Filesystem metadata instead of InMemory.
            builder.ConfigureServices(services =>
            {
                services.AddSingleton<IObjectMetadataStorage, InMemoryObjectMetadataStorage>();
                services.AddSingleton<IBucketMetadataStorage, InMemoryBucketMetadataStorage>();
                services.AddSingleton<IMultipartUploadMetadataStorage, InMemoryMultipartUploadMetadataStorage>();
            });
        });
        Client = Factory.CreateClient();
    }
}