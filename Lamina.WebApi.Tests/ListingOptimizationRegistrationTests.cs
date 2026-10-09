using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Integrity;
using Lamina.Storage.Filesystem;
using Lamina.WebApi.Services;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Lamina.WebApi.Tests;

public class ListingOptimizationRegistrationTests
{
    [Theory]
    [InlineData(null, true)]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public async Task ListingIndex_RespectsDefaultAndOverride_AndReadGeneratedMetadataIsPersisted(
        string? indexEnabled, bool expectedIndexEnabled)
    {
        var root = Path.Combine(Path.GetTempPath(), "lamina-listing-registration-" + Guid.NewGuid().ToString("N"));
        try
        {
            await using var factory = new WebApplicationFactory<global::Program>().WithWebHostBuilder(builder =>
            {
                builder.UseEnvironment("Test");
                builder.ConfigureAppConfiguration((_, config) =>
                {
                    config.Sources.Clear();
                    var settings = new Dictionary<string, string?>
                    {
                        ["StorageType"] = "Filesystem",
                        ["MetadataStorageType"] = "Filesystem",
                        ["FilesystemStorage:DataDirectory"] = Path.Combine(root, "data"),
                        ["FilesystemStorage:MetadataDirectory"] = Path.Combine(root, "metadata"),
                        ["FilesystemStorage:MetadataMode"] = "SeparateDirectory",
                        ["IntegrityPersistence:Enabled"] = "true",
                        ["LockManager"] = "InMemory",
                        ["Authentication:Enabled"] = "false",
                        ["MultipartUploadCleanup:Enabled"] = "false",
                        ["MetadataCleanup:Enabled"] = "false",
                        ["TempFileCleanup:Enabled"] = "false",
                        ["LifecycleExpiration:Enabled"] = "false"
                    };
                    if (indexEnabled != null)
                        settings["ListingIndex:Enabled"] = indexEnabled;
                    config.AddInMemoryCollection(settings);
                });
            });
            using var client = factory.CreateClient();
            Assert.Equal(expectedIndexEnabled, factory.Services.GetRequiredService<FilesystemListingIndex>().Enabled);
            Assert.True(factory.Services.GetRequiredService<IntegrityPersistenceQueue>().Enabled);
            (await client.PutAsync("/test-bucket", null)).EnsureSuccessStatusCode();
            await File.WriteAllTextAsync(Path.Combine(root, "data", "test-bucket", "external.txt"), "payload");
            using var scope = factory.Services.CreateScope();
            var objects = scope.ServiceProvider.GetRequiredService<IObjectStorageFacade>();
            var info = await objects.GetObjectInfoAsync("test-bucket", "external.txt");
            Assert.NotNull(info);
            var service = factory.Services.GetServices<IHostedService>().OfType<IntegrityPersistenceService>().Single();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await service.StopAsync(timeout.Token);
            var persisted = await scope.ServiceProvider.GetRequiredService<IObjectMetadataStorage>()
                .GetMetadataAsync("test-bucket", "external.txt");
            Assert.NotNull(persisted);
            Assert.Equal(info.ETag, persisted.Metadata.ETag);
            Assert.Equal("text/plain", persisted.Metadata.ContentType);
        }
        finally { if (Directory.Exists(root)) Directory.Delete(root, true); }
    }
}
