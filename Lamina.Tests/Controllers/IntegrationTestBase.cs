using Lamina.WebApi;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;

namespace Lamina.Tests.Controllers;

public abstract class IntegrationTestBase : IClassFixture<WebApplicationFactory<Program>>
{
    protected readonly WebApplicationFactory<Program> Factory;
    protected readonly HttpClient Client;

    protected IntegrationTestBase(WebApplicationFactory<Program> factory)
    {
        Factory = factory.WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Test");
            builder.ConfigureAppConfiguration((context, config) =>
            {
                config.Sources.Clear();
                var testProjectPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..");
                var testSettingsPath = Path.Combine(testProjectPath, "Lamina.Tests", "appsettings.Test.json");
                config.AddJsonFile(testSettingsPath, optional: false, reloadOnChange: false);
            });
        });
        Client = Factory.CreateClient();
    }
}