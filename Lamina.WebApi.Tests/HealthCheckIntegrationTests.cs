using System.Net;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace Lamina.WebApi.Tests.Controllers;

public class HealthCheckIntegrationTests : IntegrationTestBase
{
    public HealthCheckIntegrationTests(WebApplicationFactory<global::Program> factory) : base(factory)
    {
    }

    [Fact]
    public async Task HealthCheck_WithAuthenticationDisabled_Returns200()
    {
        // Arrange - authentication is disabled in appsettings.Test.json
        
        // Act
        var response = await Client.GetAsync("/health");

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        
        var content = await response.Content.ReadAsStringAsync();
        Assert.Equal("Healthy", content);
    }

    [Fact]
    public async Task HealthCheck_WithAuthenticationEnabled_Returns200()
    {
        // Arrange - create a client with authentication enabled
        var clientWithAuth = Factory.WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Test");
            builder.ConfigureAppConfiguration((context, config) =>
            {
                config.Sources.Clear();
                var testProjectPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..");
                var testSettingsPath = Path.Combine(testProjectPath, "Lamina.WebApi.Tests", "appsettings.Test.json");
                config.AddJsonFile(testSettingsPath, optional: false, reloadOnChange: false);
                
                // Override authentication to be enabled
                config.AddInMemoryCollection(new[]
                {
                    new KeyValuePair<string, string?>("Authentication:Enabled", "true")
                });
            });
        }).CreateClient();

        // Act
        var response = await clientWithAuth.GetAsync("/health");

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        
        var content = await response.Content.ReadAsStringAsync();
        Assert.Equal("Healthy", content);
    }

    [Fact]
    public async Task HealthCheck_ReturnsCorrectContentType()
    {
        // Act
        var response = await Client.GetAsync("/health");

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/plain", response.Content.Headers.ContentType?.MediaType);
    }
}