using Lamina.Storage.Core.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Lamina.WebApi.Tests.Configuration;

public class BucketDefaultsSettingsTests
{
    [Fact]
    public void BucketDefaultsSettings_InvalidType_ThrowsException()
    {
        var configurationData = new Dictionary<string, string?>
        {
            ["BucketDefaults:Type"] = "InvalidType"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configurationData)
            .Build();

        var settings = new BucketDefaultsSettings();

        // Invalid enum value should throw an exception
        var exception = Assert.Throws<InvalidOperationException>(() =>
            configuration.GetSection("BucketDefaults").Bind(settings));

        Assert.Contains("Failed to convert configuration value", exception.Message);
        // The specific inner exception message may vary, so just check for the basic error structure
    }
}