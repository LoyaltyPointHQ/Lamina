using Lamina.Configuration;
using Lamina.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Lamina.Tests.Configuration;

public class BucketDefaultsSettingsTests
{
    [Fact]
    public void BucketDefaultsSettings_DefaultValues_AreCorrect()
    {
        var settings = new BucketDefaultsSettings();

        Assert.Equal(BucketType.GeneralPurpose, settings.Type);
        Assert.Null(settings.StorageClass);
        Assert.Equal("us-east-1", settings.Region);
    }

    [Fact]
    public void BucketDefaultsSettings_FromConfiguration_BindsCorrectly()
    {
        var configurationData = new Dictionary<string, string?>
        {
            ["BucketDefaults:Type"] = "Directory",
            ["BucketDefaults:StorageClass"] = "EXPRESS_ONEZONE",
            ["BucketDefaults:Region"] = "us-west-2"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configurationData)
            .Build();

        var settings = new BucketDefaultsSettings();
        configuration.GetSection("BucketDefaults").Bind(settings);

        Assert.Equal(BucketType.Directory, settings.Type);
        Assert.Equal("EXPRESS_ONEZONE", settings.StorageClass);
        Assert.Equal("us-west-2", settings.Region);
    }

    [Fact]
    public void BucketDefaultsSettings_InvalidType_ThrowsException()
    {
        var configurationData = new Dictionary<string, string?>
        {
            ["BucketDefaults:Type"] = "InvalidType",
            ["BucketDefaults:Region"] = "us-west-2"
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