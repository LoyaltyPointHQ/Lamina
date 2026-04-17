using Lamina.Core.Models;
using Lamina.WebApi.Services;

namespace Lamina.WebApi.Tests;

public class LifecycleConfigurationParserTests
{
    private const string NS = "http://s3.amazonaws.com/doc/2006-03-01/";

    private static string Wrap(string rules) =>
        $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><LifecycleConfiguration xmlns=\"{NS}\">{rules}</LifecycleConfiguration>";

    [Fact]
    public void Parse_SimpleExpirationDays_Success()
    {
        var xml = Wrap("<Rule><ID>r1</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>7</Days></Expiration></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.True(result.IsSuccess);
        var cfg = result.Configuration!;
        Assert.Single(cfg.Rules);
        Assert.Equal("r1", cfg.Rules[0].Id);
        Assert.Equal(LifecycleRuleStatus.Enabled, cfg.Rules[0].Status);
        Assert.Equal("logs/", cfg.Rules[0].Filter?.Prefix);
        Assert.Equal(7, cfg.Rules[0].Expiration?.Days);
    }

    [Fact]
    public void Parse_LegacyRulePrefix_Success()
    {
        var xml = Wrap("<Rule><ID>r1</ID><Prefix>logs/</Prefix><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.True(result.IsSuccess);
        Assert.Equal("logs/", result.Configuration!.Rules[0].Prefix);
    }

    [Fact]
    public void Parse_ExpirationDate_Success()
    {
        var xml = Wrap("<Rule><ID>r1</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Expiration><Date>2030-01-01T00:00:00Z</Date></Expiration></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.True(result.IsSuccess);
        Assert.NotNull(result.Configuration!.Rules[0].Expiration?.Date);
    }

    [Fact]
    public void Parse_AbortIncompleteMultipartUpload_Success()
    {
        var xml = Wrap("<Rule><ID>mpu</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.True(result.IsSuccess);
        Assert.Equal(3, result.Configuration!.Rules[0].AbortIncompleteMultipartUpload?.DaysAfterInitiation);
    }

    [Fact]
    public void Parse_FilterWithAnd_PrefixPlusSize_Success()
    {
        var xml = Wrap("<Rule><ID>r1</ID><Filter><And><Prefix>docs/</Prefix><ObjectSizeGreaterThan>1024</ObjectSizeGreaterThan></And></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.True(result.IsSuccess);
        Assert.Equal("docs/", result.Configuration!.Rules[0].Filter?.And?.Prefix);
        Assert.Equal(1024, result.Configuration!.Rules[0].Filter?.And?.ObjectSizeGreaterThan);
    }

    [Fact]
    public void Parse_UnsupportedTransition_ReturnsNotImplemented()
    {
        var xml = Wrap("<Rule><ID>r1</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Transition><Days>30</Days><StorageClass>GLACIER</StorageClass></Transition></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.False(result.IsSuccess);
        Assert.True(result.IsNotImplemented);
        Assert.Contains("Transition", result.ErrorMessage!);
    }

    [Fact]
    public void Parse_UnsupportedNoncurrentVersionExpiration_ReturnsNotImplemented()
    {
        var xml = Wrap("<Rule><ID>r1</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>10</NoncurrentDays></NoncurrentVersionExpiration></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.False(result.IsSuccess);
        Assert.True(result.IsNotImplemented);
        Assert.Contains("Noncurrent", result.ErrorMessage!);
    }

    [Fact]
    public void Parse_ExpiredObjectDeleteMarker_ReturnsNotImplemented()
    {
        var xml = Wrap("<Rule><ID>r1</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.False(result.IsSuccess);
        Assert.True(result.IsNotImplemented);
    }

    [Fact]
    public void Parse_ValidationError_IsNotMarkedNotImplemented()
    {
        var xml = Wrap("<Rule><ID>r1</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Expiration><Days>0</Days></Expiration></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.False(result.IsSuccess);
        Assert.False(result.IsNotImplemented);
    }

    [Fact]
    public void Parse_ExpirationBothDaysAndDate_Returns400()
    {
        var xml = Wrap("<Rule><ID>r1</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Expiration><Days>7</Days><Date>2030-01-01T00:00:00Z</Date></Expiration></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.False(result.IsSuccess);
    }

    [Fact]
    public void Parse_DaysZero_Returns400()
    {
        var xml = Wrap("<Rule><ID>r1</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Expiration><Days>0</Days></Expiration></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.False(result.IsSuccess);
    }

    [Fact]
    public void Parse_MoreThan1000Rules_Returns400()
    {
        var rules = string.Join("", Enumerable.Range(1, 1001)
            .Select(i => $"<Rule><ID>r{i}</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>"));
        var xml = Wrap(rules);

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.False(result.IsSuccess);
        Assert.Contains("1000", result.ErrorMessage!);
    }

    [Fact]
    public void Parse_IdTooLong_Returns400()
    {
        var longId = new string('a', 256);
        var xml = Wrap($"<Rule><ID>{longId}</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.False(result.IsSuccess);
    }

    [Fact]
    public void Parse_InvalidStatus_Returns400()
    {
        var xml = Wrap("<Rule><ID>r1</ID><Filter><Prefix></Prefix></Filter><Status>Bogus</Status><Expiration><Days>1</Days></Expiration></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.False(result.IsSuccess);
    }

    [Fact]
    public void Parse_MalformedXml_Returns400()
    {
        var result = LifecycleConfigurationParser.Parse("not xml");

        Assert.False(result.IsSuccess);
    }

    [Fact]
    public void Parse_NoAction_Returns400()
    {
        var xml = Wrap("<Rule><ID>r1</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status></Rule>");

        var result = LifecycleConfigurationParser.Parse(xml);

        Assert.False(result.IsSuccess);
    }
}
