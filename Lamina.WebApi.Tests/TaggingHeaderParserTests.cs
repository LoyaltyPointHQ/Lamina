using Lamina.WebApi.Services;

namespace Lamina.WebApi.Tests;

public class TaggingHeaderParserTests
{
    [Fact]
    public void Parse_EmptyString_ReturnsEmptyDictionary()
    {
        var result = TaggingHeaderParser.Parse("");

        Assert.True(result.IsSuccess);
        Assert.Empty(result.Tags!);
    }

    [Fact]
    public void Parse_SingleTag_ReturnsDictionary()
    {
        var result = TaggingHeaderParser.Parse("key=value");

        Assert.True(result.IsSuccess);
        Assert.Single(result.Tags!);
        Assert.Equal("value", result.Tags!["key"]);
    }

    [Fact]
    public void Parse_MultipleTags_ReturnsDictionary()
    {
        var result = TaggingHeaderParser.Parse("k1=v1&k2=v2&k3=v3");

        Assert.True(result.IsSuccess);
        Assert.Equal(3, result.Tags!.Count);
        Assert.Equal("v1", result.Tags!["k1"]);
        Assert.Equal("v2", result.Tags!["k2"]);
        Assert.Equal("v3", result.Tags!["k3"]);
    }

    [Fact]
    public void Parse_UrlEncodedValue_DecodesCorrectly()
    {
        var result = TaggingHeaderParser.Parse("key=my%20value");

        Assert.True(result.IsSuccess);
        Assert.Equal("my value", result.Tags!["key"]);
    }

    [Fact]
    public void Parse_UrlEncodedKey_DecodesCorrectly()
    {
        var result = TaggingHeaderParser.Parse("my%20key=value");

        Assert.True(result.IsSuccess);
        Assert.Equal("value", result.Tags!["my key"]);
    }

    [Fact]
    public void Parse_EmptyValue_AllowsIt()
    {
        var result = TaggingHeaderParser.Parse("key=");

        Assert.True(result.IsSuccess);
        Assert.Equal("", result.Tags!["key"]);
    }

    [Fact]
    public void Parse_MalformedNoEquals_ReturnsError()
    {
        var result = TaggingHeaderParser.Parse("keyvalue");

        Assert.False(result.IsSuccess);
    }

    [Fact]
    public void Parse_DuplicateKeys_ReturnsError()
    {
        var result = TaggingHeaderParser.Parse("key=v1&key=v2");

        Assert.False(result.IsSuccess);
    }

    [Fact]
    public void Serialize_EmptyDictionary_ReturnsEmptyString()
    {
        var result = TaggingHeaderParser.Serialize(new Dictionary<string, string>());

        Assert.Equal("", result);
    }

    [Fact]
    public void Serialize_SingleTag_ReturnsEncodedPair()
    {
        var result = TaggingHeaderParser.Serialize(new Dictionary<string, string> { { "key", "value" } });

        Assert.Equal("key=value", result);
    }

    [Fact]
    public void Serialize_SpaceInValue_UrlEncoded()
    {
        var result = TaggingHeaderParser.Serialize(new Dictionary<string, string> { { "key", "my value" } });

        Assert.Equal("key=my%20value", result);
    }

    [Fact]
    public void RoundTrip_ParseThenSerialize_Preserves()
    {
        var original = new Dictionary<string, string>
        {
            { "env", "prod" },
            { "owner", "tomek" },
            { "project", "laminaa" }
        };

        var serialized = TaggingHeaderParser.Serialize(original);
        var parsed = TaggingHeaderParser.Parse(serialized);

        Assert.True(parsed.IsSuccess);
        Assert.Equal(original, parsed.Tags);
    }
}
