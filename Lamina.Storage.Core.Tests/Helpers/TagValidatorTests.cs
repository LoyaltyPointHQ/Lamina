using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.Core.Tests.Helpers;

public class TagValidatorTests
{
    [Fact]
    public void Validate_EmptyDictionary_ReturnsValid()
    {
        var tags = new Dictionary<string, string>();
        var result = TagValidator.Validate(tags);

        Assert.True(result.IsValid);
        Assert.Null(result.ErrorMessage);
    }

    [Fact]
    public void Validate_TenTags_ReturnsValid()
    {
        var tags = Enumerable.Range(1, 10)
            .ToDictionary(i => $"key{i}", i => $"value{i}");

        var result = TagValidator.Validate(tags);

        Assert.True(result.IsValid);
    }

    [Fact]
    public void Validate_ElevenTags_ReturnsInvalid()
    {
        var tags = Enumerable.Range(1, 11)
            .ToDictionary(i => $"key{i}", i => $"value{i}");

        var result = TagValidator.Validate(tags);

        Assert.False(result.IsValid);
        Assert.Contains("10", result.ErrorMessage!);
    }

    [Fact]
    public void Validate_Key128Chars_ReturnsValid()
    {
        var tags = new Dictionary<string, string> { { new string('a', 128), "value" } };

        var result = TagValidator.Validate(tags);

        Assert.True(result.IsValid);
    }

    [Fact]
    public void Validate_Key129Chars_ReturnsInvalid()
    {
        var tags = new Dictionary<string, string> { { new string('a', 129), "value" } };

        var result = TagValidator.Validate(tags);

        Assert.False(result.IsValid);
        Assert.Contains("128", result.ErrorMessage!);
    }

    [Fact]
    public void Validate_Value256Chars_ReturnsValid()
    {
        var tags = new Dictionary<string, string> { { "key", new string('a', 256) } };

        var result = TagValidator.Validate(tags);

        Assert.True(result.IsValid);
    }

    [Fact]
    public void Validate_Value257Chars_ReturnsInvalid()
    {
        var tags = new Dictionary<string, string> { { "key", new string('a', 257) } };

        var result = TagValidator.Validate(tags);

        Assert.False(result.IsValid);
        Assert.Contains("256", result.ErrorMessage!);
    }

    [Fact]
    public void Validate_EmptyKey_ReturnsInvalid()
    {
        var tags = new Dictionary<string, string> { { "", "value" } };

        var result = TagValidator.Validate(tags);

        Assert.False(result.IsValid);
    }

    [Fact]
    public void Validate_EmptyValue_ReturnsValid()
    {
        // S3 allows empty tag values
        var tags = new Dictionary<string, string> { { "key", "" } };

        var result = TagValidator.Validate(tags);

        Assert.True(result.IsValid);
    }

    [Fact]
    public void Validate_NullDictionary_ReturnsValid()
    {
        var result = TagValidator.Validate(null);

        Assert.True(result.IsValid);
    }
}
