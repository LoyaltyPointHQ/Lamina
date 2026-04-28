using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.Core.Tests.Helpers;

public class ContinuationTokenTests
{
    [Fact]
    public void Encode_ReturnsVersionedBase64()
    {
        var key = "folder/file.txt";

        var token = ContinuationToken.Encode(key);

        Assert.StartsWith("v1:", token);
        Assert.NotEqual(key, token);
    }

    [Fact]
    public void Decode_ExtractsKey()
    {
        var key = "folder/file.txt";
        var token = ContinuationToken.Encode(key);

        var decoded = ContinuationToken.Decode(token);

        Assert.Equal(key, decoded);
    }

    [Fact]
    public void Decode_InvalidToken_ReturnsNull()
    {
        var result = ContinuationToken.Decode("not-a-valid-token");

        Assert.Null(result);
    }

    [Fact]
    public void Decode_NullOrEmpty_ReturnsNull()
    {
        Assert.Null(ContinuationToken.Decode(null));
        Assert.Null(ContinuationToken.Decode(""));
    }

    [Fact]
    public void Decode_MalformedBase64_ReturnsNull()
    {
        var result = ContinuationToken.Decode("v1:not-valid-base64!!!");

        Assert.Null(result);
    }

    [Fact]
    public void Encode_EmptyKey_ReturnsEmptyString()
    {
        var result = ContinuationToken.Encode("");

        Assert.Equal("", result);
    }

    [Theory]
    [InlineData("simple.txt")]
    [InlineData("folder/subfolder/file.txt")]
    [InlineData("file with spaces.txt")]
    [InlineData("unicode-ąęść.txt")]
    [InlineData("special!@#$%^&*().txt")]
    public void RoundTrip_PreservesKey(string key)
    {
        var token = ContinuationToken.Encode(key);
        var decoded = ContinuationToken.Decode(token);

        Assert.Equal(key, decoded);
    }

    [Fact]
    public void IsOpaqueToken_ValidToken_ReturnsTrue()
    {
        var token = ContinuationToken.Encode("test");

        Assert.True(ContinuationToken.IsOpaqueToken(token));
    }

    [Fact]
    public void IsOpaqueToken_RawKey_ReturnsFalse()
    {
        Assert.False(ContinuationToken.IsOpaqueToken("folder/file.txt"));
        Assert.False(ContinuationToken.IsOpaqueToken(null));
        Assert.False(ContinuationToken.IsOpaqueToken(""));
    }
}
