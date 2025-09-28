using Lamina.Storage.Core.Helpers;
using Xunit;

namespace Lamina.WebApi.Tests.Helpers;

public class S3UrlEncoderTests
{
    [Theory]
    [InlineData("simple.txt", "simple.txt")]
    [InlineData("file with spaces.txt", "file%20with%20spaces.txt")]
    [InlineData("file[brackets].txt", "file%5Bbrackets%5D.txt")]
    [InlineData("file(parentheses).txt", "file%28parentheses%29.txt")]
    [InlineData("file%percent.txt", "file%25percent.txt")]
    [InlineData("ąćęłńóśźż.txt", "%C4%85%C4%87%C4%99%C5%82%C5%84%C3%B3%C5%9B%C5%BA%C5%BC.txt")]
    [InlineData("文件.txt", "%E6%96%87%E4%BB%B6.txt")]
    [InlineData("file&ampersand.txt", "file%26ampersand.txt")]
    [InlineData("file=equals.txt", "file%3Dequals.txt")]
    [InlineData("file+plus.txt", "file%2Bplus.txt")]
    [InlineData("file#hash.txt", "file%23hash.txt")]
    [InlineData("file?question.txt", "file%3Fquestion.txt")]
    public void Encode_EncodesSpecialCharactersCorrectly(string input, string expected)
    {
        // Act
        var result = S3UrlEncoder.Encode(input);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Encode_WithNullInput_ReturnsNull()
    {
        // Act
        var result = S3UrlEncoder.Encode(null);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void Encode_WithEmptyString_ReturnsEmptyString()
    {
        // Act
        var result = S3UrlEncoder.Encode("");

        // Assert
        Assert.Equal("", result);
    }

    [Theory]
    [InlineData("test.txt", "url", "test.txt")]
    [InlineData("test file.txt", "url", "test%20file.txt")]
    [InlineData("test file.txt", "URL", "test%20file.txt")] // Case insensitive
    [InlineData("test file.txt", "", "test file.txt")] // Empty encoding type
    [InlineData("test file.txt", "base64", "test file.txt")] // Invalid encoding type
    public void ConditionalEncode_EncodesOnlyWhenEncodingTypeIsUrl(string input, string encodingType, string expected)
    {
        // Act
        var result = S3UrlEncoder.ConditionalEncode(input, encodingType);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void ConditionalEncode_WithNullInput_ReturnsNull()
    {
        // Act
        var result = S3UrlEncoder.ConditionalEncode(null, "url");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void ConditionalEncode_WithNullEncodingType_ReturnsOriginal()
    {
        // Act
        var result = S3UrlEncoder.ConditionalEncode("test file.txt", null);

        // Assert
        Assert.Equal("test file.txt", result);
    }

    [Theory]
    [InlineData("folder/", "folder%2F")]
    [InlineData("folder with spaces/", "folder%20with%20spaces%2F")]
    [InlineData("folder[brackets]/", "folder%5Bbrackets%5D%2F")]
    [InlineData("/absolute/path/", "%2Fabsolute%2Fpath%2F")]
    public void Encode_HandlesPathsCorrectly(string input, string expected)
    {
        // Act
        var result = S3UrlEncoder.Encode(input);

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("already%20encoded.txt", "already%2520encoded.txt")] // Double encoding
    [InlineData("file%2Bwith%2Bencoded%2Bplus.txt", "file%252Bwith%252Bencoded%252Bplus.txt")]
    public void Encode_HandlesAlreadyEncodedStrings(string input, string expected)
    {
        // Act
        var result = S3UrlEncoder.Encode(input);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Encode_PreservesUnreservedCharacters()
    {
        // Unreserved characters per RFC 3986: A-Z a-z 0-9 - _ . ~
        var unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~";

        // Act
        var result = S3UrlEncoder.Encode(unreserved);

        // Assert
        Assert.Equal(unreserved, result);
    }

    [Theory]
    [InlineData("test\nfile.txt", "test%0Afile.txt")] // Newline
    [InlineData("test\rfile.txt", "test%0Dfile.txt")] // Carriage return
    [InlineData("test\tfile.txt", "test%09file.txt")] // Tab
    public void Encode_HandlesControlCharacters(string input, string expected)
    {
        // Act
        var result = S3UrlEncoder.Encode(input);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Encode_HandlesLongStrings()
    {
        // Arrange
        var longString = new string('a', 1000) + " " + new string('b', 1000);
        var expectedEncoded = new string('a', 1000) + "%20" + new string('b', 1000);

        // Act
        var result = S3UrlEncoder.Encode(longString);

        // Assert
        Assert.Equal(expectedEncoded, result);
    }
}