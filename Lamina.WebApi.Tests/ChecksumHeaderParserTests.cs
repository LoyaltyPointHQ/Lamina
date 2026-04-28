using Lamina.WebApi.Helpers;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace Lamina.WebApi.Tests;

public class ChecksumHeaderParserTests
{
    private static IHeaderDictionary CreateHeaders(params (string key, string value)[] headers)
    {
        var dict = new HeaderDictionary();
        foreach (var (key, value) in headers)
        {
            dict[key] = new StringValues(value);
        }
        return dict;
    }

    [Fact]
    public void Parse_NoHeaders_ReturnsEmptyResult()
    {
        var headers = CreateHeaders();

        var result = ChecksumHeaderParser.Parse(headers);

        Assert.True(result.IsValid);
        Assert.Null(result.Algorithm);
        Assert.Null(result.CRC32);
        Assert.Null(result.CRC32C);
        Assert.Null(result.CRC64NVME);
        Assert.Null(result.SHA1);
        Assert.Null(result.SHA256);
    }

    [Fact]
    public void Parse_WithAlgorithmOnly_ReturnsAlgorithm()
    {
        var headers = CreateHeaders(("x-amz-checksum-algorithm", "CRC32"));

        var result = ChecksumHeaderParser.Parse(headers);

        Assert.True(result.IsValid);
        Assert.Equal("CRC32", result.Algorithm);
    }

    [Fact]
    public void Parse_InvalidAlgorithm_ReturnsError()
    {
        var headers = CreateHeaders(("x-amz-checksum-algorithm", "INVALID"));

        var result = ChecksumHeaderParser.Parse(headers);

        Assert.False(result.IsValid);
        Assert.Equal("InvalidArgument", result.ErrorCode);
        Assert.Contains("INVALID", result.ErrorMessage);
    }

    [Fact]
    public void Parse_WithCRC32_ReturnsCRC32()
    {
        var headers = CreateHeaders(("x-amz-checksum-crc32", "AAAAAA=="));

        var result = ChecksumHeaderParser.Parse(headers);

        Assert.True(result.IsValid);
        Assert.Equal("AAAAAA==", result.CRC32);
    }

    [Fact]
    public void Parse_WithCRC32C_ReturnsCRC32C()
    {
        var headers = CreateHeaders(("x-amz-checksum-crc32c", "BBBBBB=="));

        var result = ChecksumHeaderParser.Parse(headers);

        Assert.True(result.IsValid);
        Assert.Equal("BBBBBB==", result.CRC32C);
    }

    [Fact]
    public void Parse_WithCRC64NVME_ReturnsCRC64NVME()
    {
        var headers = CreateHeaders(("x-amz-checksum-crc64nvme", "CCCCCCCC"));

        var result = ChecksumHeaderParser.Parse(headers);

        Assert.True(result.IsValid);
        Assert.Equal("CCCCCCCC", result.CRC64NVME);
    }

    [Fact]
    public void Parse_WithSHA1_ReturnsSHA1()
    {
        var headers = CreateHeaders(("x-amz-checksum-sha1", "DDDDDDDD"));

        var result = ChecksumHeaderParser.Parse(headers);

        Assert.True(result.IsValid);
        Assert.Equal("DDDDDDDD", result.SHA1);
    }

    [Fact]
    public void Parse_WithSHA256_ReturnsSHA256()
    {
        var headers = CreateHeaders(("x-amz-checksum-sha256", "EEEEEEEE"));

        var result = ChecksumHeaderParser.Parse(headers);

        Assert.True(result.IsValid);
        Assert.Equal("EEEEEEEE", result.SHA256);
    }

    [Fact]
    public void Parse_MultipleChecksums_ReturnsAll()
    {
        var headers = CreateHeaders(
            ("x-amz-checksum-algorithm", "SHA256"),
            ("x-amz-checksum-crc32", "CRC32VAL"),
            ("x-amz-checksum-sha256", "SHA256VAL"));

        var result = ChecksumHeaderParser.Parse(headers);

        Assert.True(result.IsValid);
        Assert.Equal("SHA256", result.Algorithm);
        Assert.Equal("CRC32VAL", result.CRC32);
        Assert.Equal("SHA256VAL", result.SHA256);
    }

    [Fact]
    public void Parse_EmptyValues_AreIgnored()
    {
        var headers = CreateHeaders(
            ("x-amz-checksum-crc32", ""),
            ("x-amz-checksum-sha256", "VALIDVALUE"));

        var result = ChecksumHeaderParser.Parse(headers);

        Assert.True(result.IsValid);
        Assert.Null(result.CRC32);
        Assert.Equal("VALIDVALUE", result.SHA256);
    }

    [Theory]
    [InlineData("CRC32")]
    [InlineData("CRC32C")]
    [InlineData("CRC64NVME")]
    [InlineData("SHA1")]
    [InlineData("SHA256")]
    [InlineData("crc32")]
    [InlineData("sha256")]
    public void Parse_ValidAlgorithms_AreAccepted(string algorithm)
    {
        var headers = CreateHeaders(("x-amz-checksum-algorithm", algorithm));

        var result = ChecksumHeaderParser.Parse(headers);

        Assert.True(result.IsValid);
        Assert.Equal(algorithm, result.Algorithm);
    }
}
