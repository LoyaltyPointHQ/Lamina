using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.Core.Tests;

public class BucketNameValidatorTests
{
    [Theory]
    [InlineData("my-bucket")]
    [InlineData("my.bucket")]
    [InlineData("bucket123")]
    [InlineData("abc")]
    [InlineData("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")] // 63 chars
    public void IsValid_ValidBucketNames_ReturnsTrue(string name)
    {
        Assert.True(BucketNameValidator.IsValid(name));
    }

    [Theory]
    [InlineData("ab")]
    [InlineData("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")] // 64 chars
    [InlineData("MyBucket")]
    [InlineData("my_bucket")]
    [InlineData("-mybucket")]
    [InlineData("mybucket-")]
    [InlineData(".mybucket")]
    [InlineData("mybucket.")]
    [InlineData("my..bucket")]
    [InlineData("my.-bucket")]
    [InlineData("my-.bucket")]
    [InlineData("192.168.1.1")]
    [InlineData("")]
    public void IsValid_InvalidBucketNames_ReturnsFalse(string name)
    {
        Assert.False(BucketNameValidator.IsValid(name));
    }

    [Theory]
    [InlineData("xn--bucket")]
    [InlineData("sthree-bucket")]
    [InlineData("amzn-s3-demo-bucket")]
    public void IsValid_ReservedPrefixes_ReturnsFalse(string name)
    {
        Assert.False(BucketNameValidator.IsValid(name));
    }

    [Theory]
    [InlineData("my-bucket-s3alias")]
    [InlineData("bucket--ol-s3")]
    [InlineData("my-bucket.mrap")]
    [InlineData("bucket--x-s3")]
    [InlineData("my-bucket--table-s3")]
    public void IsValid_ReservedSuffixes_ReturnsFalse(string name)
    {
        Assert.False(BucketNameValidator.IsValid(name));
    }
}
