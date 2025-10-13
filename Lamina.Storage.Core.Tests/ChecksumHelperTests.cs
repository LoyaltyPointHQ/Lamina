using Lamina.Storage.Core.Helpers;
using Xunit;

namespace Lamina.Storage.Core.Tests;

public class ChecksumHelperTests
{
    [Fact]
    public void IsValidAlgorithm_ValidAlgorithms_ReturnsTrue()
    {
        // Arrange & Act & Assert
        Assert.True(ChecksumHelper.IsValidAlgorithm("CRC32"));
        Assert.True(ChecksumHelper.IsValidAlgorithm("CRC32C"));
        Assert.True(ChecksumHelper.IsValidAlgorithm("SHA1"));
        Assert.True(ChecksumHelper.IsValidAlgorithm("SHA256"));
        Assert.True(ChecksumHelper.IsValidAlgorithm("CRC64NVME"));
    }

    [Fact]
    public void IsValidAlgorithm_CaseInsensitive_ReturnsTrue()
    {
        // Arrange & Act & Assert
        Assert.True(ChecksumHelper.IsValidAlgorithm("crc32"));
        Assert.True(ChecksumHelper.IsValidAlgorithm("Crc32c"));
        Assert.True(ChecksumHelper.IsValidAlgorithm("sha1"));
        Assert.True(ChecksumHelper.IsValidAlgorithm("Sha256"));
        Assert.True(ChecksumHelper.IsValidAlgorithm("crc64nvme"));
    }

    [Fact]
    public void IsValidAlgorithm_InvalidAlgorithms_ReturnsFalse()
    {
        // Arrange & Act & Assert
        Assert.False(ChecksumHelper.IsValidAlgorithm("MD5"));
        Assert.False(ChecksumHelper.IsValidAlgorithm("SHA512"));
        Assert.False(ChecksumHelper.IsValidAlgorithm("INVALID"));
        Assert.False(ChecksumHelper.IsValidAlgorithm(""));
        Assert.False(ChecksumHelper.IsValidAlgorithm(null));
    }

    [Fact]
    public void CalculateCRC32_EmptyData_ReturnsBase64String()
    {
        // Arrange
        var data = Array.Empty<byte>();

        // Act
        var result = ChecksumHelper.CalculateCRC32(data);

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
        // CRC32 of empty data should be deterministic
        Assert.Equal("AAAAAA==", result);
    }

    [Fact]
    public void CalculateCRC32_SampleData_ReturnsConsistentChecksum()
    {
        // Arrange
        var data = "Hello, World!"u8.ToArray();

        // Act
        var result1 = ChecksumHelper.CalculateCRC32(data);
        var result2 = ChecksumHelper.CalculateCRC32(data);

        // Assert
        Assert.NotNull(result1);
        Assert.NotEmpty(result1);
        Assert.Equal(result1, result2); // Should be deterministic
    }

    [Fact]
    public void CalculateCRC32C_SampleData_ReturnsConsistentChecksum()
    {
        // Arrange
        var data = "Hello, World!"u8.ToArray();

        // Act
        var result1 = ChecksumHelper.CalculateCRC32C(data);
        var result2 = ChecksumHelper.CalculateCRC32C(data);

        // Assert
        Assert.NotNull(result1);
        Assert.NotEmpty(result1);
        Assert.Equal(result1, result2);
    }

    [Fact]
    public void CalculateSHA1_SampleData_ReturnsConsistentChecksum()
    {
        // Arrange
        var data = "Hello, World!"u8.ToArray();

        // Act
        var result1 = ChecksumHelper.CalculateSHA1(data);
        var result2 = ChecksumHelper.CalculateSHA1(data);

        // Assert
        Assert.NotNull(result1);
        Assert.NotEmpty(result1);
        Assert.Equal(result1, result2);
    }

    [Fact]
    public void CalculateSHA256_SampleData_ReturnsConsistentChecksum()
    {
        // Arrange
        var data = "Hello, World!"u8.ToArray();

        // Act
        var result1 = ChecksumHelper.CalculateSHA256(data);
        var result2 = ChecksumHelper.CalculateSHA256(data);

        // Assert
        Assert.NotNull(result1);
        Assert.NotEmpty(result1);
        Assert.Equal(result1, result2);
    }

    [Fact]
    public void CalculateCRC64NVME_SampleData_ReturnsConsistentChecksum()
    {
        // Arrange
        var data = "Hello, World!"u8.ToArray();

        // Act
        var result1 = ChecksumHelper.CalculateCRC64NVME(data);
        var result2 = ChecksumHelper.CalculateCRC64NVME(data);

        // Assert
        Assert.NotNull(result1);
        Assert.NotEmpty(result1);
        Assert.Equal(result1, result2);
    }

    [Theory]
    [InlineData("CRC32")]
    [InlineData("CRC32C")]
    [InlineData("SHA1")]
    [InlineData("SHA256")]
    [InlineData("CRC64NVME")]
    public void CalculateChecksum_ValidAlgorithm_ReturnsChecksum(string algorithm)
    {
        // Arrange
        var data = "Test data"u8.ToArray();

        // Act
        var result = ChecksumHelper.CalculateChecksum(data, algorithm);

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
    }

    [Fact]
    public void CalculateChecksum_InvalidAlgorithm_ReturnsNull()
    {
        // Arrange
        var data = "Test data"u8.ToArray();

        // Act
        var result = ChecksumHelper.CalculateChecksum(data, "INVALID");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void CalculateChecksum_NullAlgorithm_ReturnsNull()
    {
        // Arrange
        var data = "Test data"u8.ToArray();

        // Act
        var result = ChecksumHelper.CalculateChecksum(data, null);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void CalculateChecksum_EmptyAlgorithm_ReturnsNull()
    {
        // Arrange
        var data = "Test data"u8.ToArray();

        // Act
        var result = ChecksumHelper.CalculateChecksum(data, "");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void DifferentAlgorithms_ProduceDifferentChecksums()
    {
        // Arrange
        var data = "Test data"u8.ToArray();

        // Act
        var crc32 = ChecksumHelper.CalculateCRC32(data);
        var crc32c = ChecksumHelper.CalculateCRC32C(data);
        var sha1 = ChecksumHelper.CalculateSHA1(data);
        var sha256 = ChecksumHelper.CalculateSHA256(data);
        var crc64nvme = ChecksumHelper.CalculateCRC64NVME(data);

        // Assert
        Assert.NotEqual(crc32, crc32c);
        Assert.NotEqual(crc32, sha1);
        Assert.NotEqual(crc32, sha256);
        Assert.NotEqual(crc32c, sha1);
        Assert.NotEqual(sha1, sha256);
        // Note: We don't check crc64nvme equality as it might collide by chance
    }

    [Fact]
    public void DifferentData_ProducesDifferentChecksums()
    {
        // Arrange
        var data1 = "Test data 1"u8.ToArray();
        var data2 = "Test data 2"u8.ToArray();

        // Act
        var checksum1 = ChecksumHelper.CalculateCRC32(data1);
        var checksum2 = ChecksumHelper.CalculateCRC32(data2);

        // Assert
        Assert.NotEqual(checksum1, checksum2);
    }
}
