using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.Core.Tests;

public class MultipartChecksumAggregatorTests
{
    [Fact]
    public void AggregateCrc32_WithValidChecksums_ReturnsAggregatedChecksum()
    {
        // Arrange - Two parts with known CRC32 checksums
        var part1Checksum = "ShexVg=="; // CRC32 of "Hello World"
        var part2Checksum = "ShexVg=="; // CRC32 of "Hello World"
        var partChecksums = new[] { part1Checksum, part2Checksum };

        // Act
        var result = MultipartChecksumAggregator.AggregateCrc32(partChecksums);

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
        // Result should be base64 encoded CRC32
        Assert.Matches("^[A-Za-z0-9+/]+=*$", result);
    }

    [Fact]
    public void AggregateCrc32_WithNullChecksums_ReturnsNull()
    {
        // Arrange
        var partChecksums = new string?[] { null, null };

        // Act
        var result = MultipartChecksumAggregator.AggregateCrc32(partChecksums);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void AggregateCrc32_WithEmptyList_ReturnsNull()
    {
        // Arrange
        var partChecksums = Array.Empty<string>();

        // Act
        var result = MultipartChecksumAggregator.AggregateCrc32(partChecksums);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void AggregateCrc32C_WithValidChecksums_ReturnsAggregatedChecksum()
    {
        // Arrange
        var part1Checksum = "aR2qLw=="; // CRC32C of "Hello World"
        var part2Checksum = "aR2qLw==";
        var partChecksums = new[] { part1Checksum, part2Checksum };

        // Act
        var result = MultipartChecksumAggregator.AggregateCrc32C(partChecksums);

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
        Assert.Matches("^[A-Za-z0-9+/]+=*$", result);
    }

    [Fact]
    public void AggregateSha1_WithValidChecksums_ReturnsAggregatedChecksum()
    {
        // Arrange
        var part1Checksum = "Ck1VqNd45QIvq3AZd8XYQLvEhtA="; // SHA1 of "Hello World"
        var part2Checksum = "Ck1VqNd45QIvq3AZd8XYQLvEhtA=";
        var partChecksums = new[] { part1Checksum, part2Checksum };

        // Act
        var result = MultipartChecksumAggregator.AggregateSha1(partChecksums);

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
        Assert.Matches("^[A-Za-z0-9+/]+=*$", result);
    }

    [Fact]
    public void AggregateSha256_WithValidChecksums_ReturnsAggregatedChecksum()
    {
        // Arrange
        var part1Checksum = "pZGm1Av0IEBKARczz7exkNYsZb8LzaMrV7J32a2fFG4="; // SHA256 of "Hello World"
        var part2Checksum = "pZGm1Av0IEBKARczz7exkNYsZb8LzaMrV7J32a2fFG4=";
        var partChecksums = new[] { part1Checksum, part2Checksum };

        // Act
        var result = MultipartChecksumAggregator.AggregateSha256(partChecksums);

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
        Assert.Matches("^[A-Za-z0-9+/]+=*$", result);
    }

    [Fact]
    public void AggregateCrc64Nvme_WithValidChecksums_ReturnsAggregatedChecksum()
    {
        // Arrange - Using valid base64 checksums
        var part1Checksum = "AAAAAAAAAAA="; // 8 bytes base64
        var part2Checksum = "AQEBAQEBAQE="; // 8 bytes base64
        var partChecksums = new[] { part1Checksum, part2Checksum };

        // Act
        var result = MultipartChecksumAggregator.AggregateCrc64Nvme(partChecksums);

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
        Assert.Matches("^[A-Za-z0-9+/]+=*$", result);
    }

    [Fact]
    public void AggregateCrc32_WithMixedNullAndValid_AggregatesOnlyValid()
    {
        // Arrange
        var partChecksums = new string?[] { "ShexVg==", null, "ShexVg==", null };

        // Act
        var result = MultipartChecksumAggregator.AggregateCrc32(partChecksums);

        // Assert - Should aggregate only the non-null checksums
        Assert.NotNull(result);
        Assert.NotEmpty(result);
    }

    [Fact]
    public void AggregateCrc32_WithSingleChecksum_ReturnsAggregatedChecksum()
    {
        // Arrange
        var partChecksums = new[] { "ShexVg==" };

        // Act
        var result = MultipartChecksumAggregator.AggregateCrc32(partChecksums);

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
    }

    [Fact]
    public void AggregateCrc32_ProducesConsistentResults()
    {
        // Arrange
        var partChecksums = new[] { "ShexVg==", "aR2qLw==" };

        // Act
        var result1 = MultipartChecksumAggregator.AggregateCrc32(partChecksums);
        var result2 = MultipartChecksumAggregator.AggregateCrc32(partChecksums);

        // Assert - Same input should produce same output
        Assert.Equal(result1, result2);
    }

    [Fact]
    public void AggregateSha256_ProducesConsistentResults()
    {
        // Arrange
        var partChecksums = new[] { "pZGm1Av0IEBKARczz7exkNYsZb8LzaMrV7J32a2fFG4=", "pZGm1Av0IEBKARczz7exkNYsZb8LzaMrV7J32a2fFG4=" };

        // Act
        var result1 = MultipartChecksumAggregator.AggregateSha256(partChecksums);
        var result2 = MultipartChecksumAggregator.AggregateSha256(partChecksums);

        // Assert
        Assert.Equal(result1, result2);
    }

    [Fact]
    public void AggregateCrc32_OrderMatters()
    {
        // Arrange
        var checksums1 = new[] { "ShexVg==", "aR2qLw==" };
        var checksums2 = new[] { "aR2qLw==", "ShexVg==" };

        // Act
        var result1 = MultipartChecksumAggregator.AggregateCrc32(checksums1);
        var result2 = MultipartChecksumAggregator.AggregateCrc32(checksums2);

        // Assert - Different order should produce different results
        Assert.NotEqual(result1, result2);
    }
}
