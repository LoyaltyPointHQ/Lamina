using Lamina.Core.Models;
using Lamina.Storage.Core.Helpers;
using Xunit;

namespace Lamina.Storage.Core.Tests;

public class TrailerChecksumMergerTests
{
    [Theory]
    [InlineData("x-amz-checksum-crc32", "CRC32")]
    [InlineData("X-Amz-Checksum-CRC32C", "CRC32C")]
    [InlineData("x-amz-checksum-crc64nvme", "CRC64NVME")]
    [InlineData("x-amz-checksum-sha1", "SHA1")]
    [InlineData("x-amz-checksum-sha256", "SHA256")]
    [InlineData("  x-amz-checksum-crc64nvme  ", "CRC64NVME")]
    public void MapTrailerNameToAlgorithm_RecognisesSupportedTrailers(string trailerName, string expected)
    {
        Assert.Equal(expected, TrailerChecksumMerger.MapTrailerNameToAlgorithm(trailerName));
    }

    [Theory]
    [InlineData("x-amz-checksum-unknown")]
    [InlineData("content-md5")]
    [InlineData("")]
    public void MapTrailerNameToAlgorithm_ReturnsNullForUnsupportedNames(string trailerName)
    {
        Assert.Null(TrailerChecksumMerger.MapTrailerNameToAlgorithm(trailerName));
    }

    [Fact]
    public void RegisterExpectedTrailers_AddsPlaceholdersForKnownTrailerNames()
    {
        var request = TrailerChecksumMerger.RegisterExpectedTrailers(
            new[] { "x-amz-checksum-crc64nvme", "x-amz-checksum-sha256" },
            existing: null);

        Assert.True(request.ProvidedChecksums.ContainsKey("CRC64NVME"));
        Assert.True(request.ProvidedChecksums.ContainsKey("SHA256"));
        Assert.Equal(string.Empty, request.ProvidedChecksums["CRC64NVME"]);
        Assert.Equal(string.Empty, request.ProvidedChecksums["SHA256"]);
    }

    [Fact]
    public void RegisterExpectedTrailers_SkipsUnknownHeadersAndPreservesExisting()
    {
        var existing = new ChecksumRequest
        {
            ProvidedChecksums = new Dictionary<string, string> { ["CRC32"] = "abc==" }
        };

        var result = TrailerChecksumMerger.RegisterExpectedTrailers(
            new[] { "x-amz-foo", "x-amz-checksum-crc64nvme" },
            existing);

        Assert.Same(existing, result);
        Assert.Equal("abc==", result.ProvidedChecksums["CRC32"]);
        Assert.Equal(string.Empty, result.ProvidedChecksums["CRC64NVME"]);
        Assert.False(result.ProvidedChecksums.ContainsKey("x-amz-foo"));
    }

    [Fact]
    public void MergeIntoCalculator_FeedsTrailerValuesIntoCalculatorForValidation()
    {
        // Calculator is set up with CRC64NVME active (via placeholder) and some data appended.
        // We then merge a trailer value that does NOT match - Finish should report mismatch.
        var request = TrailerChecksumMerger.RegisterExpectedTrailers(
            new[] { "x-amz-checksum-crc64nvme" }, existing: null);
        using var calculator = new StreamingChecksumCalculator(request.Algorithm, request.ProvidedChecksums);
        Assert.True(calculator.HasChecksums);

        calculator.Append("hello"u8);

        TrailerChecksumMerger.MergeIntoCalculator(
            new List<StreamingTrailer>
            {
                new() { Name = "x-amz-checksum-crc64nvme", Value = "AAAAAAAAAAAA" } // deliberately wrong
            },
            calculator);

        var result = calculator.Finish();
        Assert.False(result.IsValid);
        Assert.Contains("CRC64NVME", result.ErrorMessage);
    }

    [Fact]
    public void MergeIntoCalculator_AcceptsCorrectTrailerValue()
    {
        // Symmetry check: when the trailer matches the server-computed digest, Finish is IsValid.
        var request = TrailerChecksumMerger.RegisterExpectedTrailers(
            new[] { "x-amz-checksum-crc64nvme" }, existing: null);
        using var calculator = new StreamingChecksumCalculator(request.Algorithm, request.ProvidedChecksums);

        var payload = "hello"u8.ToArray();
        calculator.Append(payload);

        // First compute what the calculator would emit, so the test doesn't depend on a hard-coded
        // CRC64 value (polynomial could change). Finish() doesn't reset state here; the point is
        // only to prove that a matching trailer passes validation end-to-end, so we use a fresh
        // calculator to read the digest.
        using var reference = new StreamingChecksumCalculator(algorithm: null,
            new Dictionary<string, string> { ["CRC64NVME"] = string.Empty });
        reference.Append(payload);
        var expected = reference.Finish().CalculatedChecksums["CRC64NVME"];

        TrailerChecksumMerger.MergeIntoCalculator(
            new List<StreamingTrailer>
            {
                new() { Name = "x-amz-checksum-crc64nvme", Value = expected }
            },
            calculator);

        var result = calculator.Finish();
        Assert.True(result.IsValid, result.ErrorMessage);
    }
}
