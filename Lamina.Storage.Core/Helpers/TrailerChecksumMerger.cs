using Lamina.Core.Models;

namespace Lamina.Storage.Core.Helpers;

/// <summary>
/// Maps AWS checksum trailer headers (e.g. <c>x-amz-checksum-crc64nvme</c>) to the algorithm
/// identifiers used by <see cref="StreamingChecksumCalculator"/>, and wires trailer-delivered
/// values into a running calculator so provided-vs-computed validation kicks in at Finish().
///
/// AWS CLI v2 default checksum mode delivers the integrity value in an HTTP trailer (signalled by
/// the request-level <c>x-amz-trailer</c> header) rather than in a normal request header. Without
/// this merge the calculator never sees the client value and just records the server auto-computed
/// digest, which later mismatches what the client expects back on GetObject.
/// </summary>
public static class TrailerChecksumMerger
{
    /// <summary>
    /// Returns the calculator algorithm name for a given trailer header name, or null if the
    /// header is not a recognised checksum trailer.
    /// </summary>
    public static string? MapTrailerNameToAlgorithm(string trailerName)
    {
        if (string.IsNullOrEmpty(trailerName))
        {
            return null;
        }
        return trailerName.Trim().ToLowerInvariant() switch
        {
            "x-amz-checksum-crc32" => "CRC32",
            "x-amz-checksum-crc32c" => "CRC32C",
            "x-amz-checksum-crc64nvme" => "CRC64NVME",
            "x-amz-checksum-sha1" => "SHA1",
            "x-amz-checksum-sha256" => "SHA256",
            _ => null
        };
    }

    /// <summary>
    /// Registers placeholder entries in the given <see cref="ChecksumRequest"/> for each trailer
    /// name the client signalled via the request-level <c>x-amz-trailer</c> header, so that the
    /// resulting <see cref="StreamingChecksumCalculator"/> activates the matching hash state before
    /// streaming begins. Values are filled in later by
    /// <see cref="MergeIntoCalculator"/> once trailers are parsed.
    /// </summary>
    public static ChecksumRequest RegisterExpectedTrailers(IEnumerable<string> trailerHeaderNames, ChecksumRequest? existing)
    {
        var request = existing ?? new ChecksumRequest();
        foreach (var name in trailerHeaderNames)
        {
            var algo = MapTrailerNameToAlgorithm(name);
            if (algo != null && !request.ProvidedChecksums.ContainsKey(algo))
            {
                request.ProvidedChecksums[algo] = string.Empty;
            }
        }
        return request;
    }

    /// <summary>
    /// Feeds trailer-delivered checksum values into a running calculator immediately before Finish,
    /// so the mismatch-vs-calculated path can fire.
    /// </summary>
    public static void MergeIntoCalculator(IEnumerable<StreamingTrailer> trailers, StreamingChecksumCalculator calculator)
    {
        foreach (var trailer in trailers)
        {
            var algo = MapTrailerNameToAlgorithm(trailer.Name);
            if (algo != null)
            {
                calculator.SetProvidedChecksum(algo, trailer.Value);
            }
        }
    }
}
