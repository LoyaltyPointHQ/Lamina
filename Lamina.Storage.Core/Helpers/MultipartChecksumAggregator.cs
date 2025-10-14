using System.Buffers.Binary;
using System.IO.Hashing;
using System.Security.Cryptography;
using Force.Crc32;

namespace Lamina.Storage.Core.Helpers;

/// <summary>
/// Aggregates part checksums into a final multipart object checksum according to S3 specification.
/// For multipart uploads, S3 computes the checksum-of-checksums by:
/// 1. Decoding each part's checksum from base64
/// 2. Concatenating all decoded checksums
/// 3. Computing the checksum of the concatenated data
/// 4. Encoding the result back to base64
/// </summary>
public static class MultipartChecksumAggregator
{
    /// <summary>
    /// Aggregates CRC32 checksums from multiple parts.
    /// </summary>
    public static string? AggregateCrc32(IEnumerable<string?> partChecksums)
    {
        var checksums = partChecksums.Where(c => !string.IsNullOrEmpty(c)).ToList();
        if (checksums.Count == 0) return null;

        // Decode all part checksums and concatenate
        var concatenated = new List<byte>();
        foreach (var checksum in checksums)
        {
            var decoded = Convert.FromBase64String(checksum!);
            concatenated.AddRange(decoded);
        }

        // Compute CRC32 of the concatenated checksums
        var aggregated = Crc32Algorithm.Compute(concatenated.ToArray());

        // Encode to base64 (big-endian format for S3 compatibility)
        Span<byte> hash = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(hash, aggregated);
        return Convert.ToBase64String(hash);
    }

    /// <summary>
    /// Aggregates CRC32C checksums from multiple parts.
    /// </summary>
    public static string? AggregateCrc32C(IEnumerable<string?> partChecksums)
    {
        var checksums = partChecksums.Where(c => !string.IsNullOrEmpty(c)).ToList();
        if (checksums.Count == 0) return null;

        // Decode all part checksums and concatenate
        var concatenated = new List<byte>();
        foreach (var checksum in checksums)
        {
            var decoded = Convert.FromBase64String(checksum!);
            concatenated.AddRange(decoded);
        }

        // Compute CRC32C of the concatenated checksums
        var aggregated = Crc32CAlgorithm.Compute(concatenated.ToArray());

        // Encode to base64 (big-endian format for S3 compatibility)
        Span<byte> hash = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(hash, aggregated);
        return Convert.ToBase64String(hash);
    }

    /// <summary>
    /// Aggregates SHA1 checksums from multiple parts.
    /// </summary>
    public static string? AggregateSha1(IEnumerable<string?> partChecksums)
    {
        var checksums = partChecksums.Where(c => !string.IsNullOrEmpty(c)).ToList();
        if (checksums.Count == 0) return null;

        // Decode all part checksums and concatenate
        var concatenated = new List<byte>();
        foreach (var checksum in checksums)
        {
            var decoded = Convert.FromBase64String(checksum!);
            concatenated.AddRange(decoded);
        }

        // Compute SHA1 of the concatenated checksums
        using var sha1 = SHA1.Create();
        var aggregated = sha1.ComputeHash(concatenated.ToArray());

        // Encode to base64
        return Convert.ToBase64String(aggregated);
    }

    /// <summary>
    /// Aggregates SHA256 checksums from multiple parts.
    /// </summary>
    public static string? AggregateSha256(IEnumerable<string?> partChecksums)
    {
        var checksums = partChecksums.Where(c => !string.IsNullOrEmpty(c)).ToList();
        if (checksums.Count == 0) return null;

        // Decode all part checksums and concatenate
        var concatenated = new List<byte>();
        foreach (var checksum in checksums)
        {
            var decoded = Convert.FromBase64String(checksum!);
            concatenated.AddRange(decoded);
        }

        // Compute SHA256 of the concatenated checksums
        using var sha256 = SHA256.Create();
        var aggregated = sha256.ComputeHash(concatenated.ToArray());

        // Encode to base64
        return Convert.ToBase64String(aggregated);
    }

    /// <summary>
    /// Aggregates CRC64NVME checksums from multiple parts.
    /// </summary>
    public static string? AggregateCrc64Nvme(IEnumerable<string?> partChecksums)
    {
        var checksums = partChecksums.Where(c => !string.IsNullOrEmpty(c)).ToList();
        if (checksums.Count == 0) return null;

        // Decode all part checksums and concatenate
        var concatenated = new List<byte>();
        foreach (var checksum in checksums)
        {
            var decoded = Convert.FromBase64String(checksum!);
            concatenated.AddRange(decoded);
        }

        // Compute CRC64NVME of the concatenated checksums using System.IO.Hashing.Crc64
        var crc64 = new Crc64();
        crc64.Append(concatenated.ToArray());
        var hash = crc64.GetCurrentHash();

        // Encode to base64
        return Convert.ToBase64String(hash);
    }
}
