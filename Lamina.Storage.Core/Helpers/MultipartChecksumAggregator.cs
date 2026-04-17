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
    /// Aggregates CRC-64/NVME part checksums into the full-object checksum of the
    /// concatenated parts using GF(2) linearization (CRC combine). S3 spec
    /// (https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html)
    /// states CRC-64/NVME in multipart uploads supports full-object checksum only,
    /// not the composite (checksum-of-checksums) variant used for CRC32/CRC32C.
    /// </summary>
    /// <param name="parts">Sequence of (base64 part checksum, part size in bytes) pairs in part order.</param>
    public static string? AggregateCrc64NvmeFullObject(IEnumerable<(string? Checksum, long Size)> parts)
    {
        ulong combined = 0UL;
        bool any = false;
        Span<byte> partBytes = stackalloc byte[8];

        foreach (var (checksum, size) in parts)
        {
            if (string.IsNullOrEmpty(checksum)) continue;
            if (!Convert.TryFromBase64String(checksum, partBytes, out var written) || written != 8)
                throw new FormatException($"Invalid CRC-64/NVME part checksum (expected 8-byte base64): '{checksum}'");

            var partCrc = BinaryPrimitives.ReadUInt64BigEndian(partBytes);
            combined = any ? Crc64Nvme.Combine(combined, partCrc, size) : partCrc;
            any = true;
        }

        if (!any) return null;

        Span<byte> outBytes = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(outBytes, combined);
        return Convert.ToBase64String(outBytes);
    }
}
