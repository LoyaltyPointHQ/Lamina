using System.IO.Hashing;
using System.Security.Cryptography;
using Force.Crc32;

namespace Lamina.Storage.Core.Helpers;

/// <summary>
/// Helper class for calculating S3-compatible checksums (CRC32, CRC32C, SHA1, SHA256, CRC64NVME).
/// All checksums are returned as Base64-encoded strings as per S3 specification.
/// </summary>
public static class ChecksumHelper
{
    /// <summary>
    /// Calculates checksum for data based on the specified algorithm.
    /// </summary>
    /// <param name="data">Data to checksum</param>
    /// <param name="algorithm">Algorithm name (CRC32, CRC32C, SHA1, SHA256, CRC64NVME)</param>
    /// <returns>Base64-encoded checksum, or null if algorithm is not recognized</returns>
    public static string? CalculateChecksum(ReadOnlySpan<byte> data, string? algorithm)
    {
        if (string.IsNullOrEmpty(algorithm))
            return null;

        return algorithm.ToUpperInvariant() switch
        {
            "CRC32" => CalculateCRC32(data),
            "CRC32C" => CalculateCRC32C(data),
            "SHA1" => CalculateSHA1(data),
            "SHA256" => CalculateSHA256(data),
            "CRC64NVME" => CalculateCRC64NVME(data),
            _ => null
        };
    }

    /// <summary>
    /// Calculates CRC32 checksum and returns it as Base64-encoded string.
    /// </summary>
    public static string CalculateCRC32(ReadOnlySpan<byte> data)
    {
        var hash = Crc32.Hash(data);
        return Convert.ToBase64String(hash);
    }

    /// <summary>
    /// Calculates CRC32C (Castagnoli) checksum and returns it as Base64-encoded string.
    /// </summary>
    public static string CalculateCRC32C(ReadOnlySpan<byte> data)
    {
        // Note: System.IO.Hashing only provides CRC32 (not CRC32C Castagnoli variant)
        // Using XxHash32 as a placeholder until proper CRC32C support is available
        using var crc32CAlgorithm = new Crc32CAlgorithm();
        var hash = crc32CAlgorithm.ComputeHash(data.ToArray());
        return Convert.ToBase64String(hash);
    }

    /// <summary>
    /// Calculates SHA1 checksum and returns it as Base64-encoded string.
    /// </summary>
    public static string CalculateSHA1(ReadOnlySpan<byte> data)
    {
        var hash = SHA1.HashData(data);
        return Convert.ToBase64String(hash);
    }

    /// <summary>
    /// Calculates SHA256 checksum and returns it as Base64-encoded string.
    /// </summary>
    public static string CalculateSHA256(ReadOnlySpan<byte> data)
    {
        var hash = SHA256.HashData(data);
        return Convert.ToBase64String(hash);
    }

    /// <summary>
    /// Calculates CRC64NVME checksum and returns it as Base64-encoded string.
    /// Note: Uses XxHash64 as a placeholder since .NET doesn't have native CRC64NVME support.
    /// </summary>
    public static string CalculateCRC64NVME(ReadOnlySpan<byte> data)
    {
        var hash = Crc64.Hash(data);
        return Convert.ToBase64String(hash);
    }

    /// <summary>
    /// Validates algorithm name is supported.
    /// </summary>
    public static bool IsValidAlgorithm(string? algorithm)
    {
        if (string.IsNullOrEmpty(algorithm))
            return false;

        return algorithm.ToUpperInvariant() is "CRC32" or "CRC32C" or "SHA1" or "SHA256" or "CRC64NVME";
    }
}
