using System.Security.Cryptography;
using System.Text.RegularExpressions;

namespace Lamina.Storage.Core.Helpers;

public static class ETagHelper
{
    /// <summary>
    /// Computes the ETag for the given data using MD5 hash.
    /// </summary>
    /// <param name="data">The binary data to compute the ETag for.</param>
    /// <returns>The ETag as a lowercase hex string (without quotes).</returns>
    public static string ComputeETag(byte[] data)
    {
        using var md5 = MD5.Create();
        var hash = md5.ComputeHash(data);
        return Convert.ToHexString(hash).ToLower();
    }

    /// <summary>
    /// Computes the ETag for a file using MD5 hash without loading it into memory.
    /// </summary>
    /// <param name="filePath">The path to the file.</param>
    /// <returns>The ETag as a lowercase hex string (without quotes).</returns>
    public static async Task<string> ComputeETagFromFileAsync(string filePath)
    {
        // Use FileShare.Read to allow concurrent reads if file is being accessed elsewhere
        await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: true);
        using var md5 = MD5.Create();
        var hash = await md5.ComputeHashAsync(fileStream);
        return Convert.ToHexString(hash).ToLower();
    }

    /// <summary>
    /// Computes the ETag from a stream using MD5 hash.
    /// </summary>
    /// <param name="stream">The stream to compute the ETag from.</param>
    /// <returns>The ETag as a lowercase hex string (without quotes).</returns>
    public static async Task<string> ComputeETagFromStreamAsync(Stream stream)
    {
        using var md5 = MD5.Create();
        var hash = await md5.ComputeHashAsync(stream);
        return Convert.ToHexString(hash).ToLower();
    }

    /// <summary>
    /// Compares a computed ETag (lowercase hex MD5) against raw MD5 bytes. Used to validate
    /// the client's Content-MD5 header against the server-computed digest of a part body.
    /// </summary>
    /// <returns>True if the 16-byte hashes match exactly.</returns>
    public static bool EtagMatchesMd5(string etag, byte[] expectedMd5)
    {
        if (expectedMd5.Length != 16)
        {
            return false;
        }
        var cleanETag = etag.Trim('"');
        byte[] etagBytes;
        try
        {
            etagBytes = Convert.FromHexString(cleanETag);
        }
        catch (FormatException)
        {
            return false;
        }
        return etagBytes.AsSpan().SequenceEqual(expectedMd5);
    }

    private static readonly Regex MultipartETagPattern = new(@"^[0-9a-fA-F]{32}-\d+$", RegexOptions.Compiled);

    /// <summary>
    /// True if the ETag is in AWS S3 multipart format "{32-hex-MD5}-{partCount}". Multipart
    /// ETags are functions of individual part MD5s and cannot be reconstructed from the merged
    /// file bytes, so callers must preserve them rather than recompute from disk.
    /// </summary>
    public static bool IsMultipartETag(string? etag)
    {
        if (string.IsNullOrEmpty(etag))
        {
            return false;
        }
        return MultipartETagPattern.IsMatch(etag.Trim('"'));
    }

    /// <summary>
    /// Computes a multipart ETag from individual part ETags according to S3 specification.
    /// The multipart ETag is computed by taking the MD5 of the concatenated binary MD5 hashes
    /// of each part, followed by a dash and the number of parts.
    /// </summary>
    /// <param name="partETags">The ETags of individual parts (without quotes).</param>
    /// <returns>The multipart ETag in format "{hash}-{partCount}" (without quotes).</returns>
    public static string ComputeMultipartETag(IEnumerable<string> partETags)
    {
        var etagList = partETags.ToList();
        if (etagList.Count == 0)
        {
            throw new ArgumentException("Part ETags list cannot be empty", nameof(partETags));
        }

        // Convert each part ETag hex string to binary (16 bytes each)
        var concatenatedBytes = new List<byte>();

        foreach (var etag in etagList)
        {
            var cleanETag = etag.Trim('"');
            try
            {
                var bytes = Convert.FromHexString(cleanETag);
                if (bytes.Length != 16)
                {
                    throw new ArgumentException($"Invalid ETag format: {cleanETag}. Expected 32 hex characters.");
                }
                concatenatedBytes.AddRange(bytes);
            }
            catch (FormatException)
            {
                throw new ArgumentException($"Invalid ETag hex format: {cleanETag}");
            }
        }

        // Compute MD5 of the concatenated binary MD5s
        using var md5 = MD5.Create();
        var finalHash = md5.ComputeHash(concatenatedBytes.ToArray());
        var finalETag = Convert.ToHexString(finalHash).ToLower();

        // Return in S3 multipart format: {hash}-{partCount}
        return $"{finalETag}-{etagList.Count}";
    }
}