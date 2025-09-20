using System.Security.Cryptography;

namespace S3Test.Helpers;

public static class ETagHelper
{
    /// <summary>
    /// Computes the ETag for the given data using SHA1 hash.
    /// </summary>
    /// <param name="data">The binary data to compute the ETag for.</param>
    /// <returns>The ETag as a lowercase hex string (without quotes).</returns>
    public static string ComputeETag(byte[] data)
    {
        using var sha1 = SHA1.Create();
        var hash = sha1.ComputeHash(data);
        return Convert.ToHexString(hash).ToLower();
    }

    /// <summary>
    /// Computes the ETag for a file using SHA1 hash without loading it into memory.
    /// </summary>
    /// <param name="filePath">The path to the file.</param>
    /// <returns>The ETag as a lowercase hex string (without quotes).</returns>
    public static async Task<string> ComputeETagFromFileAsync(string filePath)
    {
        // Use FileShare.Read to allow concurrent reads if file is being accessed elsewhere
        await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: true);
        using var sha1 = SHA1.Create();
        var hash = await sha1.ComputeHashAsync(fileStream);
        return Convert.ToHexString(hash).ToLower();
    }

    /// <summary>
    /// Computes the ETag from a stream using SHA1 hash.
    /// </summary>
    /// <param name="stream">The stream to compute the ETag from.</param>
    /// <returns>The ETag as a lowercase hex string (without quotes).</returns>
    public static async Task<string> ComputeETagFromStreamAsync(Stream stream)
    {
        using var sha1 = SHA1.Create();
        var hash = await sha1.ComputeHashAsync(stream);
        return Convert.ToHexString(hash).ToLower();
    }
}