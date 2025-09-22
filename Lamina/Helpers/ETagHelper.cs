using System.Security.Cryptography;

namespace Lamina.Helpers;

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
}