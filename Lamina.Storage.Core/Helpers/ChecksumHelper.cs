using System.Buffers;

namespace Lamina.Storage.Core.Helpers;

/// <summary>
/// Helper class for computing checksums selectively based on which algorithms are needed.
/// Supports single-pass computation of multiple checksums for efficiency.
/// </summary>
public static class ChecksumHelper
{
    /// <summary>
    /// Computes only the specified checksums from a file in a single pass.
    /// </summary>
    /// <param name="filePath">Path to the file to checksum</param>
    /// <param name="algorithms">List of algorithm names to compute (e.g., "CRC32", "SHA256")</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Dictionary with algorithm names as keys and base64-encoded checksums as values</returns>
    public static async Task<Dictionary<string, string>> ComputeSelectiveChecksumsFromFileAsync(
        string filePath,
        IEnumerable<string> algorithms,
        CancellationToken cancellationToken = default)
    {
        var algorithmList = algorithms.ToList();
        if (algorithmList.Count == 0)
        {
            return new Dictionary<string, string>();
        }

        using var calculator = new StreamingChecksumCalculator(algorithmList);

        await using var fileStream = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 81920,
            useAsync: true);

        var buffer = ArrayPool<byte>.Shared.Rent(81920);
        try
        {
            int bytesRead;

            // Single pass through the file, computing all requested checksums
            while ((bytesRead = await fileStream.ReadAsync(buffer.AsMemory(0, 81920), cancellationToken)) > 0)
            {
                calculator.Append(buffer.AsSpan(0, bytesRead));
            }

            var result = calculator.Finish();
            return result.CalculatedChecksums;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
