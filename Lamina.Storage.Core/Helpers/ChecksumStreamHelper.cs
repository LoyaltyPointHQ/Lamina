using System.IO.Pipelines;

namespace Lamina.Storage.Core.Helpers;

/// <summary>
/// Helper methods for calculating checksums while streaming data.
/// </summary>
public static class ChecksumStreamHelper
{
    /// <summary>
    /// Reads data from a PipeReader, writes it to a Stream, and calculates checksums in a single pass.
    /// </summary>
    /// <param name="dataReader">The PipeReader to read from</param>
    /// <param name="outputStream">The Stream to write to</param>
    /// <param name="checksumCalculator">Optional checksum calculator for computing checksums during the write</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The total number of bytes written</returns>
    public static async Task<long> WriteDataWithChecksumsAsync(
        PipeReader dataReader,
        Stream outputStream,
        StreamingChecksumCalculator? checksumCalculator,
        CancellationToken cancellationToken = default)
    {
        long bytesWritten = 0;
        ReadResult readResult;

        do
        {
            readResult = await dataReader.ReadAsync(cancellationToken);
            var buffer = readResult.Buffer;

            foreach (var segment in buffer)
            {
                // Update checksum calculator if provided
                if (checksumCalculator?.HasChecksums == true)
                {
                    checksumCalculator.Append(segment.Span);
                }

                // Write to stream
                await outputStream.WriteAsync(segment, cancellationToken);
                bytesWritten += segment.Length;
            }

            dataReader.AdvanceTo(buffer.End);
        } while (!readResult.IsCompleted);

        await dataReader.CompleteAsync();
        return bytesWritten;
    }
}
