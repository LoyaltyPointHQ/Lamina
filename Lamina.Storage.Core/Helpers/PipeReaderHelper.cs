using System.IO.Pipelines;
using System.Text;

namespace Lamina.Storage.Core.Helpers;

/// <summary>
/// Helper class for common PipeReader operations
/// </summary>
public static class PipeReaderHelper
{
    /// <summary>
    /// Reads all data from a PipeReader into a byte array
    /// </summary>
    /// <param name="reader">The PipeReader to read from</param>
    /// <param name="completeReader">Whether to call CompleteAsync on the reader when done</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The complete data as a byte array</returns>
    public static async Task<byte[]> ReadAllBytesAsync(PipeReader reader, bool completeReader = true, CancellationToken cancellationToken = default)
    {
        var allSegments = new List<byte[]>();
        var totalSize = 0;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (buffer.Length > 0)
                {
                    foreach (var segment in buffer)
                    {
                        var data = segment.ToArray();
                        allSegments.Add(data);
                        totalSize += data.Length;
                    }
                }

                reader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        finally
        {
            if (completeReader)
            {
                await reader.CompleteAsync();
            }
        }

        // Combine all segments into a single byte array
        var combinedData = new byte[totalSize];
        var offset = 0;
        foreach (var segment in allSegments)
        {
            Array.Copy(segment, 0, combinedData, offset, segment.Length);
            offset += segment.Length;
        }

        return combinedData;
    }

    /// <summary>
    /// Reads all data from a PipeReader into a string using UTF-8 encoding
    /// </summary>
    /// <param name="reader">The PipeReader to read from</param>
    /// <param name="completeReader">Whether to call CompleteAsync on the reader when done</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The complete data as a UTF-8 string</returns>
    public static async Task<string> ReadAllTextAsync(PipeReader reader, bool completeReader = true, CancellationToken cancellationToken = default)
    {
        var bytes = await ReadAllBytesAsync(reader, completeReader, cancellationToken);
        return Encoding.UTF8.GetString(bytes);
    }

    /// <summary>
    /// Copies all data from a PipeReader to a Stream
    /// </summary>
    /// <param name="reader">The PipeReader to read from</param>
    /// <param name="destination">The destination stream to write to</param>
    /// <param name="completeReader">Whether to call CompleteAsync on the reader when done</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The total number of bytes copied</returns>
    public static async Task<long> CopyToAsync(PipeReader reader, Stream destination, bool completeReader = true, CancellationToken cancellationToken = default)
    {
        long totalBytesWritten = 0;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (buffer.IsEmpty && result.IsCompleted)
                {
                    break;
                }

                foreach (var segment in buffer)
                {
                    await destination.WriteAsync(segment, cancellationToken);
                    totalBytesWritten += segment.Length;
                }

                reader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        finally
        {
            if (completeReader)
            {
                await reader.CompleteAsync();
            }
        }

        return totalBytesWritten;
    }
}