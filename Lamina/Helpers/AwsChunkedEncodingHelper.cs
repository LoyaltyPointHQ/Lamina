using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using Lamina.Services;

namespace Lamina.Helpers
{
    public static class AwsChunkedEncodingHelper
    {
        /// <summary>
        /// Parses AWS chunked encoding data and returns the decoded chunks with optional signature validation
        /// </summary>
        /// <param name="dataReader">The pipe reader containing chunked data</param>
        /// <param name="chunkValidator">Optional chunk signature validator</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Async enumerable of decoded chunk data</returns>
        public static async IAsyncEnumerable<ReadOnlyMemory<byte>> ParseChunkedDataAsync(
            PipeReader dataReader,
            IChunkSignatureValidator? chunkValidator = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            byte[] remainingBuffer = Array.Empty<byte>();

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = await dataReader.ReadAsync(cancellationToken);
                    var buffer = result.Buffer;

                    if (buffer.IsEmpty && result.IsCompleted)
                    {
                        break;
                    }

                    // Combine with any remaining data from previous read
                    var currentData = new MemoryStream();
                    currentData.Write(remainingBuffer);
                    foreach (var segment in buffer)
                    {
                        currentData.Write(segment.Span);
                    }

                    // Parse AWS chunked encoding
                    var dataArray = currentData.ToArray();
                    var position = 0;

                    while (position < dataArray.Length)
                    {
                        // Try to parse chunk header: size;chunk-signature=...\r\n
                        var headerEnd = FindPattern(dataArray, position, new byte[] { 0x0D, 0x0A }); // \r\n
                        if (headerEnd == -1)
                        {
                            // Not enough data for complete header, save for next iteration
                            remainingBuffer = dataArray.Skip(position).ToArray();
                            break;
                        }

                        var headerLine = Encoding.ASCII.GetString(dataArray, position, headerEnd - position);
                        position = headerEnd + 2; // Skip \r\n

                        // Parse chunk size and signature
                        var parts = headerLine.Split(';');
                        if (parts.Length < 2 || !parts[1].StartsWith("chunk-signature="))
                        {
                            throw new InvalidOperationException($"Invalid chunk header: {headerLine}");
                        }

                        var chunkSizeStr = parts[0];
                        var chunkSignature = parts[1].Substring("chunk-signature=".Length);

                        if (!int.TryParse(chunkSizeStr, System.Globalization.NumberStyles.HexNumber, null, out var chunkSize))
                        {
                            throw new InvalidOperationException($"Invalid chunk size: {chunkSizeStr}");
                        }

                        if (chunkSize == 0)
                        {
                            // Final chunk - validate signature if validator is provided
                            if (chunkValidator != null)
                            {
                                var isValid = await chunkValidator.ValidateChunkAsync(
                                    ReadOnlyMemory<byte>.Empty,
                                    chunkSignature,
                                    isLastChunk: true);

                                if (!isValid)
                                {
                                    throw new InvalidOperationException("Invalid final chunk signature");
                                }
                            }

                            // Skip the final \r\n
                            if (position + 2 <= dataArray.Length)
                            {
                                position += 2;
                            }
                            break;
                        }

                        // Check if we have enough data for the chunk
                        if (position + chunkSize + 2 > dataArray.Length) // +2 for trailing \r\n
                        {
                            // Not enough data, save everything from header start
                            var headerStartPos = position - headerEnd - 2 - headerLine.Length;
                            if (headerStartPos >= 0 && headerStartPos < dataArray.Length)
                            {
                                remainingBuffer = dataArray.Skip(headerStartPos).ToArray();
                            }
                            break;
                        }

                        // Extract chunk data
                        var chunkData = new byte[chunkSize];
                        Array.Copy(dataArray, position, chunkData, 0, chunkSize);

                        // Validate chunk signature if validator is provided
                        if (chunkValidator != null)
                        {
                            var isValid = await chunkValidator.ValidateChunkAsync(
                                chunkData,
                                chunkSignature,
                                isLastChunk: false);

                            if (!isValid)
                            {
                                throw new InvalidOperationException($"Invalid chunk signature at chunk index {chunkValidator.ChunkIndex}");
                            }
                        }

                        yield return chunkData;

                        position += chunkSize + 2; // Skip chunk data and trailing \r\n
                    }

                    dataReader.AdvanceTo(buffer.End);

                    if (result.IsCompleted)
                    {
                        break;
                    }
                }
            }
            finally
            {
                await dataReader.CompleteAsync();
            }
        }

        private static int FindPattern(byte[] data, int startIndex, byte[] pattern)
        {
            for (int i = startIndex; i <= data.Length - pattern.Length; i++)
            {
                bool found = true;
                for (int j = 0; j < pattern.Length; j++)
                {
                    if (data[i + j] != pattern[j])
                    {
                        found = false;
                        break;
                    }
                }
                if (found)
                {
                    return i;
                }
            }
            return -1;
        }

        /// <summary>
        /// Parses AWS chunked encoding data and streams it directly to a destination stream with optional signature validation
        /// </summary>
        /// <param name="dataReader">The pipe reader containing chunked data</param>
        /// <param name="destinationStream">The stream to write decoded data to</param>
        /// <param name="chunkValidator">Optional chunk signature validator</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Total bytes written</returns>
        public static async Task<long> ParseChunkedDataToStreamAsync(
            PipeReader dataReader,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator = null,
            CancellationToken cancellationToken = default)
        {
            byte[] remainingBuffer = Array.Empty<byte>();
            long totalBytesWritten = 0;
            const int maxBufferSize = 64 * 1024; // 64KB max buffer size
            var bufferPool = ArrayPool<byte>.Shared;
            var crlfPattern = new byte[] { 0x0D, 0x0A }; // \r\n

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = await dataReader.ReadAsync(cancellationToken);
                    var buffer = result.Buffer;

                    if (buffer.IsEmpty && result.IsCompleted)
                    {
                        break;
                    }

                    // Rent a buffer for processing
                    var rentedBuffer = bufferPool.Rent(maxBufferSize);
                    try
                    {
                        // Combine with any remaining data from previous read
                        var dataLength = remainingBuffer.Length + (int)buffer.Length;
                        var dataBuffer = dataLength <= maxBufferSize ?
                            rentedBuffer :
                            new byte[dataLength];

                        // Copy remaining data
                        remainingBuffer.CopyTo(dataBuffer, 0);
                        var offset = remainingBuffer.Length;

                        // Copy new data from buffer
                        foreach (var segment in buffer)
                        {
                            segment.Span.CopyTo(dataBuffer.AsSpan(offset, segment.Length));
                            offset += segment.Length;
                        }

                        var position = 0;

                        while (position < dataLength)
                        {
                            // Try to parse chunk header: size;chunk-signature=...\r\n
                            var headerEnd = FindPatternInArray(dataBuffer, position, dataLength, crlfPattern);
                            if (headerEnd == -1)
                            {
                                // Not enough data for complete header, save for next iteration
                                var remainingLength = dataLength - position;
                                remainingBuffer = new byte[remainingLength];
                                Array.Copy(dataBuffer, position, remainingBuffer, 0, remainingLength);
                                break;
                            }

                            var headerLine = System.Text.Encoding.ASCII.GetString(dataBuffer, position, headerEnd - position);
                            position = headerEnd + 2; // Skip \r\n

                            // Parse chunk size and signature
                            var parts = headerLine.Split(';');
                            if (parts.Length < 2 || !parts[1].StartsWith("chunk-signature="))
                            {
                                throw new InvalidOperationException($"Invalid chunk header: {headerLine}");
                            }

                            var chunkSizeStr = parts[0];
                            var chunkSignature = parts[1].Substring("chunk-signature=".Length);

                            if (!int.TryParse(chunkSizeStr, System.Globalization.NumberStyles.HexNumber, null, out var chunkSize))
                            {
                                throw new InvalidOperationException($"Invalid chunk size: {chunkSizeStr}");
                            }

                            if (chunkSize == 0)
                            {
                                // Final chunk - validate signature if validator is provided
                                if (chunkValidator != null)
                                {
                                    using var emptyStream = new MemoryStream();
                                    var isValid = await chunkValidator.ValidateChunkStreamAsync(emptyStream, 0, chunkSignature, isLastChunk: true);

                                    if (!isValid)
                                    {
                                        throw new InvalidOperationException("Invalid final chunk signature");
                                    }
                                }

                                // Skip the final \r\n
                                if (position + 2 <= dataLength)
                                {
                                    position += 2;
                                }
                                return totalBytesWritten;
                            }

                            // Check if we have enough data for the chunk
                            if (position + chunkSize + 2 > dataLength) // +2 for trailing \r\n
                            {
                                // Not enough data, save everything from header start
                                var headerStartPos = position - headerEnd - 2 - headerLine.Length;
                                if (headerStartPos >= 0 && headerStartPos < dataLength)
                                {
                                    var remainingLength = dataLength - headerStartPos;
                                    remainingBuffer = new byte[remainingLength];
                                    Array.Copy(dataBuffer, headerStartPos, remainingBuffer, 0, remainingLength);
                                }
                                break;
                            }

                            // Extract chunk data
                            var chunkDataArray = new byte[chunkSize];
                            Array.Copy(dataBuffer, position, chunkDataArray, 0, chunkSize);

                            // Validate chunk signature if validator is provided
                            if (chunkValidator != null)
                            {
                                using var chunkStream = new MemoryStream(chunkDataArray);
                                var isValid = await chunkValidator.ValidateChunkStreamAsync(chunkStream, chunkSize, chunkSignature, isLastChunk: false);

                                if (!isValid)
                                {
                                    throw new InvalidOperationException($"Invalid chunk signature at chunk index {chunkValidator.ChunkIndex}");
                                }
                            }

                            // Write chunk data to destination stream
                            await destinationStream.WriteAsync(chunkDataArray, cancellationToken);
                            totalBytesWritten += chunkSize;

                            position += chunkSize + 2; // Skip chunk data and trailing \r\n
                        }

                        // Clear remaining buffer if we processed everything
                        if (position >= dataLength)
                        {
                            remainingBuffer = Array.Empty<byte>();
                        }
                    }
                    finally
                    {
                        bufferPool.Return(rentedBuffer);
                    }

                    dataReader.AdvanceTo(buffer.End);

                    if (result.IsCompleted)
                    {
                        break;
                    }
                }

                return totalBytesWritten;
            }
            finally
            {
                await dataReader.CompleteAsync();
            }
        }

        private static int FindPatternInArray(byte[] data, int startIndex, int dataLength, byte[] pattern)
        {
            for (int i = startIndex; i <= dataLength - pattern.Length; i++)
            {
                bool found = true;
                for (int j = 0; j < pattern.Length; j++)
                {
                    if (data[i + j] != pattern[j])
                    {
                        found = false;
                        break;
                    }
                }
                if (found)
                {
                    return i;
                }
            }
            return -1;
        }
    }
}