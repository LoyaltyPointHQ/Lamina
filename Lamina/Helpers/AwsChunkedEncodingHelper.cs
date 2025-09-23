using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using Lamina.Services;
using Lamina.Models;
using Microsoft.Extensions.Logging;

namespace Lamina.Helpers
{
    public static class AwsChunkedEncodingHelper
    {
        /// <summary>
        /// Parses AWS chunked encoding data and returns the decoded chunks with optional signature validation
        /// </summary>
        /// <param name="dataReader">The pipe reader containing chunked data</param>
        /// <param name="chunkValidator">Optional chunk signature validator</param>
        /// <param name="logger">Optional logger for debugging</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Async enumerable of decoded chunk data</returns>
        public static async IAsyncEnumerable<ReadOnlyMemory<byte>> ParseChunkedDataAsync(
            PipeReader dataReader,
            IChunkSignatureValidator? chunkValidator = null,
            Microsoft.Extensions.Logging.ILogger? logger = null,
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

                        // Debug: log chunk details
                        if (logger != null)
                        {
                            var debugBytes = chunkData.Length > 20 ? chunkData.Take(20).ToArray() : chunkData;
                            var hexString = BitConverter.ToString(debugBytes).Replace("-", " ");
                            logger.LogDebug("Received chunk - Size: {ChunkSize}, Signature: {ChunkSignature}, First bytes: {FirstBytes}",
                                chunkSize, chunkSignature, hexString);
                        }

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

        /// <summary>
        /// Parses AWS chunked encoding data with trailer support and writes to a stream
        /// </summary>
        /// <param name="dataReader">The pipe reader containing chunked data</param>
        /// <param name="destinationStream">The stream to write decoded data to</param>
        /// <param name="chunkValidator">Optional chunk signature validator</param>
        /// <param name="logger">Optional logger for debugging</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Result including total bytes written and trailer information</returns>
        public static async Task<ChunkedDataResult> ParseChunkedDataWithTrailersToStreamAsync(
            PipeReader dataReader,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator = null,
            ILogger? logger = null,
            CancellationToken cancellationToken = default)
        {
            var result = new ChunkedDataResult();
            byte[] remainingBuffer = Array.Empty<byte>();

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var readResult = await dataReader.ReadAsync(cancellationToken);
                    var buffer = readResult.Buffer;

                    if (buffer.IsEmpty && readResult.IsCompleted)
                    {
                        break;
                    }

                    // Combine remaining buffer with new data
                    var combinedLength = remainingBuffer.Length + (int)buffer.Length;
                    var dataBuffer = new byte[combinedLength];
                    remainingBuffer.CopyTo(dataBuffer, 0);

                    var position = remainingBuffer.Length;
                    foreach (var segment in buffer)
                    {
                        segment.Span.CopyTo(dataBuffer.AsSpan(position));
                        position += segment.Length;
                    }

                    dataReader.AdvanceTo(buffer.End);

                    // Parse chunks
                    var parsePosition = 0;
                    var processingResult = await ProcessChunksInBuffer(
                        dataBuffer,
                        parsePosition,
                        destinationStream,
                        chunkValidator,
                        logger,
                        cancellationToken);

                    result.TotalBytesWritten += processingResult.bytesWritten;
                    parsePosition = processingResult.newPosition;

                    if (processingResult.finalChunkReached)
                    {
                        // Parse trailers if validator expects them
                        if (chunkValidator?.ExpectsTrailers == true && parsePosition < dataBuffer.Length)
                        {
                            var trailerResult = await ParseTrailers(
                                dataBuffer,
                                parsePosition,
                                chunkValidator,
                                logger);

                            result.Trailers = trailerResult.trailers;
                            result.TrailerValidationResult = trailerResult.isValid;
                            result.ErrorMessage = trailerResult.errorMessage;
                        }
                        break;
                    }

                    // Save any remaining unparsed data
                    if (parsePosition < dataBuffer.Length)
                    {
                        var remainingLength = dataBuffer.Length - parsePosition;
                        remainingBuffer = new byte[remainingLength];
                        Array.Copy(dataBuffer, parsePosition, remainingBuffer, 0, remainingLength);
                    }
                    else
                    {
                        remainingBuffer = Array.Empty<byte>();
                    }
                }

                return result;
            }
            catch (InvalidOperationException)
            {
                // Re-throw validation/parsing errors - these are security/integrity issues
                throw;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error parsing chunked data with trailers");
                result.ErrorMessage = ex.Message;
                return result;
            }
        }

        private static async Task<(long bytesWritten, bool finalChunkReached, int newPosition)> ProcessChunksInBuffer(
            byte[] dataBuffer,
            int position,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator,
            ILogger? logger,
            CancellationToken cancellationToken)
        {
            long totalBytesWritten = 0;
            var dataLength = dataBuffer.Length;

            while (position < dataLength)
            {
                // Parse chunk header: size;chunk-signature=...\r\n
                var headerEnd = FindPattern(dataBuffer, position, new byte[] { 0x0D, 0x0A });
                if (headerEnd == -1)
                {
                    // Incomplete header, need more data
                    break;
                }

                var headerLine = Encoding.ASCII.GetString(dataBuffer, position, headerEnd - position);
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
                    // Final chunk - validate signature
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
                    return (totalBytesWritten, true, position);
                }

                // Check if we have enough data for the chunk
                if (position + chunkSize + 2 > dataLength)
                {
                    // Need more data, rewind to start of this chunk header
                    position = headerEnd + 2 - headerLine.Length - 2;
                    break;
                }

                // Write chunk data to destination stream
                await destinationStream.WriteAsync(dataBuffer.AsMemory(position, chunkSize));
                totalBytesWritten += chunkSize;

                // Validate chunk if validator is provided
                if (chunkValidator != null)
                {
                    using var chunkStream = new MemoryStream(dataBuffer, position, chunkSize, writable: false);
                    var isValid = await chunkValidator.ValidateChunkStreamAsync(chunkStream, chunkSize, chunkSignature, isLastChunk: false);
                    if (!isValid)
                    {
                        throw new InvalidOperationException($"Invalid chunk signature at chunk {chunkValidator.ChunkIndex}");
                    }
                }

                position += chunkSize + 2; // Skip chunk data and trailing \r\n
            }

            return (totalBytesWritten, false, position);
        }

        private static async Task<(List<StreamingTrailer> trailers, bool isValid, string? errorMessage)> ParseTrailers(
            byte[] dataBuffer,
            int startPosition,
            IChunkSignatureValidator chunkValidator,
            ILogger? logger)
        {
            var trailers = new List<StreamingTrailer>();
            var position = startPosition;
            string? trailerSignature = null;

            try
            {
                // Parse trailer headers until we find x-amz-trailer-signature or reach end
                while (position < dataBuffer.Length)
                {
                    var lineEnd = FindPattern(dataBuffer, position, new byte[] { 0x0D, 0x0A });
                    if (lineEnd == -1)
                    {
                        break; // Incomplete line
                    }

                    var line = Encoding.UTF8.GetString(dataBuffer, position, lineEnd - position);
                    position = lineEnd + 2;

                    if (string.IsNullOrWhiteSpace(line))
                    {
                        // Empty line marks end of trailers
                        break;
                    }

                    var colonIndex = line.IndexOf(':');
                    if (colonIndex == -1)
                    {
                        logger?.LogWarning("Invalid trailer header format: {Line}", line);
                        continue;
                    }

                    var headerName = line.Substring(0, colonIndex).Trim();
                    var headerValue = line.Substring(colonIndex + 1).Trim();

                    if (headerName.Equals("x-amz-trailer-signature", StringComparison.OrdinalIgnoreCase))
                    {
                        trailerSignature = headerValue;
                        break; // This should be the last trailer
                    }
                    else
                    {
                        trailers.Add(new StreamingTrailer { Name = headerName, Value = headerValue });
                    }
                }

                // Validate trailers if we have a signature
                if (!string.IsNullOrEmpty(trailerSignature))
                {
                    var validationResult = await chunkValidator.ValidateTrailerAsync(trailers, trailerSignature);
                    return (validationResult.Trailers, validationResult.IsValid, validationResult.ErrorMessage);
                }
                else if (chunkValidator.ExpectsTrailers)
                {
                    return (trailers, false, "Missing x-amz-trailer-signature");
                }

                return (trailers, true, null);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error parsing trailers");
                return (trailers, false, ex.Message);
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
        /// <param name="logger">Optional logger for debugging</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Total bytes written</returns>
        public static async Task<long> ParseChunkedDataToStreamAsync(
            PipeReader dataReader,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator = null,
            ILogger? logger = null,
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

                            // Debug logging
                            logger?.LogDebug("Parsing chunk header at position {Position}, headerEnd {HeaderEnd}, headerLine: '{HeaderLine}'",
                                position - headerEnd - 2, headerEnd, headerLine);

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
                                // Not enough data, save everything from the start of the current header
                                // We need to recalculate the header start position correctly:
                                // - headerEnd points to the position of the '\r' in '\r\n'
                                // - headerLine.Length is the length of the header text (without \r\n)
                                // - So header start is: headerEnd - headerLine.Length
                                var headerStartPos = headerEnd - headerLine.Length;

                                logger?.LogDebug("Not enough data for chunk {ChunkSize}, saving from headerStartPos {HeaderStartPos} (headerEnd: {HeaderEnd}, headerLine.Length: {HeaderLength})",
                                    chunkSize, headerStartPos, headerEnd, headerLine.Length);

                                if (headerStartPos >= 0 && headerStartPos < dataLength)
                                {
                                    var remainingLength = dataLength - headerStartPos;
                                    remainingBuffer = new byte[remainingLength];
                                    Array.Copy(dataBuffer, headerStartPos, remainingBuffer, 0, remainingLength);

                                    logger?.LogDebug("Saved {RemainingLength} bytes to remaining buffer", remainingLength);
                                }
                                break;
                            }

                            // Extract chunk data
                            var chunkDataArray = new byte[chunkSize];
                            Array.Copy(dataBuffer, position, chunkDataArray, 0, chunkSize);

                            // Debug: log chunk details
                            if (logger != null)
                            {
                                var debugBytes = chunkDataArray.Length > 20 ? chunkDataArray.Take(20).ToArray() : chunkDataArray;
                                var hexString = BitConverter.ToString(debugBytes).Replace("-", " ");
                                logger.LogDebug("Received chunk (streaming) - Size: {ChunkSize}, Signature: {ChunkSignature}, First bytes: {FirstBytes}",
                                    chunkSize, chunkSignature, hexString);
                            }

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