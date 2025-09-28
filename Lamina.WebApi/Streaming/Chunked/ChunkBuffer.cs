using System.Buffers;

namespace Lamina.WebApi.Streaming.Chunked
{
    /// <summary>
    /// Utility class for managing buffers during chunk parsing
    /// </summary>
    public static class ChunkBuffer
    {
        /// <summary>
        /// Combines remaining buffer with new data from PipeReader
        /// </summary>
        /// <param name="remainingBuffer">Previously remaining data</param>
        /// <param name="pipeBuffer">New data from PipeReader</param>
        /// <param name="bufferPool">Array pool for efficient buffer management</param>
        /// <returns>Combined data buffer and flag indicating if a rented buffer was used</returns>
        public static (byte[] combinedData, bool isRented) CombineBuffers(
            byte[] remainingBuffer,
            ReadOnlySequence<byte> pipeBuffer,
            ArrayPool<byte> bufferPool)
        {
            var totalLength = remainingBuffer.Length + (int)pipeBuffer.Length;

            byte[] dataBuffer;
            bool isRented = false;

            if (totalLength <= ChunkConstants.MaxBufferSize)
            {
                dataBuffer = bufferPool.Rent(ChunkConstants.MaxBufferSize);
                isRented = true;
            }
            else
            {
                dataBuffer = new byte[totalLength];
            }

            // Copy remaining data
            remainingBuffer.CopyTo(dataBuffer, 0);
            var offset = remainingBuffer.Length;

            // Copy new data from pipe buffer
            foreach (var segment in pipeBuffer)
            {
                segment.Span.CopyTo(dataBuffer.AsSpan(offset, segment.Length));
                offset += segment.Length;
            }

            return (dataBuffer, isRented);
        }

        /// <summary>
        /// Extracts remaining data from the buffer starting at the specified position
        /// </summary>
        /// <param name="dataBuffer">Source buffer</param>
        /// <param name="startPosition">Starting position for remaining data</param>
        /// <param name="dataLength">Total length of valid data in buffer</param>
        /// <returns>Array containing remaining data, or empty array if no data remains</returns>
        public static byte[] ExtractRemainingData(byte[] dataBuffer, int startPosition, int dataLength)
        {
            if (startPosition >= dataLength)
            {
                return Array.Empty<byte>();
            }

            var remainingLength = dataLength - startPosition;
            var remainingBuffer = new byte[remainingLength];
            Array.Copy(dataBuffer, startPosition, remainingBuffer, 0, remainingLength);
            return remainingBuffer;
        }

        /// <summary>
        /// Combines remaining buffer with new data using MemoryStream (legacy compatibility)
        /// </summary>
        /// <param name="remainingBuffer">Previously remaining data</param>
        /// <param name="pipeBuffer">New data from PipeReader</param>
        /// <returns>Combined data as byte array</returns>
        public static byte[] CombineBuffersLegacy(byte[] remainingBuffer, ReadOnlySequence<byte> pipeBuffer)
        {
            using var currentData = new MemoryStream();
            currentData.Write(remainingBuffer);

            foreach (var segment in pipeBuffer)
            {
                currentData.Write(segment.Span);
            }

            return currentData.ToArray();
        }

        /// <summary>
        /// Safely returns a rented buffer to the pool
        /// </summary>
        /// <param name="buffer">Buffer to return</param>
        /// <param name="bufferPool">Array pool to return to</param>
        /// <param name="isRented">Whether the buffer was actually rented</param>
        public static void SafeReturnBuffer(byte[] buffer, ArrayPool<byte> bufferPool, bool isRented)
        {
            if (isRented)
            {
                bufferPool.Return(buffer);
            }
        }

        /// <summary>
        /// Calculates the header start position when rewinding due to insufficient data
        /// </summary>
        /// <param name="headerEnd">Position where header ends</param>
        /// <param name="headerLineLength">Length of the header line</param>
        /// <returns>Start position of the header</returns>
        public static int CalculateHeaderStartPosition(int headerEnd, int headerLineLength)
        {
            return headerEnd - headerLineLength;
        }
    }
}