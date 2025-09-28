using System.Text;

namespace Lamina.WebApi.Streaming.Chunked
{
    /// <summary>
    /// Represents a parsed AWS chunk header
    /// </summary>
    public class ChunkHeader
    {
        /// <summary>
        /// Size of the chunk data in bytes
        /// </summary>
        public int Size { get; set; }

        /// <summary>
        /// Chunk signature for validation
        /// </summary>
        public string Signature { get; set; } = string.Empty;

        /// <summary>
        /// Whether this is the final chunk (size = 0)
        /// </summary>
        public bool IsFinalChunk => Size == 0;

        /// <summary>
        /// The raw header line as received
        /// </summary>
        public string RawHeaderLine { get; set; } = string.Empty;

        /// <summary>
        /// Attempts to parse a chunk header from the provided data at the specified position
        /// </summary>
        /// <param name="data">Buffer containing the data</param>
        /// <param name="position">Starting position in the buffer</param>
        /// <param name="dataLength">Length of valid data in the buffer</param>
        /// <returns>Parsed chunk header and new position, or null if incomplete</returns>
        public static ChunkHeaderParseResult? TryParse(byte[] data, int position, int dataLength)
        {
            var headerEnd = FindPattern(data, position, dataLength, ChunkConstants.CrlfPattern);
            if (headerEnd == -1)
            {
                return null; // Incomplete header
            }

            var headerLine = Encoding.ASCII.GetString(data, position, headerEnd - position);
            var newPosition = headerEnd + ChunkConstants.CrlfPattern.Length;

            var header = ParseHeaderLine(headerLine);
            if (header == null)
            {
                throw new InvalidOperationException($"Invalid chunk header: {headerLine}");
            }

            return new ChunkHeaderParseResult
            {
                Header = header,
                NewPosition = newPosition
            };
        }

        /// <summary>
        /// Parses a chunk header line into a ChunkHeader object
        /// </summary>
        /// <param name="headerLine">The header line to parse</param>
        /// <returns>Parsed ChunkHeader or null if invalid</returns>
        public static ChunkHeader? ParseHeaderLine(string headerLine)
        {
            var parts = headerLine.Split(';');
            if (parts.Length < 2 || !parts[1].StartsWith(ChunkConstants.ChunkSignaturePrefix))
            {
                return null;
            }

            var chunkSizeStr = parts[0];
            var chunkSignature = parts[1].Substring(ChunkConstants.ChunkSignaturePrefix.Length);

            if (!int.TryParse(chunkSizeStr, ChunkConstants.HexNumberStyle, null, out var chunkSize))
            {
                return null;
            }

            return new ChunkHeader
            {
                Size = chunkSize,
                Signature = chunkSignature,
                RawHeaderLine = headerLine
            };
        }

        /// <summary>
        /// Validates that enough data is available for this chunk
        /// </summary>
        /// <param name="availableBytes">Number of bytes available after the header</param>
        /// <returns>True if enough data is available</returns>
        public bool HasSufficientData(int availableBytes)
        {
            return availableBytes >= Size + ChunkConstants.CrlfPattern.Length;
        }

        /// <summary>
        /// Finds a pattern in a byte array within the specified range
        /// </summary>
        private static int FindPattern(byte[] data, int startIndex, int dataLength, byte[] pattern)
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

    /// <summary>
    /// Result of parsing a chunk header
    /// </summary>
    public class ChunkHeaderParseResult
    {
        /// <summary>
        /// The parsed chunk header
        /// </summary>
        public required ChunkHeader Header { get; set; }

        /// <summary>
        /// New position in the buffer after parsing the header
        /// </summary>
        public int NewPosition { get; set; }
    }
}