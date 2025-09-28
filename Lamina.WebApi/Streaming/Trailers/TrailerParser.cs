using System.Text;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.WebApi.Streaming.Chunked;

namespace Lamina.WebApi.Streaming.Trailers
{
    /// <summary>
    /// Parser for AWS streaming trailers
    /// </summary>
    public static class TrailerParser
    {
        /// <summary>
        /// Parses trailing headers from the data buffer and validates them
        /// </summary>
        /// <param name="dataBuffer">Buffer containing trailer data</param>
        /// <param name="startPosition">Position to start parsing from</param>
        /// <param name="chunkValidator">Validator for trailer signatures</param>
        /// <param name="logger">Optional logger</param>
        /// <returns>Parsed trailers, validation result, and any error message</returns>
        public static (List<StreamingTrailer> trailers, bool isValid, string? errorMessage) ParseTrailersAsync(
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
                var parseResult = ParseTrailerHeaders(dataBuffer, position, logger);
                trailers = parseResult.trailers;
                trailerSignature = parseResult.trailerSignature;

                if (!string.IsNullOrEmpty(trailerSignature))
                {
                    var validationResult = chunkValidator.ValidateTrailer(trailers, trailerSignature);
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

        /// <summary>
        /// Parses trailer headers from the buffer and extracts the trailer signature
        /// </summary>
        /// <param name="dataBuffer">Buffer containing trailer data</param>
        /// <param name="startPosition">Position to start parsing from</param>
        /// <param name="logger">Optional logger for warnings</param>
        /// <returns>List of parsed trailers and the trailer signature</returns>
        public static (List<StreamingTrailer> trailers, string? trailerSignature) ParseTrailerHeaders(
            byte[] dataBuffer,
            int startPosition,
            ILogger? logger)
        {
            var trailers = new List<StreamingTrailer>();
            var position = startPosition;
            string? trailerSignature = null;

            while (position < dataBuffer.Length)
            {
                var lineResult = TryParseNextLine(dataBuffer, position);
                if (lineResult == null)
                {
                    break; // Incomplete line
                }

                var line = lineResult.Value.line;
                position = lineResult.Value.newPosition;

                if (string.IsNullOrWhiteSpace(line))
                {
                    break; // Empty line marks end of trailers
                }

                var headerResult = ParseHeaderLine(line);
                if (headerResult == null)
                {
                    logger?.LogWarning("Invalid trailer header format: {Line}", line);
                    continue;
                }

                if (IsTrailerSignatureHeader(headerResult.Value.name))
                {
                    trailerSignature = headerResult.Value.value;
                    break; // This should be the last trailer
                }
                else
                {
                    trailers.Add(new StreamingTrailer
                    {
                        Name = headerResult.Value.name,
                        Value = headerResult.Value.value
                    });
                }
            }

            return (trailers, trailerSignature);
        }

        /// <summary>
        /// Attempts to parse the next line from the buffer
        /// </summary>
        /// <param name="dataBuffer">Buffer containing data</param>
        /// <param name="position">Current position in buffer</param>
        /// <returns>Parsed line and new position, or null if incomplete</returns>
        private static (string line, int newPosition)? TryParseNextLine(byte[] dataBuffer, int position)
        {
            var lineEnd = FindPattern(dataBuffer, position, ChunkConstants.CrlfPattern);
            if (lineEnd == -1)
            {
                return null; // Incomplete line
            }

            var line = Encoding.UTF8.GetString(dataBuffer, position, lineEnd - position);
            var newPosition = lineEnd + ChunkConstants.CrlfPattern.Length;

            return (line, newPosition);
        }

        /// <summary>
        /// Parses a header line into name and value components
        /// </summary>
        /// <param name="line">Header line to parse</param>
        /// <returns>Header name and value, or null if invalid format</returns>
        private static (string name, string value)? ParseHeaderLine(string line)
        {
            var colonIndex = line.IndexOf(':');
            if (colonIndex == -1)
            {
                return null;
            }

            var headerName = line.Substring(0, colonIndex).Trim();
            var headerValue = line.Substring(colonIndex + 1).Trim();

            return (headerName, headerValue);
        }

        /// <summary>
        /// Determines if a header name is the trailer signature header
        /// </summary>
        /// <param name="headerName">Header name to check</param>
        /// <returns>True if this is the trailer signature header</returns>
        private static bool IsTrailerSignatureHeader(string headerName)
        {
            return headerName.Equals("x-amz-trailer-signature", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Finds a pattern in a byte array starting from the specified position
        /// </summary>
        /// <param name="data">Data to search in</param>
        /// <param name="startIndex">Starting position</param>
        /// <param name="pattern">Pattern to find</param>
        /// <returns>Position of pattern, or -1 if not found</returns>
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
    }
}