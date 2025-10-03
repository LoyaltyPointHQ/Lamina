namespace Lamina.Core.Models
{
    /// <summary>
    /// Result of parsing chunked data with trailers
    /// </summary>
    public class ChunkedDataResult
    {
        /// <summary>
        /// Whether parsing and validation succeeded
        /// </summary>
        public bool Success { get; set; } = true;

        /// <summary>
        /// The parsed trailer headers (if any)
        /// </summary>
        public List<StreamingTrailer> Trailers { get; set; } = new();

        /// <summary>
        /// Whether trailer validation succeeded (if trailers were expected)
        /// </summary>
        public bool? TrailerValidationResult { get; set; }

        /// <summary>
        /// Total bytes written to the destination stream
        /// </summary>
        public long TotalBytesWritten { get; set; }

        /// <summary>
        /// Error message if trailer parsing/validation failed
        /// </summary>
        public string? ErrorMessage { get; set; }
    }
}