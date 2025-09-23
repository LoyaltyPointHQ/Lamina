namespace Lamina.Models
{
    /// <summary>
    /// Represents a streaming trailer header
    /// </summary>
    public class StreamingTrailer
    {
        /// <summary>
        /// The trailer header name (e.g., "x-amz-checksum-crc32c")
        /// </summary>
        public required string Name { get; set; }

        /// <summary>
        /// The trailer header value (e.g., base64-encoded checksum)
        /// </summary>
        public required string Value { get; set; }
    }

    /// <summary>
    /// Result of trailer validation
    /// </summary>
    public class TrailerValidationResult
    {
        /// <summary>
        /// Whether the trailer signature is valid
        /// </summary>
        public bool IsValid { get; set; }

        /// <summary>
        /// The validated trailers
        /// </summary>
        public List<StreamingTrailer> Trailers { get; set; } = new();

        /// <summary>
        /// Error message if validation failed
        /// </summary>
        public string? ErrorMessage { get; set; }
    }
}