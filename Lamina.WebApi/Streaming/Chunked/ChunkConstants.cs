namespace Lamina.WebApi.Streaming.Chunked
{
    /// <summary>
    /// Constants and patterns used in AWS chunked encoding
    /// </summary>
    public static class ChunkConstants
    {
        /// <summary>
        /// CRLF pattern (\r\n) used to terminate chunk headers and trailers
        /// </summary>
        public static readonly byte[] CrlfPattern = { 0x0D, 0x0A };

        /// <summary>
        /// Prefix for chunk signature parameter in chunk headers
        /// </summary>
        public const string ChunkSignaturePrefix = "chunk-signature=";

        /// <summary>
        /// Maximum buffer size for processing chunks (64KB)
        /// </summary>
        public const int MaxBufferSize = 64 * 1024;

        /// <summary>
        /// Default number of bytes to show in debug logs
        /// </summary>
        public const int DebugBytesLength = 20;

        /// <summary>
        /// Hexadecimal number style for parsing chunk sizes
        /// </summary>
        public const System.Globalization.NumberStyles HexNumberStyle = System.Globalization.NumberStyles.HexNumber;
    }
}