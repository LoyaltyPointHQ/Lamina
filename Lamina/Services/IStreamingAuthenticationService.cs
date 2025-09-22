using Lamina.Models;
using Microsoft.AspNetCore.Http;

namespace Lamina.Services
{
    /// <summary>
    /// Interface for handling streaming authentication with chunk signature verification
    /// </summary>
    public interface IStreamingAuthenticationService
    {
        /// <summary>
        /// Creates a streaming validator for STREAMING-AWS4-HMAC-SHA256-PAYLOAD requests
        /// </summary>
        Task<IChunkSignatureValidator?> CreateChunkValidatorAsync(HttpRequest request, S3User user);
    }

    /// <summary>
    /// Validates chunk signatures in streaming uploads
    /// </summary>
    public interface IChunkSignatureValidator
    {
        /// <summary>
        /// Validates a single chunk with its signature (memory-based)
        /// </summary>
        Task<bool> ValidateChunkAsync(ReadOnlyMemory<byte> chunkData, string chunkSignature, bool isLastChunk);

        /// <summary>
        /// Validates a single chunk with its signature (streaming-based)
        /// </summary>
        Task<bool> ValidateChunkStreamAsync(Stream chunkStream, long chunkSize, string chunkSignature, bool isLastChunk);

        /// <summary>
        /// Gets the expected chunk size (excluding metadata)
        /// </summary>
        long ExpectedDecodedLength { get; }

        /// <summary>
        /// Current chunk index
        /// </summary>
        int ChunkIndex { get; }
    }
}