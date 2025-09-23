using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Lamina.Models;
using Lamina.Streaming.Validation;
using Microsoft.Extensions.Logging;

namespace Lamina.Streaming.Chunked
{
    /// <summary>
    /// Interface for parsing AWS chunked encoded data
    /// </summary>
    public interface IChunkedDataParser
    {
        /// <summary>
        /// Parses AWS chunked encoding data and returns decoded chunks
        /// </summary>
        IAsyncEnumerable<ReadOnlyMemory<byte>> ParseChunkedDataAsync(
            PipeReader dataReader,
            IChunkSignatureValidator? chunkValidator = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Parses AWS chunked encoding data and writes directly to a stream
        /// </summary>
        Task<long> ParseChunkedDataToStreamAsync(
            PipeReader dataReader,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Parses AWS chunked encoding data with trailer support and writes to a stream
        /// </summary>
        Task<ChunkedDataResult> ParseChunkedDataWithTrailersToStreamAsync(
            PipeReader dataReader,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator = null,
            CancellationToken cancellationToken = default);
    }
}