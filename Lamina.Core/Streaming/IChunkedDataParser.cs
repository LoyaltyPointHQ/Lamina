using System.IO.Pipelines;
using Lamina.Core.Models;

namespace Lamina.Core.Streaming
{
    /// <summary>
    /// Interface for parsing AWS chunked encoded data
    /// </summary>
    public interface IChunkedDataParser
    {
        /// <summary>
        /// Parses AWS chunked encoding data and writes directly to a stream
        /// </summary>
        Task<ChunkedDataResult> ParseChunkedDataToStreamAsync(
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