using System.IO.Pipelines;
using JetBrains.Annotations;
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
        /// <param name="dataReader">The PipeReader containing chunked encoded data</param>
        /// <param name="destinationStream">The stream to write decoded data to</param>
        /// <param name="chunkValidator">Optional validator for chunk signatures</param>
        /// <param name="onDataWritten">Optional callback invoked after each chunk is written with the decoded data</param>
        /// <param name="cancellationToken">Cancellation token</param>
        Task<ChunkedDataResult> ParseChunkedDataToStreamAsync(
            PipeReader dataReader,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator = null,
            [InstantHandle] Action<ReadOnlySpan<byte>>? onDataWritten = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Parses AWS chunked encoding data with trailer support and writes to a stream
        /// </summary>
        /// <param name="dataReader">The PipeReader containing chunked encoded data</param>
        /// <param name="destinationStream">The stream to write decoded data to</param>
        /// <param name="chunkValidator">Optional validator for chunk signatures</param>
        /// <param name="onDataWritten">Optional callback invoked after each chunk is written with the decoded data</param>
        /// <param name="cancellationToken">Cancellation token</param>
        Task<ChunkedDataResult> ParseChunkedDataWithTrailersToStreamAsync(
            PipeReader dataReader,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator = null,
            [InstantHandle] Action<ReadOnlySpan<byte>>? onDataWritten = null,
            CancellationToken cancellationToken = default);
    }
}