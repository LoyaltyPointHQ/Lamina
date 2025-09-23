using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Lamina.Models;
using Lamina.Streaming.Chunked;
using Lamina.Streaming.Validation;
using Microsoft.Extensions.Logging;

namespace Lamina.Helpers
{
    /// <summary>
    /// Facade for AWS chunked encoding operations - maintains backward compatibility
    /// while delegating to the new refactored components
    /// </summary>
    public static class AwsChunkedEncodingHelper
    {
        /// <summary>
        /// Parses AWS chunked encoding data and returns the decoded chunks with optional signature validation
        /// </summary>
        /// <param name="dataReader">The pipe reader containing chunked data</param>
        /// <param name="chunkValidator">Optional chunk signature validator</param>
        /// <param name="logger">Optional logger for debugging</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Async enumerable of decoded chunk data</returns>
        public static async IAsyncEnumerable<ReadOnlyMemory<byte>> ParseChunkedDataAsync(
            PipeReader dataReader,
            IChunkSignatureValidator? chunkValidator = null,
            ILogger? logger = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var parser = new ChunkedDataParser(logger);
            await foreach (var chunk in parser.ParseChunkedDataAsync(dataReader, chunkValidator, cancellationToken))
            {
                yield return chunk;
            }
        }

        /// <summary>
        /// Parses AWS chunked encoding data with trailer support and writes to a stream
        /// </summary>
        /// <param name="dataReader">The pipe reader containing chunked data</param>
        /// <param name="destinationStream">The stream to write decoded data to</param>
        /// <param name="chunkValidator">Optional chunk signature validator</param>
        /// <param name="logger">Optional logger for debugging</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Result including total bytes written and trailer information</returns>
        public static async Task<ChunkedDataResult> ParseChunkedDataWithTrailersToStreamAsync(
            PipeReader dataReader,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator = null,
            ILogger? logger = null,
            CancellationToken cancellationToken = default)
        {
            var parser = new ChunkedDataParser(logger);
            return await parser.ParseChunkedDataWithTrailersToStreamAsync(
                dataReader, destinationStream, chunkValidator, cancellationToken);
        }


        /// <summary>
        /// Parses AWS chunked encoding data and streams it directly to a destination stream with optional signature validation
        /// </summary>
        /// <param name="dataReader">The pipe reader containing chunked data</param>
        /// <param name="destinationStream">The stream to write decoded data to</param>
        /// <param name="chunkValidator">Optional chunk signature validator</param>
        /// <param name="logger">Optional logger for debugging</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Total bytes written</returns>
        public static async Task<long> ParseChunkedDataToStreamAsync(
            PipeReader dataReader,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator = null,
            ILogger? logger = null,
            CancellationToken cancellationToken = default)
        {
            var parser = new ChunkedDataParser(logger);
            return await parser.ParseChunkedDataToStreamAsync(
                dataReader, destinationStream, chunkValidator, cancellationToken);
        }
    }
}