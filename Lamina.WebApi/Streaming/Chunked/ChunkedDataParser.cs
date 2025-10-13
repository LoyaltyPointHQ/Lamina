using System.Buffers;
using System.IO.Pipelines;
using JetBrains.Annotations;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.WebApi.Streaming.Trailers;

namespace Lamina.WebApi.Streaming.Chunked
{
    /// <summary>
    /// Parser for AWS chunked encoded data with signature validation support
    /// </summary>
    public class ChunkedDataParser : IChunkedDataParser
    {
        private readonly ILogger? _logger;
        private readonly ArrayPool<byte> _bufferPool;

        public ChunkedDataParser(ILogger? logger = null)
        {
            _logger = logger;
            _bufferPool = ArrayPool<byte>.Shared;
        }

        public async Task<ChunkedDataResult> ParseChunkedDataToStreamAsync(
            PipeReader dataReader,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator = null,
            [InstantHandle] Action<ReadOnlySpan<byte>>? onDataWritten = null,
            CancellationToken cancellationToken = default)
        {
            var result = new ChunkedDataResult();
            byte[] remainingBuffer = Array.Empty<byte>();

            while (!cancellationToken.IsCancellationRequested)
            {
                var readResult = await dataReader.ReadAsync(cancellationToken);
                var buffer = readResult.Buffer;

                if (buffer.IsEmpty && readResult.IsCompleted)
                {
                    break;
                }

                var (dataBuffer, isRented) = ChunkBuffer.CombineBuffers(remainingBuffer, buffer, _bufferPool);
                var dataLength = remainingBuffer.Length + (int)buffer.Length;

                try
                {
                    var processingResult = await ProcessChunksToStreamAsync(
                        dataBuffer, dataLength, destinationStream, chunkValidator, onDataWritten, cancellationToken);

                    // Check for validation failure
                    if (processingResult.validationError != null)
                    {
                        result.Success = false;
                        result.ErrorMessage = processingResult.validationError;
                        return result;
                    }

                    result.TotalBytesWritten += processingResult.bytesWritten;

                    if (processingResult.finalChunkReached)
                    {
                        dataReader.AdvanceTo(buffer.End);
                        break;
                    }

                    remainingBuffer = ChunkBuffer.ExtractRemainingData(dataBuffer, processingResult.newPosition, dataLength);
                }
                finally
                {
                    ChunkBuffer.SafeReturnBuffer(dataBuffer, _bufferPool, isRented);
                }

                dataReader.AdvanceTo(buffer.End);

                if (readResult.IsCompleted)
                {
                    break;
                }
            }

            return result;
        }

        public async Task<ChunkedDataResult> ParseChunkedDataWithTrailersToStreamAsync(
            PipeReader dataReader,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator = null,
            [InstantHandle] Action<ReadOnlySpan<byte>>? onDataWritten = null,
            CancellationToken cancellationToken = default)
        {
            var result = new ChunkedDataResult();
            byte[] remainingBuffer = Array.Empty<byte>();

            while (!cancellationToken.IsCancellationRequested)
            {
                var readResult = await dataReader.ReadAsync(cancellationToken);
                var buffer = readResult.Buffer;

                if (buffer.IsEmpty && readResult.IsCompleted)
                {
                    break;
                }

                var dataBuffer = ChunkBuffer.CombineBuffersLegacy(remainingBuffer, buffer);
                var parsePosition = 0;

                var processingResult = await ProcessChunksToStreamAsync(
                    dataBuffer, dataBuffer.Length, destinationStream, chunkValidator, onDataWritten, cancellationToken);

                // Check for validation failure
                if (processingResult.validationError != null)
                {
                    result.Success = false;
                    result.ErrorMessage = processingResult.validationError;
                    return result;
                }

                result.TotalBytesWritten += processingResult.bytesWritten;
                parsePosition = processingResult.newPosition;

                if (processingResult.finalChunkReached)
                {
                    if (chunkValidator?.ExpectsTrailers == true && parsePosition < dataBuffer.Length)
                    {
                        var trailerResult = TrailerParser.ParseTrailersAsync(
                            dataBuffer, parsePosition, chunkValidator, _logger);

                        result.Trailers = trailerResult.trailers;
                        result.TrailerValidationResult = trailerResult.isValid;
                        result.ErrorMessage = trailerResult.errorMessage;
                    }
                    dataReader.AdvanceTo(buffer.End);
                    break;
                }

                remainingBuffer = ChunkBuffer.ExtractRemainingData(dataBuffer, parsePosition, dataBuffer.Length);
                dataReader.AdvanceTo(buffer.End);

                if (readResult.IsCompleted)
                {
                    break;
                }
            }

            return result;
        }

        private async Task<(long bytesWritten, bool finalChunkReached, int newPosition, string? validationError)> ProcessChunksToStreamAsync(
            byte[] dataBuffer,
            int dataLength,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator,
            [InstantHandle] Action<ReadOnlySpan<byte>>? onDataWritten,
            CancellationToken cancellationToken)
        {
            long totalBytesWritten = 0;
            var position = 0;

            while (position < dataLength)
            {
                var headerResult = ChunkHeader.TryParse(dataBuffer, position, dataLength);
                if (headerResult == null)
                {
                    break; // Incomplete header
                }

                var header = headerResult.Header;
                position = headerResult.NewPosition;

                if (header.IsFinalChunk)
                {
                    var (isValid, errorMessage) = await ValidateFinalChunkStreamAsync(header, chunkValidator);
                    if (!isValid)
                    {
                        return (totalBytesWritten, false, position, errorMessage);
                    }
                    position += ChunkConstants.CrlfPattern.Length;
                    return (totalBytesWritten, true, position, null);
                }

                if (!header.HasSufficientData(dataLength - position))
                {
                    var headerStartPos = ChunkBuffer.CalculateHeaderStartPosition(
                        headerResult.NewPosition - ChunkConstants.CrlfPattern.Length,
                        header.RawHeaderLine.Length);
                    return (totalBytesWritten, false, headerStartPos, null);
                }

                await destinationStream.WriteAsync(dataBuffer.AsMemory(position, header.Size), cancellationToken);
                totalBytesWritten += header.Size;

                // Invoke callback with the decoded chunk data
                onDataWritten?.Invoke(dataBuffer.AsSpan(position, header.Size));

                var validationResult = await ValidateChunkStreamAsync(dataBuffer, position, header, chunkValidator);
                if (!validationResult.isValid)
                {
                    return (totalBytesWritten, false, position, validationResult.errorMessage);
                }

                position += header.Size + ChunkConstants.CrlfPattern.Length;
            }

            return (totalBytesWritten, false, position, null);
        }

        private async Task<(bool isValid, string? errorMessage)> ValidateChunkStreamAsync(byte[] dataBuffer, int position, ChunkHeader header, IChunkSignatureValidator? validator)
        {
            if (validator != null)
            {
                using var chunkStream = new MemoryStream(dataBuffer, position, header.Size, writable: false);
                var isValid = await validator.ValidateChunkStreamAsync(chunkStream, header.Size, header.Signature, false);
                if (!isValid)
                {
                    return (false, $"Invalid chunk signature at chunk {validator.ChunkIndex}");
                }
            }
            return (true, null);
        }

        private async Task<(bool isValid, string? errorMessage)> ValidateFinalChunkStreamAsync(ChunkHeader header, IChunkSignatureValidator? validator)
        {
            if (validator != null)
            {
                using var emptyStream = new MemoryStream();
                var isValid = await validator.ValidateChunkStreamAsync(emptyStream, 0, header.Signature, true);
                if (!isValid)
                {
                    return (false, "Invalid final chunk signature");
                }
            }
            return (true, null);
        }

    }
}