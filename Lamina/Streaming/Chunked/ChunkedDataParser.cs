using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Lamina.Models;
using Lamina.Streaming.Validation;
using Lamina.Streaming.Trailers;
using Microsoft.Extensions.Logging;

namespace Lamina.Streaming.Chunked
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

        public async IAsyncEnumerable<ReadOnlyMemory<byte>> ParseChunkedDataAsync(
            PipeReader dataReader,
            IChunkSignatureValidator? chunkValidator = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            byte[] remainingBuffer = Array.Empty<byte>();

            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await dataReader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (buffer.IsEmpty && result.IsCompleted)
                {
                    break;
                }

                var dataArray = ChunkBuffer.CombineBuffersLegacy(remainingBuffer, buffer);
                var position = 0;

                while (position < dataArray.Length)
                {
                    var chunkResult = ProcessNextChunk(dataArray, position, chunkValidator);

                    if (chunkResult.IsIncomplete)
                    {
                        remainingBuffer = ChunkBuffer.ExtractRemainingData(dataArray, position, dataArray.Length);
                        break;
                    }

                    if (chunkResult.IsFinalChunk)
                    {
                        break;
                    }

                    if (chunkResult.ChunkData.HasValue)
                    {
                        yield return chunkResult.ChunkData.Value;
                    }

                    position = chunkResult.NewPosition;
                }

                dataReader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }

        public async Task<long> ParseChunkedDataToStreamAsync(
            PipeReader dataReader,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator = null,
            CancellationToken cancellationToken = default)
        {
            byte[] remainingBuffer = Array.Empty<byte>();
            long totalBytesWritten = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await dataReader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (buffer.IsEmpty && result.IsCompleted)
                {
                    break;
                }

                var (dataBuffer, isRented) = ChunkBuffer.CombineBuffers(remainingBuffer, buffer, _bufferPool);
                var dataLength = remainingBuffer.Length + (int)buffer.Length;

                try
                {
                    var processingResult = await ProcessChunksToStreamAsync(
                        dataBuffer, dataLength, destinationStream, chunkValidator, cancellationToken);

                    totalBytesWritten += processingResult.bytesWritten;

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

                if (result.IsCompleted)
                {
                    break;
                }
            }

            return totalBytesWritten;
        }

        public async Task<ChunkedDataResult> ParseChunkedDataWithTrailersToStreamAsync(
            PipeReader dataReader,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator = null,
            CancellationToken cancellationToken = default)
        {
            var result = new ChunkedDataResult();
            byte[] remainingBuffer = Array.Empty<byte>();

            try
            {
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
                        dataBuffer, dataBuffer.Length, destinationStream, chunkValidator, cancellationToken);

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
            catch (InvalidOperationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error parsing chunked data with trailers");
                result.ErrorMessage = ex.Message;
                return result;
            }
        }

        private ChunkProcessResult ProcessNextChunk(
            byte[] dataArray,
            int position,
            IChunkSignatureValidator? chunkValidator)
        {
            var headerResult = ChunkHeader.TryParse(dataArray, position, dataArray.Length);
            if (headerResult == null)
            {
                return ChunkProcessResult.Incomplete();
            }

            var header = headerResult.Header;
            position = headerResult.NewPosition;

            if (header.IsFinalChunk)
            {
                ValidateFinalChunk(header, chunkValidator);
                return ChunkProcessResult.FinalChunk(position + ChunkConstants.CrlfPattern.Length);
            }

            if (!header.HasSufficientData(dataArray.Length - position))
            {
                return ChunkProcessResult.Incomplete();
            }

            var chunkData = ExtractChunkData(dataArray, position, header.Size);
            ValidateChunk(chunkData, header, chunkValidator);

            LogChunkDetails(chunkData, header);

            position += header.Size + ChunkConstants.CrlfPattern.Length;
            return ChunkProcessResult.Success(chunkData, position);
        }

        private async Task<(long bytesWritten, bool finalChunkReached, int newPosition)> ProcessChunksToStreamAsync(
            byte[] dataBuffer,
            int dataLength,
            Stream destinationStream,
            IChunkSignatureValidator? chunkValidator,
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
                    await ValidateFinalChunkStreamAsync(header, chunkValidator);
                    position += ChunkConstants.CrlfPattern.Length;
                    return (totalBytesWritten, true, position);
                }

                if (!header.HasSufficientData(dataLength - position))
                {
                    var headerStartPos = ChunkBuffer.CalculateHeaderStartPosition(
                        headerResult.NewPosition - ChunkConstants.CrlfPattern.Length,
                        header.RawHeaderLine.Length);
                    return (totalBytesWritten, false, headerStartPos);
                }

                await destinationStream.WriteAsync(dataBuffer.AsMemory(position, header.Size), cancellationToken);
                totalBytesWritten += header.Size;

                await ValidateChunkStreamAsync(dataBuffer, position, header, chunkValidator);

                position += header.Size + ChunkConstants.CrlfPattern.Length;
            }

            return (totalBytesWritten, false, position);
        }

        private static ReadOnlyMemory<byte> ExtractChunkData(byte[] dataArray, int position, int chunkSize)
        {
            var chunkData = new byte[chunkSize];
            Array.Copy(dataArray, position, chunkData, 0, chunkSize);
            return chunkData;
        }

        private void ValidateChunk(ReadOnlyMemory<byte> chunkData, ChunkHeader header, IChunkSignatureValidator? validator)
        {
            if (validator != null)
            {
                var isValid = validator.ValidateChunk(chunkData, header.Signature, false);
                if (!isValid)
                {
                    throw new InvalidOperationException($"Invalid chunk signature at chunk index {validator.ChunkIndex}");
                }
            }
        }

        private async Task ValidateChunkStreamAsync(byte[] dataBuffer, int position, ChunkHeader header, IChunkSignatureValidator? validator)
        {
            if (validator != null)
            {
                using var chunkStream = new MemoryStream(dataBuffer, position, header.Size, writable: false);
                var isValid = await validator.ValidateChunkStreamAsync(chunkStream, header.Size, header.Signature, false);
                if (!isValid)
                {
                    throw new InvalidOperationException($"Invalid chunk signature at chunk {validator.ChunkIndex}");
                }
            }
        }

        private void ValidateFinalChunk(ChunkHeader header, IChunkSignatureValidator? validator)
        {
            if (validator != null)
            {
                var isValid = validator.ValidateChunk(ReadOnlyMemory<byte>.Empty, header.Signature, true);
                if (!isValid)
                {
                    throw new InvalidOperationException("Invalid final chunk signature");
                }
            }
        }

        private async Task ValidateFinalChunkStreamAsync(ChunkHeader header, IChunkSignatureValidator? validator)
        {
            if (validator != null)
            {
                using var emptyStream = new MemoryStream();
                var isValid = await validator.ValidateChunkStreamAsync(emptyStream, 0, header.Signature, true);
                if (!isValid)
                {
                    throw new InvalidOperationException("Invalid final chunk signature");
                }
            }
        }

        private void LogChunkDetails(ReadOnlyMemory<byte> chunkData, ChunkHeader header)
        {
            if (_logger?.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Debug) == true)
            {
                var debugBytes = chunkData.Length > ChunkConstants.DebugBytesLength
                    ? chunkData.Slice(0, ChunkConstants.DebugBytesLength).ToArray()
                    : chunkData.ToArray();
                var hexString = BitConverter.ToString(debugBytes).Replace("-", " ");

                _logger.LogDebug("Received chunk - Size: {ChunkSize}, Signature: {ChunkSignature}, First bytes: {FirstBytes}",
                    chunkData.Length, header.Signature, hexString);
            }
        }
    }

    /// <summary>
    /// Result of processing a single chunk
    /// </summary>
    internal class ChunkProcessResult
    {
        public ReadOnlyMemory<byte>? ChunkData { get; set; }
        public int NewPosition { get; set; }
        public bool IsIncomplete { get; set; }
        public bool IsFinalChunk { get; set; }

        public static ChunkProcessResult Success(ReadOnlyMemory<byte> chunkData, int newPosition)
            => new() { ChunkData = chunkData, NewPosition = newPosition };

        public static ChunkProcessResult FinalChunk(int newPosition)
            => new() { IsFinalChunk = true, NewPosition = newPosition };

        public static ChunkProcessResult Incomplete()
            => new() { IsIncomplete = true };
    }
}