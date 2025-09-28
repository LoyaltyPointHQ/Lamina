using System.IO.Pipelines;
using System.Text;
using Lamina.Core.Streaming;
using Lamina.WebApi.Streaming.Chunked;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Lamina.WebApi.Tests.Streaming.Chunked
{
    public class ChunkedDataParserTests
    {
        private readonly Mock<ILogger> _loggerMock;
        private readonly Mock<IChunkSignatureValidator> _chunkValidatorMock;
        private readonly IChunkedDataParser _chunkedDataParser;

        public ChunkedDataParserTests()
        {
            _loggerMock = new Mock<ILogger>();
            _chunkValidatorMock = new Mock<IChunkSignatureValidator>();

            // Enable debug logging for our tests
            _loggerMock.Setup(x => x.IsEnabled(LogLevel.Debug)).Returns(true);
            _loggerMock.Setup(x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()))
                .Callback<LogLevel, EventId, object, Exception, Delegate>((level, eventId, state, exception, formatter) =>
                {
                    Console.WriteLine($"[DEBUG] {formatter.DynamicInvoke(state, exception)}");
                });

            _chunkedDataParser = new ChunkedDataParser(_loggerMock.Object);
        }

        [Fact]
        public async Task ParseChunkedDataToStreamAsync_HandlesSimpleChunk()
        {
            // Arrange
            var chunkedData = CreateSimpleChunkedData();
            var pipeReader = CreatePipeReader(chunkedData);
            var destinationStream = new MemoryStream();

            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), It.IsAny<long>(), It.IsAny<string>(), false))
                .ReturnsAsync(true);

            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), 0, It.IsAny<string>(), true))
                .ReturnsAsync(true);

            // Act
            var totalBytes = await _chunkedDataParser.ParseChunkedDataToStreamAsync(
                pipeReader, destinationStream, _chunkValidatorMock.Object);

            // Assert
            Assert.Equal(11, totalBytes); // "Hello World"

            // Verify destination stream content
            destinationStream.Position = 0;
            var content = await new StreamReader(destinationStream).ReadToEndAsync();
            Assert.Equal("Hello World", content);
        }

        [Fact]
        public async Task ParseChunkedDataToStreamAsync_HandlesEmptyStream()
        {
            // Arrange
            var pipeReader = CreatePipeReader(Array.Empty<byte>());
            var destinationStream = new MemoryStream();

            // Act
            var totalBytes = await _chunkedDataParser.ParseChunkedDataToStreamAsync(
                pipeReader, destinationStream, null);

            // Assert
            Assert.Equal(0, totalBytes);
        }

        [Fact]
        public async Task ParseChunkedDataToStreamAsync_HandlesChunksSpanningMultipleReads()
        {
            // This test simulates the specific bug that was fixed where a chunk header
            // is parsed but there isn't enough data to read the complete chunk,
            // causing incorrect stream position calculation

            // Arrange
            var fullChunkedData = CreateSpecificBugScenario();
            var pipeReader = CreateBugReproducingPipeReader(fullChunkedData);
            var destinationStream = new MemoryStream();

            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), It.IsAny<long>(), It.IsAny<string>(), false))
                .ReturnsAsync(true);

            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), 0, It.IsAny<string>(), true))
                .ReturnsAsync(true);

            // Act
            var totalBytes = await _chunkedDataParser.ParseChunkedDataToStreamAsync(
                pipeReader, destinationStream, _chunkValidatorMock.Object);

            // Assert
            Assert.Equal(34, totalBytes); // 21 + 13 bytes

            // Verify destination stream content
            destinationStream.Position = 0;
            var content = await new StreamReader(destinationStream).ReadToEndAsync();
            Assert.Equal("This is a long test string that wi", content);
        }

        [Fact]
        public async Task ParseChunkedDataToStreamAsync_HandlesInvalidChunkSize()
        {
            // Arrange
            var invalidChunkedData = CreateInvalidChunkedData();
            var pipeReader = CreatePipeReader(invalidChunkedData);
            var destinationStream = new MemoryStream();

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await _chunkedDataParser.ParseChunkedDataToStreamAsync(
                    pipeReader, destinationStream, null));
        }

        [Fact]
        public async Task ParseChunkedDataToStreamAsync_HandlesChunkValidationFailure()
        {
            // Arrange
            var chunkedData = CreateSimpleChunkedData();
            var pipeReader = CreatePipeReader(chunkedData);
            var destinationStream = new MemoryStream();

            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), It.IsAny<long>(), It.IsAny<string>(), false))
                .ReturnsAsync(false);

            _chunkValidatorMock.SetupGet(v => v.ChunkIndex).Returns(1);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await _chunkedDataParser.ParseChunkedDataToStreamAsync(
                    pipeReader, destinationStream, _chunkValidatorMock.Object));

            Assert.Contains("Invalid chunk signature at chunk 1", exception.Message);
        }

        private byte[] CreateSimpleChunkedData()
        {
            // Create chunked data: "Hello World" in two chunks
            var data = new StringBuilder();

            // First chunk: "Hello" (5 bytes)
            data.Append("5;chunk-signature=chunk1sig\r\n");
            data.Append("Hello\r\n");

            // Second chunk: " World" (6 bytes)
            data.Append("6;chunk-signature=chunk2sig\r\n");
            data.Append(" World\r\n");

            // Final chunk (0 bytes)
            data.Append("0;chunk-signature=finalsig\r\n");
            data.Append("\r\n");

            return Encoding.UTF8.GetBytes(data.ToString());
        }

        private byte[] CreateSpecificBugScenario()
        {
            // Create chunked data that reproduces the exact bug scenario
            var data = new StringBuilder();

            // First chunk: "This is a long test s" (21 bytes = 0x15 in hex)
            data.Append("15;chunk-signature=chunk1sig\r\n");
            data.Append("This is a long test s\r\n");

            // Second chunk: "tring that wi" (13 bytes = 0xD in hex)
            data.Append("d;chunk-signature=chunk2sig\r\n");
            data.Append("tring that wi\r\n");

            // Final chunk (0 bytes)
            data.Append("0;chunk-signature=finalsig\r\n");
            data.Append("\r\n");

            return Encoding.UTF8.GetBytes(data.ToString());
        }

        private byte[] CreateLargeChunkedData()
        {
            // Create chunked data with larger content that will test stream position bug
            var data = new StringBuilder();

            // First chunk: "This is a long test string that " (33 bytes)
            data.Append("21;chunk-signature=chunk1sig\r\n");
            data.Append("This is a long test s\r\n");

            // Second chunk: "tring that will be " (19 bytes)
            data.Append("13;chunk-signature=chunk2sig\r\n");
            data.Append("tring that wi\r\n");

            // Third chunk: "split across multiple " (22 bytes)
            data.Append("16;chunk-signature=chunk3sig\r\n");
            data.Append("ll be split acro\r\n");

            // Fourth chunk: "chunks." (7 bytes)
            data.Append("7;chunk-signature=chunk4sig\r\n");
            data.Append("ss mult\r\n");

            // Fifth chunk: "iple chunks." (12 bytes)
            data.Append("c;chunk-signature=chunk5sig\r\n");
            data.Append("iple chunks.\r\n");

            // Final chunk (0 bytes)
            data.Append("0;chunk-signature=finalsig\r\n");
            data.Append("\r\n");

            return Encoding.UTF8.GetBytes(data.ToString());
        }

        private byte[] CreateInvalidChunkedData()
        {
            // Create chunked data with invalid chunk size format
            var data = new StringBuilder();
            data.Append("INVALID;chunk-signature=chunk1sig\r\n");
            data.Append("Hello\r\n");

            return Encoding.UTF8.GetBytes(data.ToString());
        }

        private PipeReader CreatePipeReader(byte[] data)
        {
            var pipe = new Pipe();
            pipe.Writer.WriteAsync(data).AsTask().Wait();
            pipe.Writer.Complete();
            return pipe.Reader;
        }

        private PipeReader CreateSlowPipeReader(byte[] data, int chunkSize)
        {
            // Create a pipe reader that delivers data in small chunks to simulate
            // the scenario where chunk headers are parsed but not enough data
            // is available for the complete chunk
            var pipe = new Pipe();

            Task.Run(async () =>
            {
                try
                {
                    for (int i = 0; i < data.Length; i += chunkSize)
                    {
                        var actualChunkSize = Math.Min(chunkSize, data.Length - i);
                        var chunk = new ReadOnlyMemory<byte>(data, i, actualChunkSize);
                        await pipe.Writer.WriteAsync(chunk);
                        await pipe.Writer.FlushAsync();

                        // Small delay to simulate network latency
                        await Task.Delay(1);
                    }
                }
                finally
                {
                    pipe.Writer.Complete();
                }
            });

            return pipe.Reader;
        }

        private PipeReader CreateBugReproducingPipeReader(byte[] data)
        {
            // Create a pipe reader that reproduces the exact bug scenario:
            // - First read: only enough for the first chunk header
            // - Second read: the chunk data but not enough for the next header
            // - Third read: the rest
            var pipe = new Pipe();

            Task.Run(async () =>
            {
                try
                {
                    // Read 1: Just the first header "15;chunk-signature=chunk1sig\r\n" (29 bytes)
                    await pipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(data, 0, 29));
                    await pipe.Writer.FlushAsync();
                    await Task.Delay(1);

                    // Read 2: The chunk data + start of next header "This is a long test s\r\nd;chunk-" (30 bytes)
                    await pipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(data, 29, 30));
                    await pipe.Writer.FlushAsync();
                    await Task.Delay(1);

                    // Read 3: The rest
                    await pipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(data, 59, data.Length - 59));
                    await pipe.Writer.FlushAsync();
                }
                finally
                {
                    pipe.Writer.Complete();
                }
            });

            return pipe.Reader;
        }
    }
}