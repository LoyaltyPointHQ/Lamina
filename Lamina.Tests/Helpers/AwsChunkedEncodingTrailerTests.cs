using System.IO.Pipelines;
using System.Text;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using Lamina.Models;
using Lamina.Streaming.Chunked;
using Lamina.Streaming.Validation;

namespace Lamina.Tests.Helpers
{
    public class AwsChunkedEncodingTrailerTests
    {
        private readonly Mock<ILogger> _loggerMock;
        private readonly Mock<IChunkSignatureValidator> _chunkValidatorMock;
        private readonly IChunkedDataParser _chunkedDataParser;

        public AwsChunkedEncodingTrailerTests()
        {
            _loggerMock = new Mock<ILogger>();
            _chunkValidatorMock = new Mock<IChunkSignatureValidator>();
            _chunkedDataParser = new ChunkedDataParser(_loggerMock.Object);
        }

        [Fact]
        public async Task ParseChunkedDataWithTrailersToStreamAsync_ParsesTrailersSuccessfully()
        {
            // Arrange
            var chunkedData = CreateChunkedDataWithTrailers();
            var pipeReader = CreatePipeReader(chunkedData);
            var destinationStream = new MemoryStream();

            _chunkValidatorMock.Setup(v => v.ExpectsTrailers).Returns(true);
            _chunkValidatorMock.Setup(v => v.ExpectedTrailerNames)
                .Returns(new List<string> { "x-amz-checksum-crc32c" });

            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), It.IsAny<long>(), It.IsAny<string>(), false))
                .ReturnsAsync(true);

            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), 0, It.IsAny<string>(), true))
                .ReturnsAsync(true);

            var expectedTrailerResult = new TrailerValidationResult
            {
                IsValid = true,
                Trailers = new List<StreamingTrailer>
                {
                    new() { Name = "x-amz-checksum-crc32c", Value = "wdBDMA==" }
                }
            };
            _chunkValidatorMock.Setup(v => v.ValidateTrailerAsync(It.IsAny<List<StreamingTrailer>>(), It.IsAny<string>()))
                .ReturnsAsync(expectedTrailerResult);

            // Act
            var result = await _chunkedDataParser.ParseChunkedDataWithTrailersToStreamAsync(
                pipeReader, destinationStream, _chunkValidatorMock.Object);

            // Assert
            Assert.Equal(11, result.TotalBytesWritten); // "Hello World"
            Assert.True(result.TrailerValidationResult);
            Assert.Single(result.Trailers);
            Assert.Equal("x-amz-checksum-crc32c", result.Trailers[0].Name);
            Assert.Equal("wdBDMA==", result.Trailers[0].Value);
            Assert.Null(result.ErrorMessage);

            // Verify destination stream content
            destinationStream.Position = 0;
            var content = await new StreamReader(destinationStream).ReadToEndAsync();
            Assert.Equal("Hello World", content);
        }

        [Fact]
        public async Task ParseChunkedDataWithTrailersToStreamAsync_HandlesInvalidTrailerSignature()
        {
            // Arrange
            var chunkedData = CreateChunkedDataWithTrailers();
            var pipeReader = CreatePipeReader(chunkedData);
            var destinationStream = new MemoryStream();

            _chunkValidatorMock.Setup(v => v.ExpectsTrailers).Returns(true);
            _chunkValidatorMock.Setup(v => v.ExpectedTrailerNames)
                .Returns(new List<string> { "x-amz-checksum-crc32c" });

            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), It.IsAny<long>(), It.IsAny<string>(), false))
                .ReturnsAsync(true);

            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), 0, It.IsAny<string>(), true))
                .ReturnsAsync(true);

            var invalidTrailerResult = new TrailerValidationResult
            {
                IsValid = false,
                ErrorMessage = "Invalid trailer signature"
            };
            _chunkValidatorMock.Setup(v => v.ValidateTrailerAsync(It.IsAny<List<StreamingTrailer>>(), It.IsAny<string>()))
                .ReturnsAsync(invalidTrailerResult);

            // Act
            var result = await _chunkedDataParser.ParseChunkedDataWithTrailersToStreamAsync(
                pipeReader, destinationStream, _chunkValidatorMock.Object);

            // Assert
            Assert.Equal(11, result.TotalBytesWritten); // Data still written
            Assert.False(result.TrailerValidationResult);
            Assert.Equal("Invalid trailer signature", result.ErrorMessage);
        }

        [Fact]
        public async Task ParseChunkedDataWithTrailersToStreamAsync_WorksWithoutTrailers()
        {
            // Arrange
            var chunkedData = CreateChunkedDataWithoutTrailers();
            var pipeReader = CreatePipeReader(chunkedData);
            var destinationStream = new MemoryStream();

            _chunkValidatorMock.Setup(v => v.ExpectsTrailers).Returns(false);

            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), It.IsAny<long>(), It.IsAny<string>(), false))
                .ReturnsAsync(true);

            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), 0, It.IsAny<string>(), true))
                .ReturnsAsync(true);

            // Act
            var result = await _chunkedDataParser.ParseChunkedDataWithTrailersToStreamAsync(
                pipeReader, destinationStream, _chunkValidatorMock.Object);

            // Assert
            Assert.Equal(11, result.TotalBytesWritten); // "Hello World"
            Assert.Null(result.TrailerValidationResult);
            Assert.Empty(result.Trailers);
            Assert.Null(result.ErrorMessage);

            // Verify destination stream content
            destinationStream.Position = 0;
            var content = await new StreamReader(destinationStream).ReadToEndAsync();
            Assert.Equal("Hello World", content);
        }

        [Fact]
        public async Task ParseChunkedDataWithTrailersToStreamAsync_HandlesMultipleTrailers()
        {
            // Arrange
            var chunkedData = CreateChunkedDataWithMultipleTrailers();
            var pipeReader = CreatePipeReader(chunkedData);
            var destinationStream = new MemoryStream();

            _chunkValidatorMock.Setup(v => v.ExpectsTrailers).Returns(true);
            _chunkValidatorMock.Setup(v => v.ExpectedTrailerNames)
                .Returns(new List<string> { "x-amz-checksum-crc32c", "x-amz-checksum-sha256" });

            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), It.IsAny<long>(), It.IsAny<string>(), false))
                .ReturnsAsync(true);

            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), 0, It.IsAny<string>(), true))
                .ReturnsAsync(true);

            var expectedTrailerResult = new TrailerValidationResult
            {
                IsValid = true,
                Trailers = new List<StreamingTrailer>
                {
                    new() { Name = "x-amz-checksum-crc32c", Value = "wdBDMA==" },
                    new() { Name = "x-amz-checksum-sha256", Value = "abc123def456" }
                }
            };
            _chunkValidatorMock.Setup(v => v.ValidateTrailerAsync(It.IsAny<List<StreamingTrailer>>(), It.IsAny<string>()))
                .ReturnsAsync(expectedTrailerResult);

            // Act
            var result = await _chunkedDataParser.ParseChunkedDataWithTrailersToStreamAsync(
                pipeReader, destinationStream, _chunkValidatorMock.Object);

            // Assert
            Assert.Equal(5, result.TotalBytesWritten); // "Hello"
            Assert.True(result.TrailerValidationResult);
            Assert.Equal(2, result.Trailers.Count);
            Assert.Contains(result.Trailers, t => t.Name == "x-amz-checksum-crc32c" && t.Value == "wdBDMA==");
            Assert.Contains(result.Trailers, t => t.Name == "x-amz-checksum-sha256" && t.Value == "abc123def456");
        }

        [Fact]
        public async Task ParseChunkedDataWithTrailersToStreamAsync_HandlesChunkValidationFailure()
        {
            // Arrange
            var chunkedData = CreateChunkedDataWithTrailers();
            var pipeReader = CreatePipeReader(chunkedData);
            var destinationStream = new MemoryStream();

            _chunkValidatorMock.Setup(v => v.ExpectsTrailers).Returns(true);
            _chunkValidatorMock.SetupGet(v => v.ChunkIndex).Returns(1);

            // First chunk fails validation
            _chunkValidatorMock.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), It.IsAny<long>(), It.IsAny<string>(), false))
                .ReturnsAsync(false);

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await _chunkedDataParser.ParseChunkedDataWithTrailersToStreamAsync(
                    pipeReader, destinationStream, _chunkValidatorMock.Object));
        }

        [Fact]
        public async Task ParseChunkedDataWithTrailersToStreamAsync_HandlesEmptyStream()
        {
            // Arrange
            var pipeReader = CreatePipeReader(Array.Empty<byte>());
            var destinationStream = new MemoryStream();

            // Act
            var result = await _chunkedDataParser.ParseChunkedDataWithTrailersToStreamAsync(
                pipeReader, destinationStream, null);

            // Assert
            Assert.Equal(0, result.TotalBytesWritten);
            Assert.Null(result.TrailerValidationResult);
            Assert.Empty(result.Trailers);
            Assert.Null(result.ErrorMessage);
        }

        private byte[] CreateChunkedDataWithTrailers()
        {
            // Create chunked data: "Hello World" in two chunks + trailers
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

            // Trailers
            data.Append("x-amz-checksum-crc32c: wdBDMA==\r\n");
            data.Append("x-amz-trailer-signature: trailersig123\r\n");
            data.Append("\r\n");

            return Encoding.UTF8.GetBytes(data.ToString());
        }

        private byte[] CreateChunkedDataWithMultipleTrailers()
        {
            // Create chunked data: "Hello" in one chunk + multiple trailers
            var data = new StringBuilder();

            // First chunk: "Hello" (5 bytes)
            data.Append("5;chunk-signature=chunk1sig\r\n");
            data.Append("Hello\r\n");

            // Final chunk (0 bytes)
            data.Append("0;chunk-signature=finalsig\r\n");
            data.Append("\r\n");

            // Multiple trailers
            data.Append("x-amz-checksum-crc32c: wdBDMA==\r\n");
            data.Append("x-amz-checksum-sha256: abc123def456\r\n");
            data.Append("x-amz-trailer-signature: trailersig123\r\n");
            data.Append("\r\n");

            return Encoding.UTF8.GetBytes(data.ToString());
        }

        private byte[] CreateChunkedDataWithoutTrailers()
        {
            // Create chunked data: "Hello World" in two chunks without trailers
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

        private PipeReader CreatePipeReader(byte[] data)
        {
            var pipe = new Pipe();
            pipe.Writer.WriteAsync(data).AsTask().Wait();
            pipe.Writer.Complete();
            return pipe.Reader;
        }
    }
}