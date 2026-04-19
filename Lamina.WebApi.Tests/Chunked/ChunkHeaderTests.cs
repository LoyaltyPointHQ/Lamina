using Lamina.WebApi.Streaming.Chunked;
using Xunit;

namespace Lamina.WebApi.Tests.Streaming.Chunked
{
    public class ChunkHeaderTests
    {
        [Fact]
        public void ParseHeaderLine_SignedFormat_ParsesSizeAndSignature()
        {
            var header = ChunkHeader.ParseHeaderLine("b;chunk-signature=abc123");

            Assert.NotNull(header);
            Assert.Equal(11, header.Size);
            Assert.Equal("abc123", header.Signature);
            Assert.False(header.IsFinalChunk);
        }

        [Fact]
        public void ParseHeaderLine_UnsignedFormat_ParsesSizeWithEmptySignature()
        {
            var header = ChunkHeader.ParseHeaderLine("b");

            Assert.NotNull(header);
            Assert.Equal(11, header.Size);
            Assert.Equal(string.Empty, header.Signature);
            Assert.False(header.IsFinalChunk);
        }

        [Fact]
        public void ParseHeaderLine_UnsignedFinalChunk_ReturnsFinalChunk()
        {
            var header = ChunkHeader.ParseHeaderLine("0");

            Assert.NotNull(header);
            Assert.Equal(0, header.Size);
            Assert.Equal(string.Empty, header.Signature);
            Assert.True(header.IsFinalChunk);
        }

        [Fact]
        public void ParseHeaderLine_InvalidFormat_ReturnsNull()
        {
            var header = ChunkHeader.ParseHeaderLine("not-a-size;chunk-signature=abc");

            Assert.Null(header);
        }

        [Fact]
        public void ParseHeaderLine_SemicolonButNoChunkSignaturePrefix_ReturnsNull()
        {
            var header = ChunkHeader.ParseHeaderLine("b;something-else=abc");

            Assert.Null(header);
        }

        [Fact]
        public void TryParse_UnsignedChunkInBuffer_ParsesCorrectly()
        {
            var data = System.Text.Encoding.ASCII.GetBytes("b\r\nHello World\r\n");

            var result = ChunkHeader.TryParse(data, 0, data.Length);

            Assert.NotNull(result);
            Assert.Equal(11, result.Header.Size);
            Assert.Equal(string.Empty, result.Header.Signature);
            Assert.Equal(3, result.NewPosition); // "b\r\n" = 3 bytes
        }
    }
}
