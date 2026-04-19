using Lamina.WebApi.Streaming.Validation;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Lamina.WebApi.Tests.Streaming
{
    public class ChunkSignatureValidatorUnsignedTests
    {
        private static ChunkSignatureValidator CreateUnsignedValidator()
        {
            var signingKey = new byte[32];
            var logger = new Mock<ILogger>().Object;
            return new ChunkSignatureValidator(
                signingKey,
                new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc),
                "us-east-1",
                expectedDecodedLength: 1024,
                seedSignature: "seed",
                logger,
                expectsTrailers: true,
                expectedTrailerNames: new List<string> { "x-amz-checksum-crc32c" },
                isUnsignedChunks: true);
        }

        [Fact]
        public async Task ValidateChunkStreamAsync_WhenUnsignedChunks_ReturnsTrueForAnySignature()
        {
            var validator = CreateUnsignedValidator();
            var chunkData = System.Text.Encoding.UTF8.GetBytes("Hello World");
            using var stream = new MemoryStream(chunkData);

            // With unsigned chunks, even a completely wrong signature should be accepted
            var result = await validator.ValidateChunkStreamAsync(stream, chunkData.Length, "wrong-signature", false);

            Assert.True(result);
        }

        [Fact]
        public void ValidateChunk_WhenUnsignedChunks_ReturnsTrueForAnySignature()
        {
            var validator = CreateUnsignedValidator();
            var chunkData = System.Text.Encoding.UTF8.GetBytes("Hello World").AsMemory();

            var result = validator.ValidateChunk(chunkData, "wrong-signature", false);

            Assert.True(result);
        }

        [Fact]
        public async Task ValidateChunkStreamAsync_WhenUnsignedChunks_IncrementsChunkIndex()
        {
            var validator = CreateUnsignedValidator();
            using var stream1 = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("chunk1"));
            using var stream2 = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("chunk2"));

            await validator.ValidateChunkStreamAsync(stream1, 6, string.Empty, false);
            await validator.ValidateChunkStreamAsync(stream2, 6, string.Empty, false);

            Assert.Equal(2, validator.ChunkIndex);
        }

        [Fact]
        public async Task ValidateChunkStreamAsync_WhenSignedMode_RejectsWrongSignature()
        {
            // Signed mode (isUnsignedChunks = false, default) should still reject bad signatures
            var signingKey = new byte[32];
            var logger = new Mock<ILogger>().Object;
            var validator = new ChunkSignatureValidator(
                signingKey,
                new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc),
                "us-east-1",
                expectedDecodedLength: 1024,
                seedSignature: "seed",
                logger);

            var chunkData = System.Text.Encoding.UTF8.GetBytes("Hello");
            using var stream = new MemoryStream(chunkData);

            var result = await validator.ValidateChunkStreamAsync(stream, chunkData.Length, "wrong-signature", false);

            Assert.False(result);
        }
    }
}
