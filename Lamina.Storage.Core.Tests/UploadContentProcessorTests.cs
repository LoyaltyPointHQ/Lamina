using System.IO.Pipelines;
using System.Security.Cryptography;
using Lamina.Core.Streaming;
using Lamina.Core.Models;
using Lamina.Storage.Core.Helpers;
using Moq;

namespace Lamina.Storage.Core.Tests;

public class UploadContentProcessorTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(123456)]
    public async Task Process_UsesOnePassAndLeavesDestinationOpen(int length)
    {
        var bytes = Enumerable.Range(0, length).Select(i => (byte)i).ToArray();
        var request = new ChecksumRequest
        {
            ProvidedChecksums = new() { ["CRC32"] = "", ["CRC32C"] = "", ["CRC64NVME"] = "", ["SHA1"] = "", ["SHA256"] = "" }
        };
        using var destination = new WriteOnlyDestination();
        var reader = PipeReader.Create(new MemoryStream(bytes));
        var processor = new UploadContentProcessor(Mock.Of<IChunkedDataParser>());
        var result = await processor.ProcessAsync(reader, destination, null, request, MD5.HashData(bytes));
        Assert.True(result.IsSuccess);
        Assert.Equal(Convert.ToHexString(MD5.HashData(bytes)).ToLowerInvariant(), result.Value!.ETag);
        Assert.Equal(5, result.Value.Checksums.Count);
        Assert.Equal(Convert.ToBase64String(SHA256.HashData(bytes)), result.Value.Checksums["SHA256"]);
        Assert.Equal(length, result.Value.Size);
        Assert.True(destination.CanWrite);
        Assert.Equal(bytes, destination.ToArray());
    }

    [Theory]
    [InlineData(true, "BadDigest")]
    [InlineData(false, "InvalidChecksum")]
    public async Task Process_RejectsInvalidIntegrity(bool invalidMd5, string expectedError)
    {
        using var destination = new MemoryStream();
        var reader = PipeReader.Create(new MemoryStream("payload"u8.ToArray()));
        var request = invalidMd5 ? null : new ChecksumRequest { ProvidedChecksums = new() { ["SHA256"] = "invalid" } };
        var result = await new UploadContentProcessor(Mock.Of<IChunkedDataParser>()).ProcessAsync(
            reader, destination, null, request, invalidMd5 ? new byte[16] : null);
        Assert.Equal(expectedError, result.ErrorCode);
        Assert.True(destination.CanWrite);
    }

    [Fact]
    public async Task Process_CancellationPropagates()
    {
        using var destination = new MemoryStream();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            new UploadContentProcessor(Mock.Of<IChunkedDataParser>()).ProcessAsync(
                PipeReader.Create(new MemoryStream([1])), destination, null, null, null, cancellation.Token));
    }
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Process_SignedTrailersHashDecodedCallbacksAndRejectCorruption(bool empty, bool corrupt)
    {
        var bytes = empty ? Array.Empty<byte>() : "decoded payload"u8.ToArray();
        var digest = Convert.ToBase64String(SHA256.HashData(bytes));
        var validator = new Mock<IChunkSignatureValidator>();
        validator.SetupGet(x => x.ExpectsTrailers).Returns(true);
        validator.SetupGet(x => x.ExpectedTrailerNames).Returns(["x-amz-checksum-sha256"]);
        var parser = new Mock<IChunkedDataParser>(MockBehavior.Strict);
        parser.Setup(x => x.ParseChunkedDataWithTrailersToStreamAsync(It.IsAny<PipeReader>(), It.IsAny<Stream>(), validator.Object,
                It.IsAny<Action<ReadOnlySpan<byte>>>(), It.IsAny<CancellationToken>()))
            .Returns(async (PipeReader source, Stream target, IChunkSignatureValidator validation, Action<ReadOnlySpan<byte>> append, CancellationToken ct) =>
            {
                // Encoded source bytes must never be fed to the integrity calculator.
                var split = bytes.Length / 2;
                await target.WriteAsync(bytes.AsMemory(0, split), ct);
                append(bytes.AsSpan(0, split));
                await target.WriteAsync(bytes.AsMemory(split), ct);
                append(bytes.AsSpan(split));
                return new ChunkedDataResult
                {
                    Success = true,
                    TotalBytesWritten = bytes.Length,
                    Trailers = [new StreamingTrailer { Name = "x-amz-checksum-sha256", Value = corrupt ? Convert.ToBase64String(new byte[32]) : digest }]
                };
            });
        using var destination = new WriteOnlyDestination();
        var encoded = new MemoryStream("encoding framing and signature, not object bytes"u8.ToArray());
        var request = TrailerChecksumMerger.RegisterExpectedTrailers(validator.Object.ExpectedTrailerNames, null);
        var result = await new UploadContentProcessor(parser.Object).ProcessAsync(PipeReader.Create(encoded), destination, validator.Object, request, null);
        Assert.Equal(!corrupt, result.IsSuccess);
        if (corrupt) Assert.Equal("InvalidChecksum", result.ErrorCode);
        else
        {
            Assert.Equal(bytes.Length, result.Value!.Size);
            Assert.Equal(Convert.ToHexString(MD5.HashData(bytes)).ToLowerInvariant(), result.Value.ETag);
            Assert.Equal(digest, result.Value.Checksums["SHA256"]);
        }
        Assert.Equal(bytes, destination.ToArray());
        Assert.True(destination.CanWrite);
        Assert.False(encoded.CanRead);
        parser.VerifyAll();
    }

    [Fact]
    public async Task Process_ParserRejectsSignature_LeavesDestinationBorrowedAndCompletesSource()
    {
        var validator = Mock.Of<IChunkSignatureValidator>();
        var parser = new Mock<IChunkedDataParser>();
        parser.Setup(x => x.ParseChunkedDataToStreamAsync(It.IsAny<PipeReader>(), It.IsAny<Stream>(), validator,
                It.IsAny<Action<ReadOnlySpan<byte>>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ChunkedDataResult { Success = false });
        using var destination = new WriteOnlyDestination();
        var source = new MemoryStream("invalid signed encoding"u8.ToArray());
        var result = await new UploadContentProcessor(parser.Object).ProcessAsync(PipeReader.Create(source), destination, validator, null, null);
        Assert.Equal("SignatureDoesNotMatch", result.ErrorCode);
        Assert.Empty(destination.ToArray());
        Assert.True(destination.CanWrite);
        Assert.False(source.CanRead);
    }

    [Fact]
    public async Task Process_DestinationWriteFailurePropagatesWithoutClosingBorrowedStream()
    {
        using var destination = new WriteOnlyDestination(failWrites: true);
        var source = new MemoryStream("data"u8.ToArray());
        await Assert.ThrowsAsync<IOException>(() => new UploadContentProcessor(null).ProcessAsync(
            PipeReader.Create(source), destination, null, null, null));
        Assert.True(destination.CanWrite);
        Assert.False(source.CanRead);
    }

    private sealed class WriteOnlyDestination(bool failWrites = false) : Stream
    {
        private readonly MemoryStream _bytes = new();
        public byte[] ToArray() => _bytes.ToArray();
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => _bytes.CanWrite;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => _bytes.Flush();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException("Processor must not reread destination");
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => _bytes.Write(buffer, offset, count);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => failWrites ? ValueTask.FromException(new IOException("Injected destination failure")) : _bytes.WriteAsync(buffer, cancellationToken);
        protected override void Dispose(bool disposing)
        {
            if (disposing) _bytes.Dispose();
            base.Dispose(disposing);
        }
    }
}
