using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using Lamina.Core.Streaming;
using Lamina.WebApi.Streaming.Chunked;
using Moq;
using Xunit;

namespace Lamina.WebApi.Tests.Streaming.Chunked;

public class SignedChunkedCompletionTests
{
    private const string Final = "0;chunk-signature=final\r\n\r\n";
    private const string Data = "3;chunk-signature=data\r\nabc\r\n";

    private static Mock<IChunkSignatureValidator> Validator(long length, bool validFinal = true)
    {
        var validator = new Mock<IChunkSignatureValidator>();
        validator.SetupGet(v => v.ExpectedDecodedLength).Returns(length);
        validator.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), It.IsAny<long>(), It.IsAny<string>(), false)).ReturnsAsync(true);
        validator.Setup(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), 0, It.IsAny<string>(), true)).ReturnsAsync(validFinal);
        return validator;
    }

    [Theory]
    [InlineData("", 0)]
    [InlineData(Data, 3)]
    public async Task CompleteStreamSucceeds(string data, long length)
    {
        using var destination = new MemoryStream();
        var reader = new SegmentedReader(data + Final);
        var result = await new ChunkedDataParser().ParseChunkedDataToStreamAsync(reader, destination, Validator(length).Object);
        Assert.True(result.Success, result.ErrorMessage);
        Assert.Equal(length, result.TotalBytesWritten);
        Assert.Equal(length, destination.Length);
        Assert.Equal(reader.ReadCount, reader.AdvanceCount);
    }

    [Theory]
    [InlineData("", 0)]
    [InlineData(Data, 3)]
    [InlineData("0;chunk-signature=final\r\n", 0)]
    [InlineData("0;chunk-signature=final\r\n\r", 0)]
    [InlineData("0;chunk-signature=final\r\nXX", 0)]
    [InlineData(Data + Final, 4)]
    [InlineData(Final, -1)]
    [InlineData(Final + "extra", 0)]
    [InlineData("3;chunk-signature=data\r\nabcXX" + Final, 3)]
    public async Task IncompleteOrInconsistentStreamFails(string body, long length)
    {
        using var destination = new MemoryStream();
        var reader = new SegmentedReader(body);
        var result = await new ChunkedDataParser().ParseChunkedDataToStreamAsync(reader, destination, Validator(length).Object);
        Assert.False(result.Success);
        Assert.NotEmpty(result.ErrorMessage!);
        Assert.Equal(reader.ReadCount, reader.AdvanceCount);
    }

    [Fact]
    public async Task ExcessChunkIsNotWritten()
    {
        using var destination = new MemoryStream();
        var result = await new ChunkedDataParser().ParseChunkedDataToStreamAsync(new SegmentedReader(Data + Final), destination, Validator(2).Object);
        Assert.False(result.Success);
        Assert.Equal(0, destination.Length);
    }

    [Fact]
    public async Task ExcessAcrossReadsDoesNotWriteSecondChunk()
    {
        using var destination = new MemoryStream();
        var result = await new ChunkedDataParser().ParseChunkedDataToStreamAsync(
            new SegmentedReader(Data, Data + Final), destination, Validator(5).Object);
        Assert.False(result.Success);
        Assert.Equal("abc", Encoding.ASCII.GetString(destination.ToArray()));
        Assert.Equal(3, result.TotalBytesWritten);
    }

    [Fact]
    public async Task InvalidFinalSignatureFails()
    {
        using var destination = new MemoryStream();
        var reader = new SegmentedReader(Final);
        var result = await new ChunkedDataParser().ParseChunkedDataToStreamAsync(reader, destination, Validator(0, false).Object);
        Assert.False(result.Success);
        Assert.Contains("Invalid final chunk signature", result.ErrorMessage);
        Assert.Equal(reader.ReadCount, reader.AdvanceCount);
    }

    [Fact]
    public async Task EveryFrameSplitValidatesExactlyOnce()
    {
        var body = Data + Final;
        for (var split = 1; split < body.Length; split++)
        {
            using var destination = new MemoryStream();
            var validator = Validator(3);
            var reader = new SegmentedReader(body[..split], body[split..]);
            var result = await new ChunkedDataParser().ParseChunkedDataToStreamAsync(reader, destination, validator.Object);
            Assert.True(result.Success, $"Split {split}: {result.ErrorMessage}");
            Assert.Equal("abc", Encoding.ASCII.GetString(destination.ToArray()));
            validator.Verify(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), 3, "data", false), Times.Once);
            validator.Verify(v => v.ValidateChunkStreamAsync(It.IsAny<Stream>(), 0, "final", true), Times.Once);
            Assert.Equal(reader.ReadCount, reader.AdvanceCount);
        }
    }

    [Fact]
    public async Task TrailingDataInLaterReadFails()
    {
        using var destination = new MemoryStream();
        var reader = new SegmentedReader(Final, "extra");
        var result = await new ChunkedDataParser().ParseChunkedDataToStreamAsync(reader, destination, Validator(0).Object);
        Assert.False(result.Success);
        Assert.Equal(2, reader.ReadCount);
        Assert.Equal(reader.ReadCount, reader.AdvanceCount);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CancellationIsPropagated(bool tokenCanceled)
    {
        using var destination = new MemoryStream();
        using var cancellation = new CancellationTokenSource();
        var reader = new SegmentedReader(Final) { CancelRead = !tokenCanceled };
        if (tokenCanceled) cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new ChunkedDataParser().ParseChunkedDataToStreamAsync(reader, destination, Validator(0).Object, cancellationToken: cancellation.Token));
        Assert.Equal(reader.ReadCount, reader.AdvanceCount);
    }

    [Fact]
    public async Task CancellationDuringLastWriteIsPropagated()
    {
        using var destination = new MemoryStream();
        using var cancellation = new CancellationTokenSource();
        var reader = new SegmentedReader(Data + Final);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new ChunkedDataParser().ParseChunkedDataToStreamAsync(
            reader, destination, Validator(3).Object, _ => cancellation.Cancel(), cancellation.Token));
        Assert.Equal(reader.ReadCount, reader.AdvanceCount);
    }

    // Deterministic read boundaries: unlike timing-based Pipe writers, these cannot coalesce.
    private sealed class SegmentedReader(params string[] segments) : PipeReader
    {
        private int _position;
        public int ReadCount { get; private set; }
        public int AdvanceCount { get; private set; }
        public bool CancelRead { get; init; }
        public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ReadCount++;
            var data = _position < segments.Length ? Encoding.ASCII.GetBytes(segments[_position++]) : [];
            return ValueTask.FromResult(new ReadResult(new ReadOnlySequence<byte>(data), CancelRead, _position >= segments.Length));
        }
        public override void AdvanceTo(SequencePosition consumed) => AdvanceCount++;
        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined) => AdvanceTo(consumed);
        public override void CancelPendingRead() => throw new NotSupportedException();
        public override void Complete(Exception? exception = null) { }
        public override bool TryRead(out ReadResult result) => throw new NotSupportedException();
    }
}
