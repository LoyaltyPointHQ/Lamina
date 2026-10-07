using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.InMemory;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Lamina.Storage.Core.Tests;

public class StagedDataIsolationTests
{
    private static InMemoryObjectDataStorage CreateStorage() => new(NullLogger<InMemoryObjectDataStorage>.Instance);

    [Fact]
    public async Task AbortingConcurrentWrite_DoesNotDiscardOtherPreparedWrite()
    {
        var storage = CreateStorage();
        await using var first = await storage.BeginWriteAsync("bucket", "key");
        await using var second = await storage.BeginWriteAsync("bucket", "key");
        await first.Stream.WriteAsync("first"u8.ToArray());
        await second.Stream.WriteAsync("second"u8.ToArray());
        using var firstData = await first.SealAsync();
        using var secondData = await second.SealAsync();
        Assert.NotEqual(firstData.Tag, secondData.Tag);
        Assert.False(await storage.DataExistsAsync("bucket", "key"));
        await storage.AbortPreparedDataAsync(firstData);
        await storage.CommitPreparedDataAsync(secondData);
        await using var published = await storage.OpenReadAsync("bucket", "key");
        Assert.Equal("second", await new StreamReader(published!).ReadToEndAsync());
    }

    [Fact]
    public async Task DisposingUncommittedOverwrite_PreservesOldObject()
    {
        var storage = CreateStorage();
        await storage.StoreDataAsync("bucket", "key", PipeReader.Create(new MemoryStream("old"u8.ToArray())));
        await using (var pending = await storage.BeginWriteAsync("bucket", "key"))
        {
            await pending.Stream.WriteAsync("new"u8.ToArray());
            using var prepared = await pending.SealAsync();
            await using var raw = await storage.OpenPreparedReadAsync(prepared);
            Assert.Equal("new", await new StreamReader(raw).ReadToEndAsync());
        }
        await using var published = await storage.OpenReadAsync("bucket", "key");
        Assert.Equal("old", await new StreamReader(published!).ReadToEndAsync());
    }

    [Fact]
    public async Task EmptyObject_HasReadableEmptyStream()
    {
        var storage = CreateStorage();
        await storage.StoreDataAsync("bucket", "key", PipeReader.Create(new MemoryStream()));
        await using var stream = await storage.OpenReadAsync("bucket", "key");
        Assert.NotNull(stream);
        Assert.Equal(0, stream.Length);
        var pipe = new Pipe();
        Assert.True(await storage.WriteDataToPipeAsync("bucket", "key", pipe.Writer));
        Assert.Empty(await PipeReaderHelper.ReadAllBytesAsync(pipe.Reader, true));
    }

    [Fact]
    public async Task ConcurrentPartStaging_IsPrivateAndAbortIsIsolated()
    {
        var storage = new InMemoryMultipartUploadDataStorage();
        await using var first = await storage.BeginPartWriteAsync("bucket", "key", "upload", 1);
        await using var second = await storage.BeginPartWriteAsync("bucket", "key", "upload", 1);
        await first.Stream.WriteAsync("first"u8.ToArray());
        await second.Stream.WriteAsync("second"u8.ToArray());
        using var a = await first.SealAsync();
        using var b = await second.SealAsync();
        Assert.False(await storage.HasAnyPartsAsync("bucket", "key", "upload"));
        await storage.AbortPreparedPartAsync(a);
        await storage.CommitPreparedPartAsync("bucket", "key", "upload", 1, b);
        var part = Assert.Single(await storage.GetStoredPartsAsync("bucket", "key", "upload"));
        Assert.Empty(part.ETag);
        var readers = await storage.GetPartReadersAsync("bucket", "key", "upload", [new CompletedPart { PartNumber = 1, ETag = "unused" }]);
        Assert.Equal("second"u8.ToArray(), await PipeReaderHelper.ReadAllBytesAsync(Assert.Single(readers), true));
    }

    [Fact]
    public async Task CancelledSeal_DoesNotPublish()
    {
        var storage = CreateStorage();
        await using var write = await storage.BeginWriteAsync("bucket", "key");
        await write.Stream.WriteAsync("bytes"u8.ToArray());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => write.SealAsync(new CancellationToken(true)));
        Assert.False(await storage.DataExistsAsync("bucket", "key"));
    }
    [Fact]
    public async Task FailedMultipartAssembly_DisposesUnreadPartStreams()
    {
        var storage = CreateStorage();
        var bad = new FailingReadStream();
        var untouched = new MemoryStream("unread"u8.ToArray());
        await Assert.ThrowsAsync<IOException>(() => storage.PrepareMultipartDataAsync("bucket", "key",
            [PipeReader.Create(bad), PipeReader.Create(untouched)]));
        Assert.False(untouched.CanRead);
        Assert.False(await storage.DataExistsAsync("bucket", "key"));
    }

    [Fact]
    public async Task FailedSeal_DoesNotCreatePreparedHandleAndDisposalCleansUp()
    {
        var stream = new FailingFlushStream();
        var cleaned = false;
        var sealedData = false;
        await using (var session = new StagedDataWrite(stream, size =>
        {
            sealedData = true;
            return new PreparedData { BucketName = "bucket", Key = "key", Size = size };
        }, () => cleaned = true))
        {
            await stream.WriteAsync("private bytes"u8.ToArray());
            await Assert.ThrowsAsync<IOException>(() => session.SealAsync());
            Assert.False(sealedData);
        }
        Assert.True(cleaned);
        Assert.False(stream.CanWrite);
    }

    private sealed class FailingFlushStream : MemoryStream
    {
        public override Task FlushAsync(CancellationToken cancellationToken)
            => Task.FromException(new IOException("Injected flush failure"));
    }

    private sealed class FailingReadStream : MemoryStream
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => ValueTask.FromException<int>(new IOException("Injected read failure"));
    }
}
