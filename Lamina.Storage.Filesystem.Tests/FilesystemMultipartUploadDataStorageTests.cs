using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace Lamina.Storage.Filesystem.Tests;

public class FilesystemMultipartUploadDataStorageTests : IDisposable
{
    private readonly string _testDataDirectory;
    private readonly string _testMetadataDirectory;
    private readonly FilesystemMultipartUploadDataStorage _storage;

    public FilesystemMultipartUploadDataStorageTests()
    {
        _testDataDirectory = Path.Combine(Path.GetTempPath(), $"lamina_mpu_test_{Guid.NewGuid():N}");
        _testMetadataDirectory = Path.Combine(Path.GetTempPath(), $"lamina_mpu_test_meta_{Guid.NewGuid():N}");

        var settings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = _testDataDirectory,
            MetadataDirectory = _testMetadataDirectory,
            MetadataMode = MetadataStorageMode.SeparateDirectory,
            InlineMetadataDirectoryName = ".lamina-meta",
            NetworkMode = NetworkFileSystemMode.None
        });

        var networkHelper = new NetworkFileSystemHelper(settings, NullLogger<NetworkFileSystemHelper>.Instance);
        _storage = new FilesystemMultipartUploadDataStorage(settings, networkHelper, NullLogger<FilesystemMultipartUploadDataStorage>.Instance);
    }

    public void Dispose()
    {
        TryDelete(_testDataDirectory);
        TryDelete(_testMetadataDirectory);
    }

    private static void TryDelete(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task HasAnyPartsAsync_ReturnsFalse_WhenUploadDirectoryMissing()
    {
        var hasAny = await _storage.HasAnyPartsAsync("bucket", "key", "nonexistent-upload");
        Assert.False(hasAny);
    }

    [Fact]
    public async Task HasAnyPartsAsync_ReturnsTrue_AfterPartStored()
    {
        const string bucketName = "bucket";
        const string key = "object";
        const string uploadId = "upload-123";

        await StorePartAsync(bucketName, key, uploadId, partNumber: 1, content: "hello");

        var hasAny = await _storage.HasAnyPartsAsync(bucketName, key, uploadId);
        Assert.True(hasAny);
    }

    [Fact]
    public async Task GetStoredPartsAsync_ReturnsRawFactsWithoutHashing()
    {
        await StorePartAsync("bucket", "key", "upload", 1, "hello");
        var part = Assert.Single(await _storage.GetStoredPartsAsync("bucket", "key", "upload"));
        Assert.Equal(1, part.PartNumber);
        Assert.Equal(5, part.Size);
        Assert.Empty(part.ETag);
        Assert.Null(part.ChecksumSHA256);
    }

    [Fact]
    public async Task AbortedOverwrite_PreservesPublishedPart()
    {
        await StorePartAsync("bucket", "key", "upload", 1, "original");
        await using (var write = await _storage.BeginPartWriteAsync("bucket", "key", "upload", 1))
        {
            await write.Stream.WriteAsync("invalid upload"u8.ToArray());
            using var prepared = await write.SealAsync();
            await _storage.AbortPreparedPartAsync(prepared);
        }
        var readers = await _storage.GetPartReadersAsync("bucket", "key", "upload", [new CompletedPart { PartNumber = 1, ETag = "unused" }]);
        var bytes = await PipeReaderHelper.ReadAllBytesAsync(Assert.Single(readers), true);
        Assert.Equal("original"u8.ToArray(), bytes);
        Assert.Empty(Directory.GetFiles(_testMetadataDirectory, ".lamina-tmp-*", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task ConcurrentStaging_IsInvisibleAndAbortDoesNotDiscardOtherWrite()
    {
        await using var first = await _storage.BeginPartWriteAsync("bucket", "key", "upload", 1);
        await using var second = await _storage.BeginPartWriteAsync("bucket", "key", "upload", 1);
        await first.Stream.WriteAsync("first"u8.ToArray());
        await second.Stream.WriteAsync("second"u8.ToArray());
        using var a = await first.SealAsync();
        using var b = await second.SealAsync();
        Assert.NotEqual(a.Tag, b.Tag);
        Assert.False(await _storage.HasAnyPartsAsync("bucket", "key", "upload"));
        Assert.Empty(await _storage.GetStoredPartsAsync("bucket", "key", "upload"));
        await _storage.AbortPreparedPartAsync(a);
        await _storage.CommitPreparedPartAsync("bucket", "key", "upload", 1, b);
        var readers = await _storage.GetPartReadersAsync("bucket", "key", "upload", [new CompletedPart { PartNumber = 1, ETag = "unused" }]);
        Assert.Equal("second"u8.ToArray(), await PipeReaderHelper.ReadAllBytesAsync(Assert.Single(readers), true));
    }

    [Fact]
    public async Task ConcurrentStoreAndGetStoredParts_DoNotCollideOnPartFileLock()
    {
        // Regression: a part being written holds an exclusive file lock (FileShare.None) for the whole
        // write. A concurrent GetStoredPartsAsync with no metadata hint recomputes ETag by opening the
        // same file (FileShare.Read), which collides with the writer's lock and throws
        // IOException "the process cannot access the file because it is being used by another process".
        // This reproduces the production UploadPartCopy failure (parallel copy + idempotent ListParts).
        const string bucketName = "bucket";
        const string key = "object";
        const string uploadId = "upload-concurrent";
        const int partCount = 6;

        // ~4MB per part so the write window is wide enough to overlap concurrent reads.
        var payload = new byte[4 * 1024 * 1024];
        new Random(12345).NextBytes(payload);

        // Seed all parts once so the listers always have files to read.
        foreach (var pn in Enumerable.Range(1, partCount))
        {
            await StorePartBytesAsync(bucketName, key, uploadId, pn, payload);
        }

        var exceptions = new System.Collections.Concurrent.ConcurrentQueue<Exception>();
        using var cts = new CancellationTokenSource();

        // Continuous raw part listings while published files are atomically replaced.
        var listers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    await _storage.GetStoredPartsAsync(bucketName, key, uploadId);
                }
                catch (Exception ex)
                {
                    exceptions.Enqueue(ex);
                    return;
                }
            }
        })).ToArray();

        try
        {
            // Repeatedly overwrite all parts concurrently while the listers spin.
            for (int iteration = 0; iteration < 40 && exceptions.IsEmpty; iteration++)
            {
                await Parallel.ForEachAsync(
                    Enumerable.Range(1, partCount),
                    new ParallelOptions { MaxDegreeOfParallelism = partCount },
                    async (pn, ct) =>
                    {
                        try
                        {
                            await StorePartBytesAsync(bucketName, key, uploadId, pn, payload);
                        }
                        catch (Exception ex)
                        {
                            exceptions.Enqueue(ex);
                        }
                    });
            }
        }
        finally
        {
            cts.Cancel();
            await Task.WhenAll(listers);
        }

        Assert.True(exceptions.IsEmpty,
            $"Concurrent store/list collided: {string.Join("; ", exceptions.Select(e => $"{e.GetType().Name}: {e.Message}"))}");
    }

    private Task<UploadPart> StorePartAsync(string bucketName, string key, string uploadId, int partNumber, string content)
        => StorePartBytesAsync(bucketName, key, uploadId, partNumber, System.Text.Encoding.UTF8.GetBytes(content));

    private async Task<UploadPart> StorePartBytesAsync(string bucketName, string key, string uploadId, int partNumber, byte[] content)
    {
        await using var write = await _storage.BeginPartWriteAsync(bucketName, key, uploadId, partNumber);
        await write.Stream.WriteAsync(content);
        using var prepared = await write.SealAsync();
        await _storage.CommitPreparedPartAsync(bucketName, key, uploadId, partNumber, prepared);
        return new UploadPart { PartNumber = partNumber, Size = content.Length, ETag = string.Empty };
    }
}
