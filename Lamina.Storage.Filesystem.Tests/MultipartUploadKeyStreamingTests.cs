using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Lamina.Storage.InMemory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem.Tests;

public sealed class MultipartUploadKeyStreamingTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "lamina-key-stream-" + Guid.NewGuid().ToString("N"));
    private readonly TestLockManager _locks = new();

    private FilesystemMultipartUploadMetadataStorage Create(MetadataStorageMode mode)
    {
        var options = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = _root,
            MetadataDirectory = Path.Combine(_root, "metadata"),
            MetadataMode = mode
        });
        return new(options, Options.Create(new MetadataCacheSettings { Enabled = false }),
            new NetworkFileSystemHelper(options, NullLogger<NetworkFileSystemHelper>.Instance),
            _locks, NullLogger<FilesystemMultipartUploadMetadataStorage>.Instance);
    }

    [Theory]
    [InlineData(MetadataStorageMode.Inline)]
    [InlineData(MetadataStorageMode.SeparateDirectory)]
    [InlineData(MetadataStorageMode.Xattr)]
    public async Task Filesystem_FiltersBucket_PreservesDuplicateKeys_AndReadsLazily(MetadataStorageMode mode)
    {
        var storage = Create(mode);
        await storage.InitiateUploadAsync("bucket", "wal/a", new());
        await storage.InitiateUploadAsync("bucket", "wal/a", new());
        await storage.InitiateUploadAsync("other", "hidden", new());
        var stream = storage.EnumerateUploadKeysAsync("bucket");
        Assert.Equal(0, _locks.ReadCount);
        var keys = new List<string>();
        await foreach (var key in stream) keys.Add(key);
        Assert.Equal(new[] { "wal/a", "wal/a" }, keys);
        Assert.Equal(3, _locks.ReadCount);
    }

    [Fact]
    public async Task Filesystem_MissingUploadDirectory_ReturnsEmpty()
    {
        var storage = Create(MetadataStorageMode.Inline);
        await foreach (var unused in storage.EnumerateUploadKeysAsync("bucket")) Assert.Fail("No uploads exist.");
        Assert.Equal(0, _locks.ReadCount);
    }

    [Fact]
    public async Task Filesystem_ProjectsOnlyBucketAndKey()
    {
        var storage = Create(MetadataStorageMode.Inline);
        var upload = await storage.InitiateUploadAsync("bucket", "key", new());
        var path = Path.Combine(_root, ".lamina-meta", "_multipart_uploads", upload.UploadId, "upload.metadata.json");
        await File.WriteAllTextAsync(path, """{"BucketName":"bucket","Key":"key","Metadata":42,"Parts":"not a dictionary"}""");
        var keys = new List<string>();
        await foreach (var key in storage.EnumerateUploadKeysAsync("bucket")) keys.Add(key);
        Assert.Equal(new[] { "key" }, keys);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Filesystem_PropagatesReadCancellationAndAccessErrors(bool cancelled)
    {
        var storage = Create(MetadataStorageMode.Inline);
        await storage.InitiateUploadAsync("bucket", "key", new());
        _locks.ReadException = cancelled ? new OperationCanceledException() : new UnauthorizedAccessException();
        var exception = await Record.ExceptionAsync(async () =>
        {
            await foreach (var unused in storage.EnumerateUploadKeysAsync("bucket")) { }
        });
        Assert.Same(_locks.ReadException, exception);
    }

    [Fact]
    public async Task Filesystem_StopAfterFirst_DoesNotReadRemainingMetadata()
    {
        var storage = Create(MetadataStorageMode.Inline);
        for (var i = 0; i < 10; i++) await storage.InitiateUploadAsync("bucket", "key" + i, new());
        await foreach (var unused in storage.EnumerateUploadKeysAsync("bucket")) break;
        Assert.Equal(1, _locks.ReadCount);
    }

    [Fact]
    public async Task Filesystem_SkipsDisappearedMetadata()
    {
        var storage = Create(MetadataStorageMode.Inline);
        await storage.InitiateUploadAsync("bucket", "key", new());
        _locks.ReadException = new FileNotFoundException();
        await foreach (var unused in storage.EnumerateUploadKeysAsync("bucket")) Assert.Fail("Missing upload must be skipped.");
    }

    [Fact]
    public async Task Filesystem_DoesNotSwallowIoErrors()
    {
        var storage = Create(MetadataStorageMode.Inline);
        await storage.InitiateUploadAsync("bucket", "key", new());
        _locks.ReadException = new IOException("test failure");
        await Assert.ThrowsAsync<IOException>(async () =>
        {
            await foreach (var unused in storage.EnumerateUploadKeysAsync("bucket")) { }
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Enumeration_ObservesCancellation(bool filesystem)
    {
        IMultipartUploadMetadataStorage storage = filesystem ? Create(MetadataStorageMode.Inline) : new InMemoryMultipartUploadMetadataStorage();
        await storage.InitiateUploadAsync("bucket", "key", new());
        using var cancellation = new CancellationTokenSource();
        await using var iterator = storage.EnumerateUploadKeysAsync("bucket", cancellation.Token).GetAsyncEnumerator();
        Assert.True(await iterator.MoveNextAsync());
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await iterator.MoveNextAsync());
    }

    [Fact]
    public async Task InMemory_FiltersBucketAndPreservesDuplicateKeys()
    {
        var storage = new InMemoryMultipartUploadMetadataStorage();
        await storage.InitiateUploadAsync("bucket", "key", new());
        await storage.InitiateUploadAsync("bucket", "key", new());
        await storage.InitiateUploadAsync("other", "hidden", new());
        var keys = new List<string>();
        await foreach (var key in storage.EnumerateUploadKeysAsync("bucket")) keys.Add(key);
        Assert.Equal(new[] { "key", "key" }, keys);
    }

    public void Dispose() { if (Directory.Exists(_root)) Directory.Delete(_root, true); }

    private sealed class TestLockManager : IFileSystemLockManager
    {
        public int ReadCount { get; private set; }
        public Exception? ReadException { get; set; }
        public async Task<T?> ReadFileAsync<T>(string path, Func<string, Task<T>> operation, CancellationToken cancellationToken = default)
        {
            ReadCount++;
            if (ReadException is not null) throw ReadException;
            return await operation(await File.ReadAllTextAsync(path, cancellationToken));
        }
        public Task WriteFileAsync(string path, string content, CancellationToken cancellationToken = default) => File.WriteAllTextAsync(path, content, cancellationToken);
        public Task<bool> DeleteFile(string path) { File.Delete(path); return Task.FromResult(true); }
        public Task<bool> UpdateFileAsync(string path, Func<string?, Task<string?>> transform, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
