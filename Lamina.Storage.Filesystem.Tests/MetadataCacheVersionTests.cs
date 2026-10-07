using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Lamina.Storage.Filesystem.Tests;

public class MetadataCacheVersionTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), $"lamina-cache-version-{Guid.NewGuid():N}");
    private readonly MemoryCache _cache = new(new MemoryCacheOptions());
    private readonly GatedLockManager _locks = new();

    [Theory]
    [InlineData(MetadataStorageMode.Inline)]
    [InlineData(MetadataStorageMode.SeparateDirectory)]
    public async Task ReadCompletingAfterConcurrentTagUpdate_DoesNotCacheOldTagsAsCurrent(MetadataStorageMode mode)
    {
        var storage = CreateStorage(mode);
        await SeedAsync(storage);
        _cache.Compact(1.0);
        _locks.PauseRead = true;
        var read = storage.GetMetadataAsync("bucket", "key");
        await _locks.Paused.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            Assert.True(await storage.SetObjectTagsAsync("bucket", "key", new() { ["version"] = "new" }));
        }
        finally { _locks.Resume.TrySetResult(); }
        Assert.Equal("old", (await read)!.Metadata.Tags["version"]);
        Assert.Equal("new", (await storage.GetMetadataAsync("bucket", "key"))!.Metadata.Tags["version"]);
    }

    [Theory]
    [InlineData(MetadataStorageMode.Inline)]
    [InlineData(MetadataStorageMode.SeparateDirectory)]
    public async Task StoreCompletingAfterConcurrentTagUpdate_DoesNotWarmCacheWithOldTags(MetadataStorageMode mode)
    {
        var storage = CreateStorage(mode);
        _locks.PauseWrite = true;
        var write = SeedAsync(storage);
        await _locks.Paused.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            Assert.True(await storage.SetObjectTagsAsync("bucket", "key", new() { ["version"] = "new" }));
        }
        finally { _locks.Resume.TrySetResult(); }
        await write;
        Assert.Equal("new", (await storage.GetMetadataAsync("bucket", "key"))!.Metadata.Tags["version"]);
    }

    private static Task<S3Object?> SeedAsync(IObjectMetadataStorage storage) => storage.StoreMetadataAsync("bucket", "key", "etag", 4,
        new PutObjectRequest { Key = "key", Tags = new() { ["version"] = "old" } }, lastModified: DateTime.UtcNow);

    private IObjectMetadataStorage CreateStorage(MetadataStorageMode mode)
    {
        var options = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = Path.Combine(_root, "data"),
            MetadataDirectory = Path.Combine(_root, "metadata"),
            MetadataMode = mode
        });
        var bucket = new Mock<IBucketStorageFacade>();
        bucket.Setup(x => x.BucketExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(true);
        var data = new Mock<IObjectDataStorage>();
        data.Setup(x => x.DataExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(true);
        var cacheOptions = Options.Create(new MetadataCacheSettings { Enabled = true });
        var network = new NetworkFileSystemHelper(options, NullLogger<NetworkFileSystemHelper>.Instance);
        return mode == MetadataStorageMode.Inline
            ? new InlineObjectMetadataStorage(options, cacheOptions, bucket.Object, data.Object, _locks, network,
                NullLogger<InlineObjectMetadataStorage>.Instance, _cache)
            : new SeparateDirectoryObjectMetadataStorage(options, cacheOptions, bucket.Object, data.Object, _locks, network,
                NullLogger<SeparateDirectoryObjectMetadataStorage>.Instance, _cache);
    }

    public void Dispose()
    {
        _cache.Dispose();
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }

    private sealed class GatedLockManager : IFileSystemLockManager
    {
        private readonly InMemoryLockManager _inner = new();
        public bool PauseRead { get; set; }
        public bool PauseWrite { get; set; }
        public TaskCompletionSource Paused { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Resume { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<T?> ReadFileAsync<T>(string path, Func<string, Task<T>> operation, CancellationToken ct = default)
        {
            var result = await _inner.ReadFileAsync(path, operation, ct);
            if (PauseRead)
            {
                PauseRead = false;
                Paused.TrySetResult();
                await Resume.Task.WaitAsync(ct);
            }
            return result;
        }

        public async Task WriteFileAsync(string path, string content, CancellationToken ct = default)
        {
            await _inner.WriteFileAsync(path, content, ct);
            if (PauseWrite)
            {
                PauseWrite = false;
                Paused.TrySetResult();
                await Resume.Task.WaitAsync(ct);
            }
        }

        public Task<bool> DeleteFile(string path) => _inner.DeleteFile(path);

        public async Task<bool> UpdateFileAsync(string path, Func<string?, Task<string?>> transform, CancellationToken ct = default)
        {
            var originalStamp = File.GetLastWriteTimeUtc(path);
            var updated = await _inner.UpdateFileAsync(path, transform, ct);
            // Make the two filesystem versions distinct even on coarse timestamp filesystems.
            if (updated) File.SetLastWriteTimeUtc(path, originalStamp.AddSeconds(1));
            return updated;
        }
    }
}
