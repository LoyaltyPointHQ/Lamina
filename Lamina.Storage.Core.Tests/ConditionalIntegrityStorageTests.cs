using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Core.Integrity;
using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Lamina.Storage.InMemory;
using Lamina.Storage.Sql;
using Lamina.Storage.Sql.Context;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Lamina.Storage.Core.Tests;

public class ConditionalIntegrityStorageTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "lamina-conditional-" + Guid.NewGuid().ToString("N"));
    private LaminaDbContext? _context;

    [Theory]
    [InlineData("memory")]
    [InlineData("inline")]
    [InlineData("separate")]
    [InlineData("sqlite")]
    [InlineData("xattr")]
    public async Task CreateAndRefresh_AreConditional_AndPreserveCurrentUserFields(string backend)
    {
        var store = Create(backend);
        var conditional = Assert.IsAssignableFrom<IConditionalObjectIntegrityStorage>(store);
        var timestamp = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var create = new ObjectIntegrityWrite("bucket", "key", 4, timestamp, null, "first",
            new Dictionary<string, string> { ["SHA256"] = "checksum" }, "text/plain");
        Assert.Equal(IntegrityWriteResult.Created, await conditional.TryWriteIntegrityAsync(create));
        Assert.Equal(IntegrityWriteResult.Conflict, await conditional.TryWriteIntegrityAsync(create));
        var snapshot = (await store.GetMetadataAsync("bucket", "key"))!;
        Assert.Equal("text/plain", snapshot.Metadata.ContentType);
        Assert.Equal("checksum", snapshot.Metadata.ChecksumSHA256);
        Assert.True(await store.SetObjectTagsAsync("bucket", "key", new() { ["current"] = "tag" }));
        var refresh = create with
        {
            Expected = ObjectIntegrityState.FromSnapshot(snapshot),
            ETag = "second",
            DataLastModified = timestamp.AddSeconds(1),
            ContentType = "must/not/replace"
        };
        Assert.Equal(IntegrityWriteResult.Updated, await conditional.TryWriteIntegrityAsync(refresh));
        var updated = (await store.GetMetadataAsync("bucket", "key"))!;
        Assert.Equal("second", updated.Metadata.ETag);
        Assert.Equal("tag", updated.Metadata.Tags["current"]);
        Assert.Equal("text/plain", updated.Metadata.ContentType);
        Assert.Equal(IntegrityWriteResult.Conflict, await conditional.TryWriteIntegrityAsync(refresh));
        await store.DeleteMetadataAsync("bucket", "key");
        Assert.Equal(IntegrityWriteResult.Conflict, await conditional.TryWriteIntegrityAsync(refresh));
        Assert.Null(await store.GetMetadataAsync("bucket", "key"));
    }

    [Theory]
    [InlineData("memory")]
    [InlineData("inline")]
    [InlineData("separate")]
    [InlineData("sqlite")]
    [InlineData("xattr")]
    public async Task QueuedCreate_CannotReplaceMetadataWrittenByPut(string backend)
    {
        var store = Create(backend);
        var conditional = Assert.IsAssignableFrom<IConditionalObjectIntegrityStorage>(store);
        await store.StoreMetadataAsync("bucket", "key", "published", 4,
            new PutObjectRequest { ContentType = "custom/type", Metadata = new() { ["keep"] = "value" }, OwnerId = "owner" });
        var write = new ObjectIntegrityWrite("bucket", "key", 4, DateTime.UtcNow, null, "old",
            new Dictionary<string, string>(), "text/plain");
        Assert.Equal(IntegrityWriteResult.Conflict, await conditional.TryWriteIntegrityAsync(write));
        var current = (await store.GetMetadataAsync("bucket", "key"))!.Metadata;
        Assert.Equal("published", current.ETag);
        Assert.Equal("value", current.Metadata["keep"]);
        Assert.Equal("owner", current.OwnerId);
    }

    private IObjectMetadataStorage Create(string backend)
    {
        if (backend == "memory") return new InMemoryObjectMetadataStorage();
        if (backend == "sqlite")
        {
            _context = new LaminaDbContext(new DbContextOptionsBuilder<LaminaDbContext>().UseSqlite("Data Source=:memory:").Options);
            _context.Database.OpenConnection();
            _context.Database.EnsureCreated();
            return new SqlObjectMetadataStorage(_context, NullLogger<SqlObjectMetadataStorage>.Instance);
        }
        Directory.CreateDirectory(Path.Combine(_root, "data", "bucket"));
        File.WriteAllText(Path.Combine(_root, "data", "bucket", "key"), "body");
        var options = Options.Create(new FilesystemStorageSettings { DataDirectory = Path.Combine(_root, "data"), MetadataDirectory = Path.Combine(_root, "metadata") });
        var buckets = new Mock<IBucketStorageFacade>();
        buckets.Setup(x => x.BucketExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(true);
        var data = new Mock<IObjectDataStorage>();
        data.Setup(x => x.DataExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(true);
        data.Setup(x => x.GetDataInfoAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync((4L, DateTime.UtcNow));
        if (backend == "xattr") return new XattrObjectMetadataStorage(options, buckets.Object, data.Object, NullLogger<XattrObjectMetadataStorage>.Instance, NullLoggerFactory.Instance);
        var cache = Options.Create(new MetadataCacheSettings { Enabled = false });
        var network = new NetworkFileSystemHelper(options, NullLogger<NetworkFileSystemHelper>.Instance);
        return backend == "inline"
            ? new InlineObjectMetadataStorage(options, cache, buckets.Object, data.Object, new InMemoryLockManager(), network, NullLogger<InlineObjectMetadataStorage>.Instance)
            : new SeparateDirectoryObjectMetadataStorage(options, cache, buckets.Object, data.Object, new InMemoryLockManager(), network, NullLogger<SeparateDirectoryObjectMetadataStorage>.Instance);
    }

    public void Dispose()
    {
        _context?.Dispose();
        if (Directory.Exists(_root)) Directory.Delete(_root, true);
    }
}
