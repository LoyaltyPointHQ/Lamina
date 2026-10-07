using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Sql;
using Lamina.Storage.Sql.Context;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Moq;

namespace Lamina.Storage.Sql.Tests;

/// <summary>
/// Tests for raw metadata persistence; freshness resolution belongs to the facade.
/// </summary>
public class SqlObjectMetadataStorageStaleTests : IDisposable
{
    private readonly LaminaDbContext _context;
    private readonly Mock<ILogger<SqlObjectMetadataStorage>> _loggerMock;
    private readonly SqlObjectMetadataStorage _storage;

    public SqlObjectMetadataStorageStaleTests()
    {
        var options = new DbContextOptionsBuilder<LaminaDbContext>()
            .UseSqlite("Data Source=:memory:")
            .Options;

        _context = new LaminaDbContext(options);
        _context.Database.OpenConnection();
        _context.Database.EnsureCreated();

        _loggerMock = new Mock<ILogger<SqlObjectMetadataStorage>>();

        _storage = new SqlObjectMetadataStorage(_context, _loggerMock.Object);
    }

    [Fact]
    public async Task SingleAndBatch_ReturnStoredValuesWithoutReadingData()
    {
        var time = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        await _storage.StoreMetadataAsync("bucket", "key", "old", 7, null, new() { ["SHA256"] = "stored" }, time);
        var snapshot = await _storage.GetMetadataAsync("bucket", "key");
        var batch = await _storage.GetMetadataBatchAsync("bucket", ["key", "missing"]);
        Assert.NotNull(snapshot);
        Assert.Equal(time, snapshot.DataLastModified);
        Assert.Equal("old", snapshot.Metadata.ETag);
        Assert.Equal("stored", batch["key"]!.Metadata.ChecksumSHA256);
        Assert.Null(batch["missing"]);
    }

    [Fact]
    public async Task UpdateIntegrity_PersistsOnlyIntegrityAndDoesNotUpsert()
    {
        Assert.False(await _storage.UpdateIntegrityAsync("bucket", "missing", "etag", 1, DateTime.UtcNow, new()));
        await _storage.StoreMetadataAsync("bucket", "key", "old", 7,
            new PutObjectRequest { Key = "key", ContentType = "text/plain", Metadata = new() { ["user"] = "value" } },
            new() { ["SHA256"] = "old" });
        await _storage.SetObjectTagsAsync("bucket", "key", new() { ["tag"] = "latest" });
        var time = DateTime.UtcNow;
        Assert.True(await _storage.UpdateIntegrityAsync("bucket", "key", "new", 9, time, new() { ["CRC32"] = "crc" }));
        _context.ChangeTracker.Clear();
        var snapshot = (await _storage.GetMetadataAsync("bucket", "key"))!;
        Assert.Equal(time, snapshot.DataLastModified);
        Assert.Equal("new", snapshot.Metadata.ETag);
        Assert.Equal(9, snapshot.Metadata.Size);
        Assert.Equal("latest", snapshot.Metadata.Tags["tag"]);
        Assert.Equal("value", snapshot.Metadata.Metadata["user"]);
        Assert.Equal("text/plain", snapshot.Metadata.ContentType);
        Assert.Null(snapshot.Metadata.ChecksumSHA256);
        Assert.Equal("crc", snapshot.Metadata.ChecksumCRC32);
    }

    [Fact]
    public async Task StoreChecksums_NullFallsBackToRequestButEmptyClearsValues()
    {
        var request = new PutObjectRequest { Key = "key", ChecksumSHA256 = "request-sha" };
        await _storage.StoreMetadataAsync("bucket", "key", "etag", 1, request);
        Assert.Equal("request-sha", (await _storage.GetMetadataAsync("bucket", "key"))!.Metadata.ChecksumSHA256);
        await _storage.StoreMetadataAsync("bucket", "key", "etag", 1, request, new());
        Assert.Null((await _storage.GetMetadataAsync("bucket", "key"))!.Metadata.ChecksumSHA256);
    }

    public void Dispose()
    {
        _context.Database.CloseConnection();
        _context.Dispose();
    }
}

public sealed class PostgreSqlMetadataTimestampTests : IAsyncLifetime
{
    private readonly Testcontainers.PostgreSql.PostgreSqlContainer _container =
        new Testcontainers.PostgreSql.PostgreSqlBuilder("postgres:16-alpine").Build();

    public Task InitializeAsync() => _container.StartAsync();
    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    [Fact]
    public async Task StoreAndRefresh_RoundTripWatermarkDoesNotMoveBeforeDataTimestamp()
    {
        var options = new DbContextOptionsBuilder<LaminaDbContext>()
            .UseNpgsql(_container.GetConnectionString()).Options;
        var first = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(1);
        var updated = first.AddSeconds(1).AddTicks(6);
        SqlObjectMetadataStorage Storage(LaminaDbContext context) => new(context,
            Mock.Of<ILogger<SqlObjectMetadataStorage>>());
        await using (var context = new LaminaDbContext(options))
        {
            await context.Database.EnsureCreatedAsync();
            await Storage(context).StoreMetadataAsync("bucket", "key", "old", 4, lastModified: first);
        }
        await using (var context = new LaminaDbContext(options))
        {
            var storage = Storage(context);
            var snapshot = (await storage.GetMetadataAsync("bucket", "key"))!;
            Assert.Equal(first.AddTicks(9), snapshot.DataLastModified);
            Assert.True(await storage.UpdateIntegrityAsync("bucket", "key", "new", 5, updated, new() { ["SHA256"] = "sha" }));
        }
        await using (var context = new LaminaDbContext(options))
        {
            var snapshot = (await Storage(context).GetMetadataAsync("bucket", "key"))!;
            Assert.Equal(updated.AddTicks(3), snapshot.DataLastModified);
            Assert.Equal("new", snapshot.Metadata.ETag);
            Assert.Equal("sha", snapshot.Metadata.ChecksumSHA256);
        }
    }
}
