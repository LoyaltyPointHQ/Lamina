using Lamina.Storage.Sql.Context;
using Lamina.Storage.Sql.Entities;
using Microsoft.EntityFrameworkCore;

namespace Lamina.Storage.Sql.Tests;

public class MultipartUploadKeyStreamingTests
{
    [Fact]
    public async Task Enumeration_ProjectsOnlyKeys_FiltersBucket_AndDoesNotTrackEntities()
    {
        await using var context = new LaminaDbContext(new DbContextOptionsBuilder<LaminaDbContext>().UseSqlite("Data Source=:memory:").Options);
        await context.Database.OpenConnectionAsync();
        await context.Database.EnsureCreatedAsync();
        context.MultipartUploads.AddRange(
            new MultipartUploadEntity { UploadId = "1", BucketName = "bucket", Key = "key", MetadataJson = "invalid json" },
            new MultipartUploadEntity { UploadId = "2", BucketName = "bucket", Key = "key" },
            new MultipartUploadEntity { UploadId = "3", BucketName = "other", Key = "hidden" });
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();
        var storage = new SqlMultipartUploadMetadataStorage(context);
        var keys = new List<string>();
        await foreach (var key in storage.EnumerateUploadKeysAsync("bucket")) keys.Add(key);
        Assert.Equal(new[] { "key", "key" }, keys);
        Assert.Empty(context.ChangeTracker.Entries());
    }

    [Fact]
    public async Task Enumeration_ObservesCancellation()
    {
        await using var context = new LaminaDbContext(new DbContextOptionsBuilder<LaminaDbContext>().UseSqlite("Data Source=:memory:").Options);
        var storage = new SqlMultipartUploadMetadataStorage(context);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var unused in storage.EnumerateUploadKeysAsync("bucket", cancellation.Token)) { }
        });
    }
}
