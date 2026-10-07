using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem.Tests;

public sealed class FilesystemBucketDataStorageTests : IDisposable
{
    private const string BucketName = "test-bucket";
    private readonly string _root = Path.Combine(Path.GetTempPath(), $"lamina-bucket-delete-{Guid.NewGuid():N}");
    private string BucketPath => Path.Combine(_root, "data", BucketName);

    public static IEnumerable<object[]> Modes => Enum.GetValues<MetadataStorageMode>().Select(mode => new object[] { mode });

    public static IEnumerable<object[]> FileCases =>
        from mode in Enum.GetValues<MetadataStorageMode>()
        from path in new[] { "nested/object.bin", "nested/empty.bin", "nested/.hidden", ".lamina-meta/object.json", ".lamina-tmp-upload", "nested/.lamina-tmp-upload" }
        select new object[] { mode, path };

    private FilesystemBucketDataStorage CreateStorage(MetadataStorageMode mode)
    {
        var settings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = Path.Combine(_root, "data"),
            MetadataDirectory = Path.Combine(_root, "metadata"),
            MetadataMode = mode,
            NetworkMode = NetworkFileSystemMode.None
        });
        var helper = new NetworkFileSystemHelper(settings, NullLogger<NetworkFileSystemHelper>.Instance);
        return new FilesystemBucketDataStorage(settings, helper, NullLogger<FilesystemBucketDataStorage>.Instance);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task DeleteBucketAsync_EmptyBucket_Succeeds(MetadataStorageMode mode)
    {
        var storage = CreateStorage(mode);
        Directory.CreateDirectory(BucketPath);

        Assert.Equal(DeleteBucketResult.Success, await storage.DeleteBucketAsync(BucketName));
        Assert.False(Directory.Exists(BucketPath));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task DeleteBucketAsync_OnlyNestedEmptyDirectories_Succeeds(MetadataStorageMode mode)
    {
        var storage = CreateStorage(mode);
        Directory.CreateDirectory(Path.Combine(BucketPath, "first", "deep", "leaf"));
        Directory.CreateDirectory(Path.Combine(BucketPath, "first", "sibling"));
        Directory.CreateDirectory(Path.Combine(BucketPath, "second"));
        Directory.CreateDirectory(Path.Combine(BucketPath, ".lamina-meta", "empty"));

        Assert.Equal(DeleteBucketResult.Success, await storage.DeleteBucketAsync(BucketName));
        Assert.False(Directory.Exists(BucketPath));
    }

    [Theory]
    [MemberData(nameof(FileCases))]
    public async Task DeleteBucketAsync_AnyFile_RemainsNotEmptyAndPreservesBytes(MetadataStorageMode mode, string relativePath)
    {
        var storage = CreateStorage(mode);
        var path = Path.Combine(BucketPath, relativePath);
        var emptySibling = Path.Combine(BucketPath, "empty-sibling", "nested");
        Directory.CreateDirectory(emptySibling);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        byte[] content = relativePath.EndsWith("empty.bin", StringComparison.Ordinal) ? [] : [0, 1, 255, 13, 10];
        await File.WriteAllBytesAsync(path, content);

        Assert.Equal(DeleteBucketResult.NotEmpty, await storage.DeleteBucketAsync(BucketName));
        Assert.True(Directory.Exists(BucketPath));
        Assert.True(Directory.Exists(emptySibling));
        Assert.True(File.Exists(path));
        Assert.Equal(content, await File.ReadAllBytesAsync(path));
    }

    [Theory]
    [InlineData("directory")]
    [InlineData("file")]
    [InlineData("dangling")]
    [InlineData("bucket-root")]
    public async Task DeleteBucketAsync_SymbolicLink_IsNotEmptyAndPreservesTarget(string kind)
    {
        // Windows symlink creation requires privileges not provided by the test harness.
        if (OperatingSystem.IsWindows())
            return;

        var storage = CreateStorage(MetadataStorageMode.Inline);
        var target = Path.Combine(_root, "outside-bucket");
        Directory.CreateDirectory(target);
        var targetFile = Path.Combine(target, "preserved.bin");
        byte[] content = [1, 2, 3];
        await File.WriteAllBytesAsync(targetFile, content);

        string link;
        if (kind == "bucket-root")
        {
            // An empty root target must not be mistaken for an ordinary empty bucket.
            target = Path.Combine(_root, "empty-target");
            Directory.CreateDirectory(target);
            link = BucketPath;
            Directory.CreateSymbolicLink(link, target);
        }
        else
        {
            Directory.CreateDirectory(Path.Combine(BucketPath, "nested"));
            link = Path.Combine(BucketPath, "nested", "link");
            if (kind == "directory")
            {
                target = Path.Combine(_root, "empty-target");
                Directory.CreateDirectory(target);
                Directory.CreateSymbolicLink(link, target);
            }
            else
                File.CreateSymbolicLink(link, kind == "file" ? targetFile : Path.Combine(target, "missing"));
        }

        Assert.Equal(DeleteBucketResult.NotEmpty, await storage.DeleteBucketAsync(BucketName));
        Assert.NotNull(new FileInfo(link).LinkTarget);
        Assert.True(Directory.Exists(target));
        Assert.Equal(content, await File.ReadAllBytesAsync(targetFile));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task DeleteBucketAsync_Force_DeletesPopulatedBucket(MetadataStorageMode mode)
    {
        var storage = CreateStorage(mode);
        Directory.CreateDirectory(Path.Combine(BucketPath, "nested"));
        await File.WriteAllBytesAsync(Path.Combine(BucketPath, "nested", "object.bin"), [1, 2, 3]);

        Assert.Equal(DeleteBucketResult.Success, await storage.DeleteBucketAsync(BucketName, force: true));
        Assert.False(Directory.Exists(BucketPath));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task DeleteBucketAsync_MissingBucket_ReturnsNotFound(MetadataStorageMode mode)
    {
        var storage = CreateStorage(mode);

        Assert.Equal(DeleteBucketResult.NotFound, await storage.DeleteBucketAsync(BucketName));
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
            Directory.Delete(_root, recursive: true);
    }
}
