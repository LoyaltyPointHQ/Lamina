using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace Lamina.Storage.Filesystem.Tests;

public class FilesystemObjectDataStorageCopyTests : IDisposable
{
    private readonly string _testDataDirectory;
    private readonly string _testMetadataDirectory;

    public FilesystemObjectDataStorageCopyTests()
    {
        _testDataDirectory = Path.Combine(Path.GetTempPath(), $"lamina_copy_data_{Guid.NewGuid():N}");
        _testMetadataDirectory = Path.Combine(Path.GetTempPath(), $"lamina_copy_meta_{Guid.NewGuid():N}");
    }

    public void Dispose()
    {
        TryDelete(_testDataDirectory);
        TryDelete(_testMetadataDirectory);
    }

    private static void TryDelete(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); } catch { /* best-effort */ }
    }

    private FilesystemObjectDataStorage CreateStorage()
    {
        var settings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = _testDataDirectory,
            MetadataDirectory = _testMetadataDirectory,
            MetadataMode = MetadataStorageMode.SeparateDirectory,
            InlineMetadataDirectoryName = ".lamina-meta",
            NetworkMode = NetworkFileSystemMode.None,
            UseZeroCopyCompleteMultipart = true
        });
        var networkHelper = new NetworkFileSystemHelper(settings, NullLogger<NetworkFileSystemHelper>.Instance);
        var zeroCopy = new LinuxZeroCopyHelper(NullLogger<LinuxZeroCopyHelper>.Instance);
        return new FilesystemObjectDataStorage(
            settings,
            networkHelper,
            zeroCopy,
            NullLogger<FilesystemObjectDataStorage>.Instance,
            new Mock<IChunkedDataParser>().Object);
    }

    private async Task<string> CreateObjectAsync(string bucketName, string key, byte[] content)
    {
        var bucketDir = Path.Combine(_testDataDirectory, bucketName);
        Directory.CreateDirectory(bucketDir);
        var path = Path.Combine(bucketDir, key);
        await File.WriteAllBytesAsync(path, content);
        return path;
    }

    [Fact]
    public async Task PrepareCopyDataAsync_CopiesSourceContentToDestination()
    {
        var storage = CreateStorage();
        var sourceContent = System.Text.Encoding.UTF8.GetBytes("Hello, zero-copy world!");
        await CreateObjectAsync("src-bucket", "source.txt", sourceContent);

        var prepared = await storage.PrepareCopyDataAsync("src-bucket", "source.txt", "dst-bucket", "dest.txt");

        Assert.NotNull(prepared);
        using (prepared!)
        {
            Assert.Equal(sourceContent.Length, prepared.Size);
            await storage.CommitPreparedDataAsync(prepared);
        }

        var destPath = Path.Combine(_testDataDirectory, "dst-bucket", "dest.txt");
        Assert.Equal(sourceContent, await File.ReadAllBytesAsync(destPath));
    }

    [Fact]
    public async Task PrepareCopyDataAsync_ReturnsCorrectSizeAndETag()
    {
        var storage = CreateStorage();
        var sourceContent = System.Text.Encoding.UTF8.GetBytes("Some content for ETag verification");
        await CreateObjectAsync("bucket", "obj.txt", sourceContent);

        var prepared = await storage.PrepareCopyDataAsync("bucket", "obj.txt", "bucket", "obj-copy.txt");

        Assert.NotNull(prepared);
        using (prepared!)
        {
            Assert.Equal(sourceContent.Length, prepared.Size);
            Assert.NotEmpty(prepared.ETag);
        }
    }

    [Fact]
    public async Task PrepareCopyDataAsync_ReturnsNull_WhenSourceDoesNotExist()
    {
        var storage = CreateStorage();

        var prepared = await storage.PrepareCopyDataAsync("bucket", "nonexistent.txt", "bucket", "dest.txt");

        Assert.Null(prepared);
    }

    [Fact]
    public async Task PrepareCopyDataAsync_DeletesTempFile_OnDispose()
    {
        var storage = CreateStorage();
        var sourceContent = System.Text.Encoding.UTF8.GetBytes("Cleanup test content");
        await CreateObjectAsync("bucket", "src.txt", sourceContent);

        string? tempPath = null;
        var prepared = await storage.PrepareCopyDataAsync("bucket", "src.txt", "bucket", "dst.txt");
        Assert.NotNull(prepared);

        // Tag holds the temp file path
        tempPath = (string?)prepared!.Tag;
        Assert.NotNull(tempPath);
        Assert.True(File.Exists(tempPath), "Temp file should exist before dispose");

        prepared.Dispose();

        Assert.False(File.Exists(tempPath), "Temp file should be deleted after dispose");
    }

    [Fact]
    public async Task PrepareCopyDataAsync_PreservesContentForLargeFile()
    {
        var storage = CreateStorage();
        // 200 KB — większy niż bufor ArrayPool (81920), weryfikuje pętlę kopiowania
        var sourceContent = new byte[200 * 1024];
        new Random(42).NextBytes(sourceContent);
        await CreateObjectAsync("bucket", "large.bin", sourceContent);

        var prepared = await storage.PrepareCopyDataAsync("bucket", "large.bin", "bucket", "large-copy.bin");

        Assert.NotNull(prepared);
        using (prepared!)
        {
            Assert.Equal(sourceContent.Length, prepared.Size);
            await storage.CommitPreparedDataAsync(prepared);
        }

        var destPath = Path.Combine(_testDataDirectory, "bucket", "large-copy.bin");
        Assert.Equal(sourceContent, await File.ReadAllBytesAsync(destPath));
    }
}
