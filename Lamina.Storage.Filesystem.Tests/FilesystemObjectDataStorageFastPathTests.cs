using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace Lamina.Storage.Filesystem.Tests;

/// <summary>
/// Tests for the IFileBackedObjectDataStorage fast path that assembles multipart objects directly
/// from part file paths (Linux copy_file_range, or userspace fallback) instead of going through
/// PipeReader streaming.
/// </summary>
public class FilesystemObjectDataStorageFastPathTests : IDisposable
{
    private readonly string _testDataDirectory;
    private readonly string _testMetadataDirectory;

    public FilesystemObjectDataStorageFastPathTests()
    {
        _testDataDirectory = Path.Combine(Path.GetTempPath(), $"lamina_fastpath_data_{Guid.NewGuid():N}");
        _testMetadataDirectory = Path.Combine(Path.GetTempPath(), $"lamina_fastpath_meta_{Guid.NewGuid():N}");
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

    private FilesystemObjectDataStorage CreateStorage(bool zeroCopyEnabled)
    {
        var settings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = _testDataDirectory,
            MetadataDirectory = _testMetadataDirectory,
            MetadataMode = MetadataStorageMode.SeparateDirectory,
            InlineMetadataDirectoryName = ".lamina-meta",
            NetworkMode = NetworkFileSystemMode.None,
            UseZeroCopyCompleteMultipart = zeroCopyEnabled
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

    [Fact]
    public async Task PrepareMultipartDataFromFilesAsync_AssemblesPartsIntoTempfileInOrder()
    {
        const string bucketName = "bucket";
        const string key = "assembled";
        var storage = CreateStorage(zeroCopyEnabled: true);

        var partsDir = Path.Combine(_testDataDirectory, "_parts");
        Directory.CreateDirectory(partsDir);
        var part1 = Path.Combine(partsDir, "p1");
        var part2 = Path.Combine(partsDir, "p2");
        var part3 = Path.Combine(partsDir, "p3");
        var part1Bytes = System.Text.Encoding.UTF8.GetBytes("Lorem ipsum ");
        var part2Bytes = System.Text.Encoding.UTF8.GetBytes("dolor sit ");
        var part3Bytes = System.Text.Encoding.UTF8.GetBytes("amet.");
        await File.WriteAllBytesAsync(part1, part1Bytes);
        await File.WriteAllBytesAsync(part2, part2Bytes);
        await File.WriteAllBytesAsync(part3, part3Bytes);

        var prepared = await storage.PrepareMultipartDataFromFilesAsync(bucketName, key, new[] { part1, part2, part3 });
        Assert.NotNull(prepared);
        using (prepared!)
        {
            Assert.Equal(part1Bytes.Length + part2Bytes.Length + part3Bytes.Length, prepared.Size);

            // Commit to make the object visible, then verify the on-disk bytes.
            await storage.CommitPreparedDataAsync(prepared);
        }

        var expected = part1Bytes.Concat(part2Bytes).Concat(part3Bytes).ToArray();
        var actualPath = Path.Combine(_testDataDirectory, bucketName, key);
        Assert.Equal(expected, await File.ReadAllBytesAsync(actualPath));
    }

    [Fact]
    public async Task PrepareMultipartDataFromFilesAsync_ReturnsNull_WhenFeatureFlagDisabled()
    {
        // With the flag off, the fast-path method must return null so the facade falls back to
        // the PipeReader-based PrepareMultipartDataAsync path.
        var storage = CreateStorage(zeroCopyEnabled: false);

        var partsDir = Path.Combine(_testDataDirectory, "_parts");
        Directory.CreateDirectory(partsDir);
        var part1 = Path.Combine(partsDir, "p1");
        await File.WriteAllBytesAsync(part1, System.Text.Encoding.UTF8.GetBytes("x"));

        var prepared = await storage.PrepareMultipartDataFromFilesAsync("bucket", "k", new[] { part1 });
        Assert.Null(prepared);
    }

    [Fact]
    public async Task PrepareMultipartDataFromFilesAsync_HandlesEmptyPartsList()
    {
        var storage = CreateStorage(zeroCopyEnabled: true);
        var prepared = await storage.PrepareMultipartDataFromFilesAsync("bucket", "empty", Array.Empty<string>());
        Assert.NotNull(prepared);
        using (prepared!)
        {
            Assert.Equal(0L, prepared.Size);
        }
    }
}
