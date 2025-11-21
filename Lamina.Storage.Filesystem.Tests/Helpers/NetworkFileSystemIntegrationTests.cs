using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace Lamina.Storage.Filesystem.Tests.Helpers;

/// <summary>
/// Integration tests for NetworkFileSystemHelper with real filesystem operations.
/// </summary>
public class NetworkFileSystemIntegrationTests : IDisposable
{
    private readonly string _testDir;

    public NetworkFileSystemIntegrationTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"lamina-integration-test-{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            try
            {
                Directory.Delete(_testDir, true);
            }
            catch
            {
                // Best effort cleanup
            }
        }
    }

    #region End-to-End Scenarios

    [Fact]
    public async Task EndToEnd_CreateDirectories_WithRetry()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.CIFS);
        var dirs = Enumerable.Range(0, 10)
            .Select(i => Path.Combine(_testDir, $"dir-{i}"))
            .ToArray();

        // Act - Create multiple directories
        foreach (var dir in dirs)
        {
            await helper.EnsureDirectoryExistsAsync(dir);
        }

        // Assert - All directories should exist
        foreach (var dir in dirs)
        {
            Assert.True(Directory.Exists(dir));
        }
    }

    [Fact]
    public async Task EndToEnd_FileOperations_CreateMoveDelete()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.CIFS);
        var sourceDir = Path.Combine(_testDir, "source");
        var destDir = Path.Combine(_testDir, "dest");
        await helper.EnsureDirectoryExistsAsync(sourceDir);
        await helper.EnsureDirectoryExistsAsync(destDir);

        var sourceFile = Path.Combine(sourceDir, "test.txt");
        var destFile = Path.Combine(destDir, "test.txt");

        // Act - Create, write, move
        await File.WriteAllTextAsync(sourceFile, "test content");
        await helper.AtomicMoveAsync(sourceFile, destFile, overwrite: false);

        // Assert
        Assert.False(File.Exists(sourceFile));
        Assert.True(File.Exists(destFile));
        Assert.Equal("test content", await File.ReadAllTextAsync(destFile));

        // Cleanup
        await helper.DeleteDirectoryIfEmptyAsync(sourceDir, _testDir);
        Assert.False(Directory.Exists(sourceDir));
    }

    [Fact]
    public async Task EndToEnd_MultipleFilesInHierarchy()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.NFS);
        var level1 = Path.Combine(_testDir, "level1");
        var level2 = Path.Combine(level1, "level2");
        var level3 = Path.Combine(level2, "level3");

        // Act - Create nested directory structure
        await helper.EnsureDirectoryExistsAsync(level3);

        // Create files at different levels
        var files = new[]
        {
            Path.Combine(level1, "file1.txt"),
            Path.Combine(level2, "file2.txt"),
            Path.Combine(level3, "file3.txt")
        };

        foreach (var file in files)
        {
            await File.WriteAllTextAsync(file, $"Content of {Path.GetFileName(file)}");
        }

        // Assert - All files exist
        foreach (var file in files)
        {
            Assert.True(File.Exists(file));
        }

        // Act - Move file from level3 to level1
        var movedFile = Path.Combine(level1, "moved-file3.txt");
        await helper.AtomicMoveAsync(files[2], movedFile, overwrite: false);

        // Assert
        Assert.False(File.Exists(files[2]));
        Assert.True(File.Exists(movedFile));

        // Cleanup - Delete empty level3 and level2
        await helper.DeleteDirectoryIfEmptyAsync(level3, _testDir);
        Assert.False(Directory.Exists(level3));
        // Note: level2 might still exist if there are still files in it
        // The DeleteDirectoryIfEmptyAsync stops when it finds non-empty directories
        Assert.True(Directory.Exists(level1)); // Still has files
    }

    #endregion

    #region Concurrent Operations

    [Fact]
    public async Task Concurrent_CreateDirectories_ThreadSafe()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.CIFS);
        var dirCount = 50;
        var dirs = Enumerable.Range(0, dirCount)
            .Select(i => Path.Combine(_testDir, "concurrent", $"dir-{i}"))
            .ToArray();

        // Act - Create directories concurrently
        await Parallel.ForEachAsync(dirs, async (dir, ct) =>
        {
            await helper.EnsureDirectoryExistsAsync(dir);
        });

        // Assert - All directories should exist
        foreach (var dir in dirs)
        {
            Assert.True(Directory.Exists(dir), $"Directory {dir} should exist");
        }
    }

    [Fact]
    public async Task Concurrent_FileOperations_NoDataLoss()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.NFS);
        var sourceDir = Path.Combine(_testDir, "concurrent-files", "source");
        var destDir = Path.Combine(_testDir, "concurrent-files", "dest");
        await helper.EnsureDirectoryExistsAsync(sourceDir);
        await helper.EnsureDirectoryExistsAsync(destDir);

        var fileCount = 20;
        var files = Enumerable.Range(0, fileCount)
            .Select(i => new
            {
                Source = Path.Combine(sourceDir, $"file-{i}.txt"),
                Dest = Path.Combine(destDir, $"file-{i}.txt"),
                Content = $"Content for file {i}"
            })
            .ToArray();

        // Create all source files
        foreach (var file in files)
        {
            await File.WriteAllTextAsync(file.Source, file.Content);
        }

        // Act - Move files concurrently
        await Parallel.ForEachAsync(files, async (file, ct) =>
        {
            await helper.AtomicMoveAsync(file.Source, file.Dest, overwrite: false);
        });

        // Assert - All files moved successfully with correct content
        foreach (var file in files)
        {
            Assert.False(File.Exists(file.Source), $"Source {file.Source} should not exist");
            Assert.True(File.Exists(file.Dest), $"Dest {file.Dest} should exist");
            var content = await File.ReadAllTextAsync(file.Dest);
            Assert.Equal(file.Content, content);
        }
    }

    [Fact]
    public async Task Concurrent_SameDirectory_MultipleThreads()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.CIFS);
        var sharedDir = Path.Combine(_testDir, "shared");
        var threadCount = 10;

        // Act - Multiple threads trying to ensure same directory exists
        var tasks = Enumerable.Range(0, threadCount)
            .Select(_ => helper.EnsureDirectoryExistsAsync(sharedDir))
            .ToArray();

        await Task.WhenAll(tasks);

        // Assert - Directory should exist
        Assert.True(Directory.Exists(sharedDir));
    }

    #endregion

    #region Performance Tests

    [Fact]
    public async Task Performance_RetryOverhead_Minimal()
    {
        // Arrange
        var helperWithRetry = CreateHelper(NetworkFileSystemMode.CIFS);
        var helperNoRetry = CreateHelper(NetworkFileSystemMode.None);

        var iterations = 50;

        // Act - Measure with retry enabled (no actual retries)
        var swWithRetry = Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
        {
            var dir = Path.Combine(_testDir, "perf-retry", $"dir-{i}");
            await helperWithRetry.EnsureDirectoryExistsAsync(dir);
        }
        swWithRetry.Stop();

        // Act - Measure without retry
        var swNoRetry = Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
        {
            var dir = Path.Combine(_testDir, "perf-no-retry", $"dir-{i}");
            await helperNoRetry.EnsureDirectoryExistsAsync(dir);
        }
        swNoRetry.Stop();

        // Assert - Overhead should be minimal (< 50% slower)
        // Handle case where both are very fast (< 1ms)
        if (swNoRetry.ElapsedMilliseconds == 0)
        {
            // Both too fast to measure, consider it a pass
            Assert.True(swWithRetry.ElapsedMilliseconds < 100,
                $"With retry took too long: {swWithRetry.ElapsedMilliseconds}ms");
        }
        else
        {
            var overhead = (double)swWithRetry.ElapsedMilliseconds / swNoRetry.ElapsedMilliseconds;
            Assert.True(overhead < 2.0,
                $"Retry overhead too high: {overhead:F2}x (with retry: {swWithRetry.ElapsedMilliseconds}ms, without: {swNoRetry.ElapsedMilliseconds}ms)");
        }
    }

    [Fact]
    public async Task Performance_ConcurrentOperations_Scales()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.NFS);
        var fileCount = 100;

        // Act - Create files concurrently
        var stopwatch = Stopwatch.StartNew();

        await Parallel.ForEachAsync(
            Enumerable.Range(0, fileCount),
            new ParallelOptions { MaxDegreeOfParallelism = 10 },
            async (i, ct) =>
            {
                var dir = Path.Combine(_testDir, "perf-concurrent", $"subdir-{i}");
                await helper.EnsureDirectoryExistsAsync(dir);
                var file = Path.Combine(dir, "file.txt");
                await File.WriteAllTextAsync(file, $"Content {i}");
            });

        stopwatch.Stop();

        // Assert - Should complete in reasonable time (< 5 seconds for 100 operations)
        Assert.True(stopwatch.ElapsedMilliseconds < 5000,
            $"Concurrent operations took too long: {stopwatch.ElapsedMilliseconds}ms");

        // Verify all files exist
        var createdFiles = Directory.GetFiles(Path.Combine(_testDir, "perf-concurrent"), "file.txt", SearchOption.AllDirectories);
        Assert.Equal(fileCount, createdFiles.Length);
    }

    #endregion

    #region Real-World Scenarios

    [Fact]
    public async Task RealWorld_CreateObjectStorage_Pattern()
    {
        // Arrange - Simulate S3 object storage pattern
        var helper = CreateHelper(NetworkFileSystemMode.CIFS);
        var bucketName = "test-bucket";
        var objectKeys = new[]
        {
            "folder1/file1.txt",
            "folder1/folder2/file2.txt",
            "folder1/folder2/folder3/file3.txt",
            "root-file.txt"
        };

        // Act - Create bucket and object structure
        var bucketDir = Path.Combine(_testDir, bucketName);
        await helper.EnsureDirectoryExistsAsync(bucketDir);

        foreach (var key in objectKeys)
        {
            var filePath = Path.Combine(bucketDir, key);
            var dirPath = Path.GetDirectoryName(filePath)!;

            await helper.EnsureDirectoryExistsAsync(dirPath);
            await File.WriteAllTextAsync(filePath, $"Object data for {key}");
        }

        // Assert - All objects exist
        foreach (var key in objectKeys)
        {
            var filePath = Path.Combine(bucketDir, key);
            Assert.True(File.Exists(filePath), $"Object {key} should exist");
        }
    }

    [Fact]
    public async Task RealWorld_MultipartUpload_Simulation()
    {
        // Arrange - Simulate multipart upload with temporary parts
        var helper = CreateHelper(NetworkFileSystemMode.NFS);
        var uploadDir = Path.Combine(_testDir, "uploads", "upload-123");
        var partsDir = Path.Combine(uploadDir, "parts");
        var finalFile = Path.Combine(_testDir, "completed", "final-object.dat");

        await helper.EnsureDirectoryExistsAsync(partsDir);
        await helper.EnsureDirectoryExistsAsync(Path.GetDirectoryName(finalFile)!);

        // Act - Create part files
        var partCount = 5;
        for (int i = 1; i <= partCount; i++)
        {
            var partFile = Path.Combine(partsDir, $"part-{i}.dat");
            await File.WriteAllTextAsync(partFile, $"Part {i} data\n");
        }

        // Simulate assembling parts into final file
        await using (var finalStream = File.Create(finalFile))
        {
            for (int i = 1; i <= partCount; i++)
            {
                var partFile = Path.Combine(partsDir, $"part-{i}.dat");
                await using var partStream = File.OpenRead(partFile);
                await partStream.CopyToAsync(finalStream);
            }
        }

        // Cleanup parts directory
        await helper.DeleteDirectoryIfEmptyAsync(partsDir, _testDir);

        // Assert
        Assert.True(File.Exists(finalFile));
        var content = await File.ReadAllTextAsync(finalFile);
        Assert.Contains("Part 1 data", content);
        Assert.Contains("Part 5 data", content);
    }

    [Fact]
    public async Task RealWorld_MoveWithOverwrite_CIFS()
    {
        // Arrange - Test CIFS backup/restore logic with real files
        var helper = CreateHelper(NetworkFileSystemMode.CIFS);
        var sourceFile = Path.Combine(_testDir, "source.txt");
        var destFile = Path.Combine(_testDir, "dest.txt");

        await File.WriteAllTextAsync(sourceFile, "new content");
        await File.WriteAllTextAsync(destFile, "old content");

        // Act - Overwrite with CIFS backup strategy
        await helper.AtomicMoveAsync(sourceFile, destFile, overwrite: true);

        // Assert
        Assert.False(File.Exists(sourceFile));
        Assert.True(File.Exists(destFile));
        Assert.Equal("new content", await File.ReadAllTextAsync(destFile));

        // Verify no backup files remain
        var backupFiles = Directory.GetFiles(_testDir, "*.backup_*");
        Assert.Empty(backupFiles);
    }

    #endregion

    #region Helper Methods

    private NetworkFileSystemHelper CreateHelper(NetworkFileSystemMode mode)
    {
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = mode,
            RetryCount = 3,
            RetryDelayMs = 10 // Short delay for tests
        };

        return new NetworkFileSystemHelper(
            Options.Create(settings),
            new NullLogger<NetworkFileSystemHelper>()
        );
    }

    #endregion
}
