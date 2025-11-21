using System;
using System.Collections.Generic;
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
/// Tests for NetworkFileSystemHelper complex operations like AtomicMove, DeleteDirectoryIfEmpty, and EnsureDirectoryExists.
/// </summary>
public class NetworkFileSystemHelperComplexOperationsTests : IDisposable
{
    private readonly string _testDir;

    public NetworkFileSystemHelperComplexOperationsTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"lamina-test-{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, true);
        }
    }

    #region EnsureDirectoryExistsAsync Tests

    [Fact]
    public async Task EnsureDirectoryExists_AlreadyExists_Succeeds()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.None);
        var dirPath = Path.Combine(_testDir, "existing");
        Directory.CreateDirectory(dirPath);

        // Act & Assert - should not throw
        await helper.EnsureDirectoryExistsAsync(dirPath);
        Assert.True(Directory.Exists(dirPath));
    }

    [Fact]
    public async Task EnsureDirectoryExists_DoesNotExist_CreatesDirectory()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.None);
        var dirPath = Path.Combine(_testDir, "new-dir");

        // Act
        await helper.EnsureDirectoryExistsAsync(dirPath);

        // Assert
        Assert.True(Directory.Exists(dirPath));
    }

    [Fact]
    public async Task EnsureDirectoryExists_FileExists_ThrowsInvalidOperation()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.None);
        var filePath = Path.Combine(_testDir, "file.txt");
        await File.WriteAllTextAsync(filePath, "content");

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            helper.EnsureDirectoryExistsAsync(filePath));

        Assert.Contains("file already exists", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task EnsureDirectoryExists_NestedPath_CreatesAllLevels()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.None);
        var nestedPath = Path.Combine(_testDir, "level1", "level2", "level3");

        // Act
        await helper.EnsureDirectoryExistsAsync(nestedPath);

        // Assert
        Assert.True(Directory.Exists(nestedPath));
        Assert.True(Directory.Exists(Path.Combine(_testDir, "level1")));
        Assert.True(Directory.Exists(Path.Combine(_testDir, "level1", "level2")));
    }

    #endregion

    #region AtomicMoveAsync Tests

    [Fact]
    public async Task AtomicMove_CIFS_SourceDoesNotExist_ThrowsFileNotFound()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.CIFS);
        var sourcePath = Path.Combine(_testDir, "nonexistent.txt");
        var destPath = Path.Combine(_testDir, "dest.txt");

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(() =>
            helper.AtomicMoveAsync(sourcePath, destPath, overwrite: false));
    }

    [Fact]
    public async Task AtomicMove_CIFS_NoOverwrite_SuccessfulMove()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.CIFS);
        var sourcePath = Path.Combine(_testDir, "source.txt");
        var destPath = Path.Combine(_testDir, "dest.txt");
        await File.WriteAllTextAsync(sourcePath, "source content");

        // Act
        await helper.AtomicMoveAsync(sourcePath, destPath, overwrite: false);

        // Assert
        Assert.False(File.Exists(sourcePath));
        Assert.True(File.Exists(destPath));
        Assert.Equal("source content", await File.ReadAllTextAsync(destPath));
    }

    [Fact]
    public async Task AtomicMove_CIFS_Overwrite_UsesBackupStrategy()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.CIFS);
        var sourcePath = Path.Combine(_testDir, "source.txt");
        var destPath = Path.Combine(_testDir, "dest.txt");
        await File.WriteAllTextAsync(sourcePath, "new content");
        await File.WriteAllTextAsync(destPath, "old content");

        // Act
        await helper.AtomicMoveAsync(sourcePath, destPath, overwrite: true);

        // Assert
        Assert.False(File.Exists(sourcePath));
        Assert.True(File.Exists(destPath));
        Assert.Equal("new content", await File.ReadAllTextAsync(destPath));
    }

    [Fact]
    public async Task AtomicMove_NFS_NoOverwrite_SuccessfulMove()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.NFS);
        var sourcePath = Path.Combine(_testDir, "source.txt");
        var destPath = Path.Combine(_testDir, "dest.txt");
        await File.WriteAllTextAsync(sourcePath, "source content");

        // Act
        await helper.AtomicMoveAsync(sourcePath, destPath, overwrite: false);

        // Assert
        Assert.False(File.Exists(sourcePath));
        Assert.True(File.Exists(destPath));
        Assert.Equal("source content", await File.ReadAllTextAsync(destPath));
    }

    [Fact]
    public async Task AtomicMove_NFS_Overwrite_UsesStandardMove()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.NFS);
        var sourcePath = Path.Combine(_testDir, "source.txt");
        var destPath = Path.Combine(_testDir, "dest.txt");
        await File.WriteAllTextAsync(sourcePath, "new content");
        await File.WriteAllTextAsync(destPath, "old content");

        // Act
        await helper.AtomicMoveAsync(sourcePath, destPath, overwrite: true);

        // Assert
        Assert.False(File.Exists(sourcePath));
        Assert.True(File.Exists(destPath));
        Assert.Equal("new content", await File.ReadAllTextAsync(destPath));
    }

    [Fact]
    public async Task AtomicMove_None_NoOverwrite_SuccessfulMove()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.None);
        var sourcePath = Path.Combine(_testDir, "source.txt");
        var destPath = Path.Combine(_testDir, "dest.txt");
        await File.WriteAllTextAsync(sourcePath, "source content");

        // Act
        await helper.AtomicMoveAsync(sourcePath, destPath, overwrite: false);

        // Assert
        Assert.False(File.Exists(sourcePath));
        Assert.True(File.Exists(destPath));
        Assert.Equal("source content", await File.ReadAllTextAsync(destPath));
    }

    [Fact]
    public async Task AtomicMove_CIFS_DestinationInDifferentDirectory_SuccessfulMove()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.CIFS);
        var sourcePath = Path.Combine(_testDir, "source.txt");
        var destDir = Path.Combine(_testDir, "subdir");
        var destPath = Path.Combine(destDir, "dest.txt");
        await File.WriteAllTextAsync(sourcePath, "content");
        Directory.CreateDirectory(destDir); // Caller is responsible for creating directory

        // Act
        await helper.AtomicMoveAsync(sourcePath, destPath, overwrite: false);

        // Assert
        Assert.True(Directory.Exists(destDir));
        Assert.True(File.Exists(destPath));
        Assert.False(File.Exists(sourcePath));
        Assert.Equal("content", await File.ReadAllTextAsync(destPath));
    }

    #endregion

    #region DeleteDirectoryIfEmptyAsync Tests

    [Fact]
    public async Task DeleteDirectoryIfEmpty_EmptyDirectory_DeletesIt()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.None);
        var dirPath = Path.Combine(_testDir, "empty-dir");
        Directory.CreateDirectory(dirPath);

        // Act
        await helper.DeleteDirectoryIfEmptyAsync(dirPath, _testDir);

        // Assert
        Assert.False(Directory.Exists(dirPath));
    }

    [Fact]
    public async Task DeleteDirectoryIfEmpty_NonEmptyDirectory_DoesNotDelete()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.None);
        var dirPath = Path.Combine(_testDir, "nonempty-dir");
        Directory.CreateDirectory(dirPath);
        await File.WriteAllTextAsync(Path.Combine(dirPath, "file.txt"), "content");

        // Act
        await helper.DeleteDirectoryIfEmptyAsync(dirPath, _testDir);

        // Assert
        Assert.True(Directory.Exists(dirPath)); // Should still exist
    }

    [Fact]
    public async Task DeleteDirectoryIfEmpty_RecursivelyDeletesEmptyParents()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.None);
        var level1 = Path.Combine(_testDir, "level1");
        var level2 = Path.Combine(level1, "level2");
        var level3 = Path.Combine(level2, "level3");
        Directory.CreateDirectory(level3);

        // Act - delete level3, should cascade up to level1
        await helper.DeleteDirectoryIfEmptyAsync(level3, _testDir);

        // Assert
        Assert.False(Directory.Exists(level3));
        Assert.False(Directory.Exists(level2));
        Assert.False(Directory.Exists(level1));
        Assert.True(Directory.Exists(_testDir)); // Stop directory should still exist
    }

    [Fact]
    public async Task DeleteDirectoryIfEmpty_StopsAtStopDirectory()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.None);
        var level1 = Path.Combine(_testDir, "level1");
        var level2 = Path.Combine(level1, "level2");
        Directory.CreateDirectory(level2);

        // Act - delete level2, stop at level1
        await helper.DeleteDirectoryIfEmptyAsync(level2, level1);

        // Assert
        Assert.False(Directory.Exists(level2));
        Assert.True(Directory.Exists(level1)); // Should stop here
    }

    [Fact]
    public async Task DeleteDirectoryIfEmpty_StopsWhenParentNotEmpty()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.None);
        var level1 = Path.Combine(_testDir, "level1");
        var level2 = Path.Combine(level1, "level2");
        var level3 = Path.Combine(level2, "level3");
        Directory.CreateDirectory(level3);

        // Add a file to level1 so it's not empty
        await File.WriteAllTextAsync(Path.Combine(level1, "file.txt"), "content");

        // Act - delete level3
        await helper.DeleteDirectoryIfEmptyAsync(level3, _testDir);

        // Assert
        Assert.False(Directory.Exists(level3));
        Assert.False(Directory.Exists(level2)); // Parent of level3 was empty, deleted
        Assert.True(Directory.Exists(level1));  // Has file, should not be deleted
    }

    [Fact]
    public async Task DeleteDirectoryIfEmpty_DirectoryDoesNotExist_DoesNotThrow()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.None);
        var dirPath = Path.Combine(_testDir, "nonexistent");

        // Act & Assert - should not throw
        await helper.DeleteDirectoryIfEmptyAsync(dirPath, _testDir);
    }

    [Fact]
    public async Task DeleteDirectoryIfEmpty_StopDirectoryIsCurrentDirectory_DoesNotDelete()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.None);
        var dirPath = Path.Combine(_testDir, "somedir");
        Directory.CreateDirectory(dirPath);

        // Act - trying to delete with stop directory = the directory itself
        await helper.DeleteDirectoryIfEmptyAsync(dirPath, dirPath);

        // Assert - should still exist (we hit the stop condition)
        Assert.True(Directory.Exists(dirPath));
    }

    #endregion

    #region DeleteDirectoryIfEmptyAsync Race Condition Tests

    [Fact]
    public async Task DeleteDirectoryIfEmpty_ConcurrentDeletionOfSameHierarchy_HandlesRaceCondition()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.CIFS);
        var level1 = Path.Combine(_testDir, "level1");
        var level2 = Path.Combine(level1, "level2");
        var level3 = Path.Combine(level2, "level3");
        Directory.CreateDirectory(level3);

        // Act - Multiple threads simultaneously delete the same directory hierarchy
        var tasks = Enumerable.Range(0, 10)
            .Select(_ => helper.DeleteDirectoryIfEmptyAsync(level3, _testDir))
            .ToArray();

        // Should not throw - all tasks should handle DirectoryNotFoundException gracefully
        await Task.WhenAll(tasks);

        // Assert
        Assert.False(Directory.Exists(level3));
        Assert.False(Directory.Exists(level2));
        Assert.False(Directory.Exists(level1));
    }

    [Fact]
    public async Task DeleteDirectoryIfEmpty_ParentDeletedByAnotherThread_DoesNotThrow()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.NFS);
        var level1 = Path.Combine(_testDir, "level1");
        var level2 = Path.Combine(level1, "level2");
        var level3a = Path.Combine(level2, "level3a");
        var level3b = Path.Combine(level2, "level3b");
        Directory.CreateDirectory(level3a);
        Directory.CreateDirectory(level3b);

        // Act - Two threads delete sibling directories, racing to delete shared parents
        var task1 = helper.DeleteDirectoryIfEmptyAsync(level3a, _testDir);
        var task2 = helper.DeleteDirectoryIfEmptyAsync(level3b, _testDir);

        // Should not throw even though both try to delete level2 and level1
        await Task.WhenAll(task1, task2);

        // Assert
        Assert.False(Directory.Exists(level3a));
        Assert.False(Directory.Exists(level3b));
        Assert.False(Directory.Exists(level2));
        Assert.False(Directory.Exists(level1));
    }

    [Fact]
    public async Task DeleteDirectoryIfEmpty_HighConcurrencyStressTest_NoExceptions()
    {
        // Arrange
        var helper = CreateHelper(NetworkFileSystemMode.CIFS);
        const int threadCount = 20;
        const int hierarchyCount = 5;

        // Create multiple overlapping directory hierarchies
        var hierarchies = new List<string>();
        for (int i = 0; i < hierarchyCount; i++)
        {
            var level1 = Path.Combine(_testDir, $"shared");
            var level2 = Path.Combine(level1, $"sub{i}");
            var level3 = Path.Combine(level2, $"deep{i}");
            Directory.CreateDirectory(level3);
            hierarchies.Add(level3);
        }

        // Act - Many threads concurrently delete overlapping hierarchies
        var tasks = new List<Task>();
        for (int i = 0; i < threadCount; i++)
        {
            var hierarchy = hierarchies[i % hierarchyCount];
            tasks.Add(helper.DeleteDirectoryIfEmptyAsync(hierarchy, _testDir));
        }

        // Should handle all race conditions without throwing
        await Task.WhenAll(tasks);

        // Assert - All hierarchies should be cleaned up
        foreach (var hierarchy in hierarchies)
        {
            Assert.False(Directory.Exists(hierarchy));
        }
    }

    [Fact]
    public async Task DeleteDirectoryIfEmpty_MultipleObjectsInSameDirectory_SimulatesRealWorldScenario()
    {
        // Arrange - Simulates the MongoDB backup scenario from the logs
        var helper = CreateHelper(NetworkFileSystemMode.CIFS);
        var bucketDir = Path.Combine(_testDir, "mongo");
        var dateDir = Path.Combine(bucketDir, "2025-11-14T02:00:00Z");
        var rs0Dir = Path.Combine(dateDir, "rs0");
        var metaDir = Path.Combine(rs0Dir, ".lamina-meta");

        Directory.CreateDirectory(metaDir);

        // Create multiple metadata files (simulating multiple objects)
        var metadataFiles = new[]
        {
            Path.Combine(metaDir, "meta.pbm.json"),
            Path.Combine(metaDir, "admin.system.version.gz.json"),
            Path.Combine(metaDir, "admin.pbmRUsers.gz.json"),
            Path.Combine(metaDir, "admin.pbmRRoles.gz.json")
        };

        foreach (var file in metadataFiles)
        {
            await File.WriteAllTextAsync(file, "{}");
        }

        // Act - Simulate concurrent deletion of multiple objects (each triggers directory cleanup)
        var deleteTasks = metadataFiles.Select(async file =>
        {
            // Delete file
            File.Delete(file);
            // Then try to clean up directories
            await helper.DeleteDirectoryIfEmptyAsync(metaDir, bucketDir);
        }).ToArray();

        // Should not throw DirectoryNotFoundException
        await Task.WhenAll(deleteTasks);

        // Assert - All empty directories should be cleaned up
        Assert.False(Directory.Exists(metaDir));
        Assert.False(Directory.Exists(rs0Dir));
        Assert.False(Directory.Exists(dateDir));
        Assert.True(Directory.Exists(bucketDir)); // Stop directory should remain
    }

    #endregion

    #region Helper Methods

    private NetworkFileSystemHelper CreateHelper(NetworkFileSystemMode mode)
    {
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = mode,
            RetryCount = 3,
            RetryDelayMs = 10
        };

        return new NetworkFileSystemHelper(
            Options.Create(settings),
            new NullLogger<NetworkFileSystemHelper>()
        );
    }

    #endregion
}
