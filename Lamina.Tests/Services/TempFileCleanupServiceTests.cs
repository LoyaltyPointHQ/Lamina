using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.WebApi.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace Lamina.Tests.Services;

public class TempFileCleanupServiceTests : IDisposable
{
    private readonly Mock<ILogger<TempFileCleanupService>> _mockLogger;
    private readonly IConfiguration _configuration;
    private readonly string _testDataDirectory;
    private readonly string _tempFilePrefix = ".lamina-tmp-";
    private readonly FilesystemStorageSettings _filesystemSettings;

    public TempFileCleanupServiceTests()
    {
        _mockLogger = new Mock<ILogger<TempFileCleanupService>>();

        // Create temporary test directory
        _testDataDirectory = Path.Combine(Path.GetTempPath(), $"lamina-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDataDirectory);

        // Setup filesystem settings
        _filesystemSettings = new FilesystemStorageSettings
        {
            DataDirectory = _testDataDirectory,
            TempFilePrefix = _tempFilePrefix
        };

        // Setup configuration with fast intervals for testing
        var configValues = new Dictionary<string, string>
        {
            ["TempFileCleanup:CleanupIntervalMinutes"] = "1", // 1 minute for faster testing
            ["TempFileCleanup:TempFileAgeMinutes"] = "1", // 1 minute age threshold
            ["TempFileCleanup:BatchSize"] = "2" // Small batch size for testing
        };
        _configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configValues!)
            .Build();
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDataDirectory))
        {
            Directory.Delete(_testDataDirectory, true);
        }
    }

    [Fact]
    public async Task CleanupStaleTempFilesAsync_RemovesOldTempFiles()
    {
        // Arrange
        var oldTempFile = Path.Combine(_testDataDirectory, $"{_tempFilePrefix}old-file");
        var recentTempFile = Path.Combine(_testDataDirectory, $"{_tempFilePrefix}recent-file");
        var regularFile = Path.Combine(_testDataDirectory, "regular-file.txt");

        // Create test files
        await File.WriteAllTextAsync(oldTempFile, "old temp content");
        await File.WriteAllTextAsync(recentTempFile, "recent temp content");
        await File.WriteAllTextAsync(regularFile, "regular content");

        // Make the old temp file appear old by setting its last write time
        File.SetLastWriteTimeUtc(oldTempFile, DateTime.UtcNow.AddMinutes(-5));

        var options = Options.Create(_filesystemSettings);
        var service = new TempFileCleanupService(_mockLogger.Object, _configuration, options);

        // Act - Use reflection to call the private method
        var method = typeof(TempFileCleanupService).GetMethod("CleanupStaleTempFilesAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        method!.Invoke(service, new object[] { CancellationToken.None });

        // Assert
        Assert.False(File.Exists(oldTempFile), "Old temp file should be deleted");
        Assert.True(File.Exists(recentTempFile), "Recent temp file should be preserved");
        Assert.True(File.Exists(regularFile), "Regular file should be preserved");
    }

    [Fact]
    public async Task CleanupStaleTempFilesAsync_HandlesSubdirectories()
    {
        // Arrange
        var subDir = Path.Combine(_testDataDirectory, "bucket1", "path", "to");
        Directory.CreateDirectory(subDir);

        var oldTempFile = Path.Combine(subDir, $"{_tempFilePrefix}old-file");
        var recentTempFile = Path.Combine(subDir, $"{_tempFilePrefix}recent-file");

        await File.WriteAllTextAsync(oldTempFile, "old temp content");
        await File.WriteAllTextAsync(recentTempFile, "recent temp content");

        File.SetLastWriteTimeUtc(oldTempFile, DateTime.UtcNow.AddMinutes(-5));

        var options = Options.Create(_filesystemSettings);
        var service = new TempFileCleanupService(_mockLogger.Object, _configuration, options);

        // Act
        var method = typeof(TempFileCleanupService).GetMethod("CleanupStaleTempFilesAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        method!.Invoke(service, new object[] { CancellationToken.None });

        // Assert
        Assert.False(File.Exists(oldTempFile), "Old temp file in subdirectory should be deleted");
        Assert.True(File.Exists(recentTempFile), "Recent temp file in subdirectory should be preserved");
    }

    [Fact]
    public void CleanupStaleTempFilesAsync_HandlesNonExistentDirectory()
    {
        // Arrange
        var nonExistentDir = Path.Combine(Path.GetTempPath(), $"non-existent-{Guid.NewGuid():N}");
        var badSettings = new FilesystemStorageSettings
        {
            DataDirectory = nonExistentDir,
            TempFilePrefix = _tempFilePrefix
        };

        var options = Options.Create(badSettings);
        var service = new TempFileCleanupService(_mockLogger.Object, _configuration, options);

        // Act - Should not throw
        var method = typeof(TempFileCleanupService).GetMethod("CleanupStaleTempFilesAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        method!.Invoke(service, new object[] { CancellationToken.None });

        // Assert - Should log warning about non-existent directory
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Data directory does not exist")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task CleanupStaleTempFilesAsync_HandlesBatchProcessing()
    {
        // Arrange - Create more files than batch size
        var oldFiles = new List<string>();
        for (int i = 0; i < 5; i++)
        {
            var filePath = Path.Combine(_testDataDirectory, $"{_tempFilePrefix}old-file-{i}");
            await File.WriteAllTextAsync(filePath, $"old content {i}");
            File.SetLastWriteTimeUtc(filePath, DateTime.UtcNow.AddMinutes(-5));
            oldFiles.Add(filePath);
        }

        var options = Options.Create(_filesystemSettings);
        var service = new TempFileCleanupService(_mockLogger.Object, _configuration, options);

        // Act
        var method = typeof(TempFileCleanupService).GetMethod("CleanupStaleTempFilesAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        method!.Invoke(service, new object[] { CancellationToken.None });

        // Assert
        foreach (var filePath in oldFiles)
        {
            Assert.False(File.Exists(filePath), $"Old temp file should be deleted: {filePath}");
        }
    }

    [Fact]
    public async Task CleanupStaleTempFilesAsync_HandlesDirectoryAccessError()
    {
        // Arrange - Create a directory we don't have access to
        var restrictedDir = Path.Combine(_testDataDirectory, "restricted");
        Directory.CreateDirectory(restrictedDir);

        // Create a temp file in the restricted directory
        var tempFile = Path.Combine(restrictedDir, $"{_tempFilePrefix}file");
        await File.WriteAllTextAsync(tempFile, "content");
        File.SetLastWriteTimeUtc(tempFile, DateTime.UtcNow.AddMinutes(-5));

        // Remove read permissions from the directory to trigger access error
        var dirInfo = new DirectoryInfo(restrictedDir);
        var originalAttributes = dirInfo.Attributes;

        try
        {
            // On Linux, set directory to not readable/executable to simulate access error
            if (Environment.OSVersion.Platform == PlatformID.Unix)
            {
                // This might not work on all systems, so we'll make this test conditional
                try
                {
                    var process = Process.Start("chmod", $"000 \"{restrictedDir}\"");
                    process?.WaitForExit();
                    if (process?.ExitCode != 0)
                    {
                        // Skip this test if we can't change permissions
                        return;
                    }
                }
                catch
                {
                    // Skip this test if chmod is not available
                    return;
                }
            }

            var options = Options.Create(_filesystemSettings);
            var service = new TempFileCleanupService(_mockLogger.Object, _configuration, options);

            // Act
            var method = typeof(TempFileCleanupService).GetMethod("CleanupStaleTempFilesAsync",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            method!.Invoke(service, new object[] { CancellationToken.None });

            // Assert - Should log warning about directory access error but not crash
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Access denied to directory")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.AtLeastOnce);
        }
        finally
        {
            // Restore permissions for cleanup
            if (Environment.OSVersion.Platform == PlatformID.Unix)
            {
                try
                {
                    var process = Process.Start("chmod", $"755 \"{restrictedDir}\"");
                    process?.WaitForExit();
                }
                catch
                {
                    // Ignore errors during cleanup
                }
            }
            else
            {
                dirInfo.Attributes = originalAttributes;
            }
        }
    }

    [Fact]
    public void ExecuteAsync_DisabledWhenNoFilesystemSettings()
    {
        // Arrange
        var service = new TempFileCleanupService(_mockLogger.Object, _configuration, null);

        // Act
        _ = service.StartAsync(CancellationToken.None);

        // Assert - Should log that service is disabled
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Temp file cleanup service disabled")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task CleanupStaleTempFilesAsync_IgnoresNonTempFiles()
    {
        // Arrange
        var regularFile = Path.Combine(_testDataDirectory, "not-a-temp-file.txt");
        var tempLikeFile = Path.Combine(_testDataDirectory, "temp-but-wrong-prefix.tmp");
        var validTempFile = Path.Combine(_testDataDirectory, $"{_tempFilePrefix}valid-temp");

        await File.WriteAllTextAsync(regularFile, "regular content");
        await File.WriteAllTextAsync(tempLikeFile, "temp-like content");
        await File.WriteAllTextAsync(validTempFile, "valid temp content");

        // Make all files old
        File.SetLastWriteTimeUtc(regularFile, DateTime.UtcNow.AddMinutes(-5));
        File.SetLastWriteTimeUtc(tempLikeFile, DateTime.UtcNow.AddMinutes(-5));
        File.SetLastWriteTimeUtc(validTempFile, DateTime.UtcNow.AddMinutes(-5));

        var options = Options.Create(_filesystemSettings);
        var service = new TempFileCleanupService(_mockLogger.Object, _configuration, options);

        // Act
        var method = typeof(TempFileCleanupService).GetMethod("CleanupStaleTempFilesAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        method!.Invoke(service, new object[] { CancellationToken.None });

        // Assert
        Assert.True(File.Exists(regularFile), "Regular file should be preserved");
        Assert.True(File.Exists(tempLikeFile), "Temp-like file with wrong prefix should be preserved");
        Assert.False(File.Exists(validTempFile), "Valid temp file should be deleted");
    }
}