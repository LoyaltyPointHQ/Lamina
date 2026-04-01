using Lamina.Storage.Filesystem.Locking;
using Xunit;

namespace Lamina.Storage.Filesystem.Tests.Locking;

public class InMemoryLockManagerTests : IDisposable
{
    private readonly InMemoryLockManager _lockManager;
    private readonly string _testDirectory;
    private readonly string _testFilePath;

    public InMemoryLockManagerTests()
    {
        _lockManager = new InMemoryLockManager();
        _testDirectory = Path.Combine(Path.GetTempPath(), $"inmemory-lock-test-{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);
        _testFilePath = Path.Combine(_testDirectory, "test-file.txt");
    }

    [Fact]
    public async Task ReadFileAsync_EmptyFile_ReturnsDefault()
    {
        // Arrange - create an empty file (simulates truncated/corrupted metadata)
        await File.WriteAllTextAsync(_testFilePath, "");

        // Act
        var result = await _lockManager.ReadFileAsync(_testFilePath, content =>
            Task.FromResult(content));

        // Assert - should return default (null) instead of passing empty string to callback
        Assert.Null(result);
    }

    [Fact]
    public async Task ReadFileAsync_WhitespaceOnlyFile_ReturnsDefault()
    {
        // Arrange
        await File.WriteAllTextAsync(_testFilePath, "   \n  ");

        // Act
        var result = await _lockManager.ReadFileAsync(_testFilePath, content =>
            Task.FromResult(content));

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task WriteFileAsync_IsAtomic_NeverExposesEmptyFile()
    {
        // Arrange - write initial content
        await _lockManager.WriteFileAsync(_testFilePath, "initial content");

        var sawEmptyContent = false;
        var iterations = 100;
        var cts = new CancellationTokenSource();

        // Act - concurrent reads while writing
        var readerTask = Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    if (File.Exists(_testFilePath))
                    {
                        var content = await File.ReadAllTextAsync(_testFilePath, cts.Token);
                        if (content.Length == 0)
                        {
                            sawEmptyContent = true;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (IOException)
                {
                    // File might be locked, that's fine
                }
            }
        });

        // Write many times to increase chance of catching race condition
        for (int i = 0; i < iterations; i++)
        {
            await _lockManager.WriteFileAsync(_testFilePath, $"content iteration {i}");
        }

        cts.Cancel();
        await readerTask;

        // Assert - reader should never have seen empty content
        Assert.False(sawEmptyContent, "Reader observed empty file content during write - write is not atomic");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, true);
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
    }
}
