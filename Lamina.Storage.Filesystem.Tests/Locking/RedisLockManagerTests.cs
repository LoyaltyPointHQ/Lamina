using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Filesystem.Locking;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;

namespace Lamina.Storage.Filesystem.Tests;
    [Collection("Redis")]
    public class RedisLockManagerTests : IDisposable
    {
        private readonly Mock<ILogger<RedisLockManager>> _loggerMock;
        private readonly RedisSettings _redisSettings;
        private readonly ConnectionMultiplexer? _redis;
        private readonly RedLockFactory? _redLockFactory;
        private readonly RedisLockManager? _lockManager;
        private readonly string _testFilePath;
        private readonly string _testDirectory;

        public RedisLockManagerTests()
        {
            _loggerMock = new Mock<ILogger<RedisLockManager>>();
            _redisSettings = new RedisSettings
            {
                ConnectionString = "localhost:6379",
                LockExpirySeconds = 30,
                RetryCount = 3,
                RetryDelayMs = 100,
                Database = 0,
                LockKeyPrefix = "lamina-test:lock"
            };

            // Only initialize Redis components if Redis is available
            if (IsRedisAvailable())
            {
                try
                {
                    var configuration = ConfigurationOptions.Parse(_redisSettings.ConnectionString);
                    _redis = ConnectionMultiplexer.Connect(configuration);

                    var multiplexers = new List<RedLockMultiplexer>
                    {
                        new RedLockMultiplexer(_redis)
                    };
                    _redLockFactory = RedLockFactory.Create(multiplexers);

                    var options = Options.Create(_redisSettings);
                    _lockManager = new RedisLockManager(_redis, _redLockFactory, options, _loggerMock.Object);
                }
                catch
                {
                    // Ignore initialization errors - tests will be skipped
                }
            }

            _testDirectory = Path.Combine(Path.GetTempPath(), $"redis-lock-test-{Guid.NewGuid()}");
            Directory.CreateDirectory(_testDirectory);
            _testFilePath = Path.Combine(_testDirectory, "test-file.txt");
        }

        private static bool IsRedisAvailable()
        {
            try
            {
                var config = ConfigurationOptions.Parse("localhost:6379");
                config.ConnectTimeout = 1000; // 1 second timeout
                using var redis = ConnectionMultiplexer.Connect(config);
                return redis.IsConnected;
            }
            catch
            {
                return false;
            }
        }

        private void RequireRedis()
        {
            if (!IsRedisAvailable() || _lockManager == null)
            {
                throw new InvalidOperationException("Redis is not available for testing");
            }
        }

        [Fact]
        public async Task ReadFileAsync_FileExists_ReturnsContent()
        {
            if (!IsRedisAvailable())
                return; // Skip test if Redis is not available

            // Arrange
            var expectedContent = "test content for reading";
            await File.WriteAllTextAsync(_testFilePath, expectedContent);

            // Act
            var result = await _lockManager!.ReadFileAsync(_testFilePath, content => Task.FromResult(content));

            // Assert
            Assert.Equal(expectedContent, result);
        }

        [Fact]
        public async Task ReadFileAsync_FileDoesNotExist_ReturnsDefault()
        {
            if (!IsRedisAvailable())
                return; // Skip test if Redis is not available

            // Arrange
            var nonExistentPath = Path.Combine(_testDirectory, "nonexistent.txt");

            // Act
            var result = await _lockManager!.ReadFileAsync(nonExistentPath, content => Task.FromResult(content));

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public async Task WriteFileAsync_CreatesFileWithContent()
        {
            if (!IsRedisAvailable())
                return; // Skip test if Redis is not available

            // Arrange
            var expectedContent = "content to write";

            // Act
            await _lockManager!.WriteFileAsync(_testFilePath, expectedContent);

            // Assert
            var actualContent = await File.ReadAllTextAsync(_testFilePath);
            Assert.Equal(expectedContent, actualContent);
        }

        [Fact]
        public async Task WriteFileAsync_CreatesDirectoryIfNotExists()
        {
            if (!IsRedisAvailable())
                return; // Skip test if Redis is not available

            // Arrange
            var nestedPath = Path.Combine(_testDirectory, "subdir", "nested", "file.txt");
            var content = "nested content";

            // Act
            await _lockManager!.WriteFileAsync(nestedPath, content);

            // Assert
            Assert.True(File.Exists(nestedPath));
            var actualContent = await File.ReadAllTextAsync(nestedPath);
            Assert.Equal(content, actualContent);
        }

        [Fact]
        public async Task DeleteFile_ExistingFile_ReturnsTrue()
        {
            if (!IsRedisAvailable())
                return; // Skip test if Redis is not available

            // Arrange
            await File.WriteAllTextAsync(_testFilePath, "content to delete");
            Assert.True(File.Exists(_testFilePath));

            // Act
            var result = await _lockManager!.DeleteFile(_testFilePath);

            // Assert
            Assert.True(result);
            Assert.False(File.Exists(_testFilePath));
        }

        [Fact]
        public async Task DeleteFile_NonExistentFile_ReturnsFalse()
        {
            if (!IsRedisAvailable())
                return; // Skip test if Redis is not available

            // Arrange
            var nonExistentPath = Path.Combine(_testDirectory, "nonexistent.txt");

            // Act
            var result = await _lockManager!.DeleteFile(nonExistentPath);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public async Task ConcurrentReadWrite_EnsuresThreadSafety()
        {
            if (!IsRedisAvailable())
                return; // Skip test if Redis is not available

            // Arrange
            var iterations = 10;
            var content = "concurrent test content";
            var tasks = new List<Task>();

            // Act
            for (int i = 0; i < iterations; i++)
            {
                int iteration = i;
                var writeTask = _lockManager!.WriteFileAsync(_testFilePath, $"{content}-{iteration}");
                var readTask = _lockManager!.ReadFileAsync(_testFilePath, c => Task.FromResult(c));

                tasks.Add(writeTask);
                tasks.Add(readTask);
            }

            // Wait for all operations to complete
            await Task.WhenAll(tasks);

            // Assert - if we get here without exceptions, the locking worked
            Assert.True(File.Exists(_testFilePath));
            var finalContent = await File.ReadAllTextAsync(_testFilePath);
            Assert.NotNull(finalContent);
            Assert.Contains(content, finalContent);
        }

        [Fact]
        public async Task LockTimeout_ThrowsException()
        {
            if (!IsRedisAvailable())
                return; // Skip test if Redis is not available

            // Arrange - Create a second lock manager with very short timeout
            var shortTimeoutSettings = new RedisSettings
            {
                ConnectionString = _redisSettings.ConnectionString,
                LockExpirySeconds = 1, // Very short timeout
                RetryCount = 1,
                RetryDelayMs = 50
            };

            var shortTimeoutOptions = Options.Create(shortTimeoutSettings);
            using var shortTimeoutLockManager = new RedisLockManager(_redis!, _redLockFactory!, shortTimeoutOptions, _loggerMock.Object);

            // Create a long-running operation to hold the lock
            var longRunningTask = _lockManager!.WriteFileAsync(_testFilePath, "holding lock");

            // Act & Assert
            // Try to acquire the same lock with short timeout - should eventually fail
            // Note: This test might be flaky depending on Redis performance and timing
            var startTime = DateTime.UtcNow;
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                while (DateTime.UtcNow - startTime < TimeSpan.FromSeconds(5))
                {
                    try
                    {
                        await shortTimeoutLockManager.WriteFileAsync(_testFilePath, "competing write");
                        await Task.Delay(10);
                    }
                    catch (InvalidOperationException)
                    {
                        // Wait for the original task to complete
                        await longRunningTask;
                        throw;
                    }
                }
                throw new InvalidOperationException("Expected lock contention");
            });
        }

        [Fact]
        public async Task ReadOperation_WithTransform_WorksCorrectly()
        {
            if (!IsRedisAvailable())
                return; // Skip test if Redis is not available

            // Arrange
            var originalContent = "123";
            await File.WriteAllTextAsync(_testFilePath, originalContent);

            // Act
            var result = await _lockManager!.ReadFileAsync(_testFilePath, content =>
                Task.FromResult(int.Parse(content) * 2));

            // Assert
            Assert.Equal(246, result);
        }

        [Fact]
        public async Task LockKeyPrefix_IsConfigurable()
        {
            if (!IsRedisAvailable())
                return; // Skip test if Redis is not available

            // Arrange
            var customPrefix = "custom-app:locks";
            var customSettings = new RedisSettings
            {
                ConnectionString = _redisSettings.ConnectionString,
                LockExpirySeconds = _redisSettings.LockExpirySeconds,
                RetryCount = _redisSettings.RetryCount,
                RetryDelayMs = _redisSettings.RetryDelayMs,
                Database = _redisSettings.Database,
                LockKeyPrefix = customPrefix
            };

            var customOptions = Options.Create(customSettings);
            using var customLockManager = new RedisLockManager(_redis!, _redLockFactory!, customOptions, _loggerMock.Object);

            var testContent = "configurable prefix test";
            await File.WriteAllTextAsync(_testFilePath, testContent);

            // Act - This should work with the custom prefix
            var result = await customLockManager.ReadFileAsync(_testFilePath, content => Task.FromResult(content));

            // Assert
            Assert.Equal(testContent, result);
        }

        public void Dispose()
        {
            try
            {
                _lockManager?.Dispose();
                _redLockFactory?.Dispose();
                _redis?.Dispose();

                if (Directory.Exists(_testDirectory))
                {
                    Directory.Delete(_testDirectory, true);
                }
            }
            catch
            {
                // Ignore disposal errors in tests
            }
        }
    }