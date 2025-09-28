using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lamina.Storage.Core.Abstract;
using Lamina.WebApi.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Lamina.WebApi.Tests.Services;

public class MetadataCleanupServiceTests
{
    private readonly Mock<IObjectDataStorage> _mockDataStorage;
    private readonly Mock<IObjectMetadataStorage> _mockMetadataStorage;
    private readonly Mock<IServiceProvider> _mockServiceProvider;
    private readonly Mock<IServiceScope> _mockServiceScope;
    private readonly Mock<IServiceScopeFactory> _mockServiceScopeFactory;
    private readonly Mock<ILogger<MetadataCleanupService>> _mockLogger;
    private readonly IConfiguration _configuration;

    public MetadataCleanupServiceTests()
    {
        _mockDataStorage = new Mock<IObjectDataStorage>();
        _mockMetadataStorage = new Mock<IObjectMetadataStorage>();
        _mockServiceProvider = new Mock<IServiceProvider>();
        _mockServiceScope = new Mock<IServiceScope>();
        _mockServiceScopeFactory = new Mock<IServiceScopeFactory>();
        _mockLogger = new Mock<ILogger<MetadataCleanupService>>();

        // Setup service provider chain
        var mockScopeServiceProvider = new Mock<IServiceProvider>();
        mockScopeServiceProvider.Setup(x => x.GetService(typeof(IObjectDataStorage)))
            .Returns(_mockDataStorage.Object);
        mockScopeServiceProvider.Setup(x => x.GetService(typeof(IObjectMetadataStorage)))
            .Returns(_mockMetadataStorage.Object);

        _mockServiceScope.Setup(x => x.ServiceProvider)
            .Returns(mockScopeServiceProvider.Object);
        _mockServiceScopeFactory.Setup(x => x.CreateScope())
            .Returns(_mockServiceScope.Object);
        _mockServiceProvider.Setup(x => x.GetService(typeof(IServiceScopeFactory)))
            .Returns(_mockServiceScopeFactory.Object);

        // Setup configuration
        var configValues = new Dictionary<string, string>
        {
            ["MetadataCleanup:CleanupIntervalMinutes"] = "1", // 1 minute for faster testing
            ["MetadataCleanup:BatchSize"] = "2" // Small batch size for testing
        };
        _configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configValues!)
            .Build();
    }

    [Fact]
    public async Task CleanupStaleMetadataAsync_RemovesOrphanedMetadata()
    {
        // Arrange
        var metadataEntries = new List<(string bucketName, string key)>
        {
            ("bucket1", "key1"), // Has data - should not be deleted
            ("bucket1", "key2"), // No data - should be deleted
            ("bucket2", "key3")  // No data - should be deleted
        };

        _mockMetadataStorage.Setup(x => x.ListAllMetadataKeysAsync(It.IsAny<CancellationToken>()))
            .Returns(ToAsyncEnumerable(metadataEntries));

        // Only key1 has data
        _mockDataStorage.Setup(x => x.DataExistsAsync("bucket1", "key1", It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        _mockDataStorage.Setup(x => x.DataExistsAsync("bucket1", "key2", It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        _mockDataStorage.Setup(x => x.DataExistsAsync("bucket2", "key3", It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        _mockMetadataStorage.Setup(x => x.DeleteMetadataAsync("bucket1", "key2", It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        _mockMetadataStorage.Setup(x => x.DeleteMetadataAsync("bucket2", "key3", It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var service = new MetadataCleanupService(_mockServiceScopeFactory.Object, _mockLogger.Object, _configuration);

        // Act - Use reflection to call the private method
        var method = typeof(MetadataCleanupService).GetMethod("CleanupStaleMetadataAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        await (Task)method!.Invoke(service, new object[] { CancellationToken.None })!;

        // Assert
        // Verify that only orphaned metadata was deleted
        _mockMetadataStorage.Verify(x => x.DeleteMetadataAsync("bucket1", "key2", It.IsAny<CancellationToken>()), Times.Once);
        _mockMetadataStorage.Verify(x => x.DeleteMetadataAsync("bucket2", "key3", It.IsAny<CancellationToken>()), Times.Once);

        // Verify that metadata with existing data was not deleted
        _mockMetadataStorage.Verify(x => x.DeleteMetadataAsync("bucket1", "key1", It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CleanupStaleMetadataAsync_HandlesEmptyMetadata()
    {
        // Arrange
        var emptyMetadata = new List<(string bucketName, string key)>();

        _mockMetadataStorage.Setup(x => x.ListAllMetadataKeysAsync(It.IsAny<CancellationToken>()))
            .Returns(ToAsyncEnumerable(emptyMetadata));

        var service = new MetadataCleanupService(_mockServiceScopeFactory.Object, _mockLogger.Object, _configuration);

        // Act
        var method = typeof(MetadataCleanupService).GetMethod("CleanupStaleMetadataAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        await (Task)method!.Invoke(service, new object[] { CancellationToken.None })!;

        // Assert
        _mockMetadataStorage.Verify(x => x.DeleteMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CleanupStaleMetadataAsync_ProcessesBatchesCorrectly()
    {
        // Arrange - Create more entries than batch size (batch size is 2 in config)
        var metadataEntries = new List<(string bucketName, string key)>
        {
            ("bucket1", "key1"), // No data - should be deleted
            ("bucket1", "key2"), // No data - should be deleted
            ("bucket1", "key3"), // No data - should be deleted
            ("bucket1", "key4")  // No data - should be deleted
        };

        _mockMetadataStorage.Setup(x => x.ListAllMetadataKeysAsync(It.IsAny<CancellationToken>()))
            .Returns(ToAsyncEnumerable(metadataEntries));

        // None of the keys have data
        _mockDataStorage.Setup(x => x.DataExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        _mockMetadataStorage.Setup(x => x.DeleteMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var service = new MetadataCleanupService(_mockServiceScopeFactory.Object, _mockLogger.Object, _configuration);

        // Act
        var method = typeof(MetadataCleanupService).GetMethod("CleanupStaleMetadataAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        await (Task)method!.Invoke(service, new object[] { CancellationToken.None })!;

        // Assert - All 4 orphaned entries should be deleted
        _mockMetadataStorage.Verify(x => x.DeleteMetadataAsync("bucket1", "key1", It.IsAny<CancellationToken>()), Times.Once);
        _mockMetadataStorage.Verify(x => x.DeleteMetadataAsync("bucket1", "key2", It.IsAny<CancellationToken>()), Times.Once);
        _mockMetadataStorage.Verify(x => x.DeleteMetadataAsync("bucket1", "key3", It.IsAny<CancellationToken>()), Times.Once);
        _mockMetadataStorage.Verify(x => x.DeleteMetadataAsync("bucket1", "key4", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CleanupStaleMetadataAsync_HandlesExceptions()
    {
        // Arrange
        var metadataEntries = new List<(string bucketName, string key)>
        {
            ("bucket1", "key1"), // Will throw exception
            ("bucket1", "key2")  // Should still be processed
        };

        _mockMetadataStorage.Setup(x => x.ListAllMetadataKeysAsync(It.IsAny<CancellationToken>()))
            .Returns(ToAsyncEnumerable(metadataEntries));

        // First call throws exception, second succeeds
        _mockDataStorage.Setup(x => x.DataExistsAsync("bucket1", "key1", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Test exception"));
        _mockDataStorage.Setup(x => x.DataExistsAsync("bucket1", "key2", It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        _mockMetadataStorage.Setup(x => x.DeleteMetadataAsync("bucket1", "key2", It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var service = new MetadataCleanupService(_mockServiceScopeFactory.Object, _mockLogger.Object, _configuration);

        // Act
        var method = typeof(MetadataCleanupService).GetMethod("CleanupStaleMetadataAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        await (Task)method!.Invoke(service, new object[] { CancellationToken.None })!;

        // Assert - key2 should still be processed despite key1 throwing exception
        _mockMetadataStorage.Verify(x => x.DeleteMetadataAsync("bucket1", "key2", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CleanupStaleMetadataAsync_HandlesDeleteFailure()
    {
        // Arrange
        var metadataEntries = new List<(string bucketName, string key)>
        {
            ("bucket1", "key1") // No data but delete will fail
        };

        _mockMetadataStorage.Setup(x => x.ListAllMetadataKeysAsync(It.IsAny<CancellationToken>()))
            .Returns(ToAsyncEnumerable(metadataEntries));

        _mockDataStorage.Setup(x => x.DataExistsAsync("bucket1", "key1", It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        // Delete operation fails
        _mockMetadataStorage.Setup(x => x.DeleteMetadataAsync("bucket1", "key1", It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var service = new MetadataCleanupService(_mockServiceScopeFactory.Object, _mockLogger.Object, _configuration);

        // Act
        var method = typeof(MetadataCleanupService).GetMethod("CleanupStaleMetadataAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        await (Task)method!.Invoke(service, new object[] { CancellationToken.None })!;

        // Assert
        _mockMetadataStorage.Verify(x => x.DeleteMetadataAsync("bucket1", "key1", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CleanupStaleMetadataAsync_RespectsCancellation()
    {
        // Arrange
        var metadataEntries = new List<(string bucketName, string key)>
        {
            ("bucket1", "key1"),
            ("bucket1", "key2")
        };

        _mockMetadataStorage.Setup(x => x.ListAllMetadataKeysAsync(It.IsAny<CancellationToken>()))
            .Returns(ToAsyncEnumerable(metadataEntries));

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        var service = new MetadataCleanupService(_mockServiceScopeFactory.Object, _mockLogger.Object, _configuration);

        // Act
        var method = typeof(MetadataCleanupService).GetMethod("CleanupStaleMetadataAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        await (Task)method!.Invoke(service, new object[] { cts.Token })!;

        // Assert - Should not attempt to delete anything due to cancellation
        _mockMetadataStorage.Verify(x => x.DeleteMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public void Constructor_UsesDefaultConfiguration()
    {
        // Arrange
        var emptyConfig = new ConfigurationBuilder().Build();

        // Act
        var service = new MetadataCleanupService(_mockServiceScopeFactory.Object, _mockLogger.Object, emptyConfig);

        // Assert - Service should be created successfully with defaults
        Assert.NotNull(service);
    }

    private static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }
        await Task.CompletedTask;
    }
}