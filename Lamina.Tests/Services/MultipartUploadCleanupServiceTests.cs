using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using Lamina.Models;
using Lamina.Services;
using Lamina.Storage.Abstract;
using Xunit;

namespace Lamina.Tests.Services;

public class MultipartUploadCleanupServiceTests
{
    private readonly Mock<IBucketStorageFacade> _mockBucketService;
    private readonly Mock<IMultipartUploadStorageFacade> _mockMultipartUploadService;
    private readonly Mock<ILogger<MultipartUploadCleanupService>> _mockLogger;
    private readonly IServiceProvider _serviceProvider;
    private readonly IConfiguration _configuration;

    public MultipartUploadCleanupServiceTests()
    {
        _mockBucketService = new Mock<IBucketStorageFacade>();
        _mockMultipartUploadService = new Mock<IMultipartUploadStorageFacade>();
        _mockLogger = new Mock<ILogger<MultipartUploadCleanupService>>();

        var services = new ServiceCollection();
        services.AddSingleton(_mockBucketService.Object);
        services.AddSingleton(_mockMultipartUploadService.Object);
        _serviceProvider = services.BuildServiceProvider();

        var inMemorySettings = new Dictionary<string, string>
        {
            {"MultipartUploadCleanup:CleanupIntervalMinutes", "1"},
            {"MultipartUploadCleanup:UploadTimeoutHours", "1"}
        };

        _configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(inMemorySettings!)
            .Build();
    }

    [Fact]
    public async Task CleanupService_Should_Abort_Stale_Uploads()
    {
        // Arrange
        var buckets = new ListBucketsResponse
        {
            Buckets = new List<Bucket>
            {
                new Bucket { Name = "test-bucket", CreationDate = DateTime.UtcNow.AddDays(-1) }
            }
        };

        var staleUpload = new MultipartUpload
        {
            UploadId = "stale-upload-id",
            BucketName = "test-bucket",
            Key = "stale-file.txt",
            Initiated = DateTime.UtcNow.AddHours(-25) // Older than 24 hours
        };

        var recentUpload = new MultipartUpload
        {
            UploadId = "recent-upload-id",
            BucketName = "test-bucket",
            Key = "recent-file.txt",
            Initiated = DateTime.UtcNow.AddMinutes(-30) // Recent upload
        };

        var uploads = new List<MultipartUpload> { staleUpload, recentUpload };

        _mockBucketService.Setup(x => x.ListBucketsAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(buckets);

        _mockMultipartUploadService.Setup(x => x.ListMultipartUploadsAsync("test-bucket", It.IsAny<CancellationToken>()))
            .ReturnsAsync(uploads);

        _mockMultipartUploadService.Setup(x => x.AbortMultipartUploadAsync(
            "test-bucket",
            "stale-file.txt",
            "stale-upload-id",
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var cleanupService = new MultipartUploadCleanupService(_serviceProvider, _mockLogger.Object, _configuration);

        // Use reflection to call the private CleanupStaleUploadsAsync method
        var method = typeof(MultipartUploadCleanupService).GetMethod("CleanupStaleUploadsAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        // Act
        await (Task)method!.Invoke(cleanupService, new object[] { CancellationToken.None })!;

        // Assert
        _mockMultipartUploadService.Verify(x => x.AbortMultipartUploadAsync(
            "test-bucket",
            "stale-file.txt",
            "stale-upload-id",
            It.IsAny<CancellationToken>()), Times.Once);

        _mockMultipartUploadService.Verify(x => x.AbortMultipartUploadAsync(
            "test-bucket",
            "recent-file.txt",
            "recent-upload-id",
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CleanupService_Should_Handle_Errors_Gracefully()
    {
        // Arrange
        var buckets = new ListBucketsResponse
        {
            Buckets = new List<Bucket>
            {
                new Bucket { Name = "test-bucket", CreationDate = DateTime.UtcNow.AddDays(-1) }
            }
        };

        _mockBucketService.Setup(x => x.ListBucketsAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(buckets);

        _mockMultipartUploadService.Setup(x => x.ListMultipartUploadsAsync("test-bucket", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Test exception"));

        var cleanupService = new MultipartUploadCleanupService(_serviceProvider, _mockLogger.Object, _configuration);

        // Use reflection to call the private CleanupStaleUploadsAsync method
        var method = typeof(MultipartUploadCleanupService).GetMethod("CleanupStaleUploadsAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        // Act & Assert - Should not throw
        await (Task)method!.Invoke(cleanupService, new object[] { CancellationToken.None })!;

        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Failed to process bucket")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task CleanupService_Should_Respect_Configuration()
    {
        // Arrange
        var customConfig = new Dictionary<string, string>
        {
            {"MultipartUploadCleanup:CleanupIntervalMinutes", "120"},
            {"MultipartUploadCleanup:UploadTimeoutHours", "48"}
        };

        var customConfiguration = new ConfigurationBuilder()
            .AddInMemoryCollection(customConfig!)
            .Build();

        var buckets = new ListBucketsResponse
        {
            Buckets = new List<Bucket>
            {
                new Bucket { Name = "test-bucket", CreationDate = DateTime.UtcNow.AddDays(-1) }
            }
        };

        var upload36HoursOld = new MultipartUpload
        {
            UploadId = "upload-36h",
            BucketName = "test-bucket",
            Key = "file-36h.txt",
            Initiated = DateTime.UtcNow.AddHours(-36)
        };

        var upload50HoursOld = new MultipartUpload
        {
            UploadId = "upload-50h",
            BucketName = "test-bucket",
            Key = "file-50h.txt",
            Initiated = DateTime.UtcNow.AddHours(-50)
        };

        var uploads = new List<MultipartUpload> { upload36HoursOld, upload50HoursOld };

        _mockBucketService.Setup(x => x.ListBucketsAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(buckets);

        _mockMultipartUploadService.Setup(x => x.ListMultipartUploadsAsync("test-bucket", It.IsAny<CancellationToken>()))
            .ReturnsAsync(uploads);

        _mockMultipartUploadService.Setup(x => x.AbortMultipartUploadAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var cleanupService = new MultipartUploadCleanupService(_serviceProvider, _mockLogger.Object, customConfiguration);

        // Use reflection to call the private CleanupStaleUploadsAsync method
        var method = typeof(MultipartUploadCleanupService).GetMethod("CleanupStaleUploadsAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        // Act
        await (Task)method!.Invoke(cleanupService, new object[] { CancellationToken.None })!;

        // Assert - Only the 50-hour old upload should be cleaned (older than 48 hours)
        _mockMultipartUploadService.Verify(x => x.AbortMultipartUploadAsync(
            "test-bucket",
            "file-36h.txt",
            "upload-36h",
            It.IsAny<CancellationToken>()), Times.Never);

        _mockMultipartUploadService.Verify(x => x.AbortMultipartUploadAsync(
            "test-bucket",
            "file-50h.txt",
            "upload-50h",
            It.IsAny<CancellationToken>()), Times.Once);
    }
}