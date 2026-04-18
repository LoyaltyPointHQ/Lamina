using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.WebApi.Configuration;
using Lamina.WebApi.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace Lamina.WebApi.Tests.Services;

public class AutoBucketCreationServiceTests
{
    private readonly Mock<IBucketStorageFacade> _mockBucketStorage;
    private readonly Mock<ILogger<AutoBucketCreationService>> _mockLogger;

    public AutoBucketCreationServiceTests()
    {
        _mockBucketStorage = new Mock<IBucketStorageFacade>();
        _mockLogger = new Mock<ILogger<AutoBucketCreationService>>();
    }

    private AutoBucketCreationService CreateService(AutoBucketCreationSettings settings)
    {
        var options = new Mock<IOptions<AutoBucketCreationSettings>>();
        options.Setup(x => x.Value).Returns(settings);
        return new AutoBucketCreationService(options.Object, _mockBucketStorage.Object, _mockLogger.Object);
    }

    [Fact]
    public async Task CreateConfiguredBucketsAsync_WhenDisabled_DoesNotCreateBuckets()
    {
        // Arrange
        var settings = new AutoBucketCreationSettings { Enabled = false };
        var service = CreateService(settings);

        // Act
        await service.CreateConfiguredBucketsAsync();

        // Assert
        _mockBucketStorage.Verify(x => x.CreateBucketAsync(It.IsAny<string>(), It.IsAny<CreateBucketRequest?>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CreateConfiguredBucketsAsync_WhenNoBuckets_DoesNotCreateBuckets()
    {
        // Arrange
        var settings = new AutoBucketCreationSettings 
        { 
            Enabled = true, 
            Buckets = new List<BucketConfiguration>() 
        };
        var service = CreateService(settings);

        // Act
        await service.CreateConfiguredBucketsAsync();

        // Assert
        _mockBucketStorage.Verify(x => x.CreateBucketAsync(It.IsAny<string>(), It.IsAny<CreateBucketRequest?>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CreateConfiguredBucketsAsync_WithValidBucket_CreatesBucket()
    {
        // Arrange
        var settings = new AutoBucketCreationSettings 
        { 
            Enabled = true, 
            Buckets = new List<BucketConfiguration>
            {
                new BucketConfiguration { Name = "test-bucket", Type = BucketType.GeneralPurpose }
            }
        };
        var service = CreateService(settings);

        var mockBucket = new Bucket { Name = "test-bucket", CreationDate = DateTime.UtcNow };
        _mockBucketStorage.Setup(x => x.CreateBucketAsync("test-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose), 
                                                           It.IsAny<CancellationToken>()))
                         .ReturnsAsync(mockBucket);

        // Act
        await service.CreateConfiguredBucketsAsync();

        // Assert
        _mockBucketStorage.Verify(x => x.CreateBucketAsync("test-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose), 
                                                           It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateConfiguredBucketsAsync_WithExistingBucket_SkipsBucket()
    {
        // Arrange
        var settings = new AutoBucketCreationSettings 
        { 
            Enabled = true, 
            Buckets = new List<BucketConfiguration>
            {
                new BucketConfiguration { Name = "existing-bucket", Type = BucketType.GeneralPurpose }
            }
        };
        var service = CreateService(settings);

        _mockBucketStorage.Setup(x => x.CreateBucketAsync("existing-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose), 
                                                           It.IsAny<CancellationToken>()))
                         .ReturnsAsync((Bucket?)null);

        // Act
        await service.CreateConfiguredBucketsAsync();

        // Assert
        _mockBucketStorage.Verify(x => x.CreateBucketAsync("existing-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose), 
                                                           It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateConfiguredBucketsAsync_WithInvalidBucketName_SkipsBucket()
    {
        // Arrange
        var settings = new AutoBucketCreationSettings 
        { 
            Enabled = true, 
            Buckets = new List<BucketConfiguration>
            {
                new BucketConfiguration { Name = "INVALID_BUCKET_NAME", Type = BucketType.GeneralPurpose }, // uppercase not allowed
                new BucketConfiguration { Name = "valid-bucket", Type = BucketType.GeneralPurpose }
            }
        };
        var service = CreateService(settings);

        var mockBucket = new Bucket { Name = "valid-bucket", CreationDate = DateTime.UtcNow };
        _mockBucketStorage.Setup(x => x.CreateBucketAsync("valid-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose), 
                                                           It.IsAny<CancellationToken>()))
                         .ReturnsAsync(mockBucket);

        // Act
        await service.CreateConfiguredBucketsAsync();

        // Assert
        _mockBucketStorage.Verify(x => x.CreateBucketAsync("INVALID_BUCKET_NAME", 
                                                           It.IsAny<CreateBucketRequest>(), 
                                                           It.IsAny<CancellationToken>()), Times.Never);
        _mockBucketStorage.Verify(x => x.CreateBucketAsync("valid-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose), 
                                                           It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateConfiguredBucketsAsync_WithEmptyBucketName_SkipsBucket()
    {
        // Arrange
        var settings = new AutoBucketCreationSettings 
        { 
            Enabled = true, 
            Buckets = new List<BucketConfiguration>
            {
                new BucketConfiguration { Name = "", Type = BucketType.GeneralPurpose },
                new BucketConfiguration { Name = "valid-bucket", Type = BucketType.GeneralPurpose }
            }
        };
        var service = CreateService(settings);

        var mockBucket = new Bucket { Name = "valid-bucket", CreationDate = DateTime.UtcNow };
        _mockBucketStorage.Setup(x => x.CreateBucketAsync("valid-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose), 
                                                           It.IsAny<CancellationToken>()))
                         .ReturnsAsync(mockBucket);

        // Act
        await service.CreateConfiguredBucketsAsync();

        // Assert
        _mockBucketStorage.Verify(x => x.CreateBucketAsync("", 
                                                           It.IsAny<CreateBucketRequest>(), 
                                                           It.IsAny<CancellationToken>()), Times.Never);
        _mockBucketStorage.Verify(x => x.CreateBucketAsync("valid-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose), 
                                                           It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateConfiguredBucketsAsync_WithDirectoryBucket_CreatesBucketWithCorrectType()
    {
        // Arrange
        var settings = new AutoBucketCreationSettings 
        { 
            Enabled = true, 
            Buckets = new List<BucketConfiguration>
            {
                new BucketConfiguration { Name = "directory-bucket", Type = BucketType.Directory }
            }
        };
        var service = CreateService(settings);

        var mockBucket = new Bucket { Name = "directory-bucket", CreationDate = DateTime.UtcNow };
        _mockBucketStorage.Setup(x => x.CreateBucketAsync("directory-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.Directory), 
                                                           It.IsAny<CancellationToken>()))
                         .ReturnsAsync(mockBucket);

        // Act
        await service.CreateConfiguredBucketsAsync();

        // Assert
        _mockBucketStorage.Verify(x => x.CreateBucketAsync("directory-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.Directory), 
                                                           It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateConfiguredBucketsAsync_WithMixedBucketTypes_CreatesEachWithCorrectType()
    {
        // Arrange
        var settings = new AutoBucketCreationSettings 
        { 
            Enabled = true, 
            Buckets = new List<BucketConfiguration>
            {
                new BucketConfiguration { Name = "general-bucket", Type = BucketType.GeneralPurpose },
                new BucketConfiguration { Name = "directory-bucket", Type = BucketType.Directory }
            }
        };
        var service = CreateService(settings);

        var generalBucket = new Bucket { Name = "general-bucket", CreationDate = DateTime.UtcNow };
        var directoryBucket = new Bucket { Name = "directory-bucket", CreationDate = DateTime.UtcNow };
        
        _mockBucketStorage.Setup(x => x.CreateBucketAsync("general-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose), 
                                                           It.IsAny<CancellationToken>()))
                         .ReturnsAsync(generalBucket);
        
        _mockBucketStorage.Setup(x => x.CreateBucketAsync("directory-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.Directory), 
                                                           It.IsAny<CancellationToken>()))
                         .ReturnsAsync(directoryBucket);

        // Act
        await service.CreateConfiguredBucketsAsync();

        // Assert
        _mockBucketStorage.Verify(x => x.CreateBucketAsync("general-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose), 
                                                           It.IsAny<CancellationToken>()), Times.Once);
        _mockBucketStorage.Verify(x => x.CreateBucketAsync("directory-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.Directory), 
                                                           It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateConfiguredBucketsAsync_WithException_ContinuesWithOtherBuckets()
    {
        // Arrange
        var settings = new AutoBucketCreationSettings 
        { 
            Enabled = true, 
            Buckets = new List<BucketConfiguration>
            {
                new BucketConfiguration { Name = "failing-bucket", Type = BucketType.GeneralPurpose },
                new BucketConfiguration { Name = "success-bucket", Type = BucketType.GeneralPurpose }
            }
        };
        var service = CreateService(settings);

        _mockBucketStorage.Setup(x => x.CreateBucketAsync("failing-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose), 
                                                           It.IsAny<CancellationToken>()))
                         .ThrowsAsync(new Exception("Test exception"));

        var mockBucket = new Bucket { Name = "success-bucket", CreationDate = DateTime.UtcNow };
        _mockBucketStorage.Setup(x => x.CreateBucketAsync("success-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose), 
                                                           It.IsAny<CancellationToken>()))
                         .ReturnsAsync(mockBucket);

        // Act
        await service.CreateConfiguredBucketsAsync();

        // Assert
        _mockBucketStorage.Verify(x => x.CreateBucketAsync("failing-bucket", 
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose), 
                                                           It.IsAny<CancellationToken>()), Times.Once);
        _mockBucketStorage.Verify(x => x.CreateBucketAsync("success-bucket",
                                                           It.Is<CreateBucketRequest>(r => r.Type == BucketType.GeneralPurpose),
                                                           It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateConfiguredBucketsAsync_WithLifecycle_AppliesConfiguration()
    {
        var lifecycle = new LifecycleConfiguration
        {
            Rules = new()
            {
                new LifecycleRule
                {
                    Id = "expire-logs",
                    Status = LifecycleRuleStatus.Enabled,
                    Filter = new LifecycleFilter { Prefix = "logs/" },
                    Expiration = new LifecycleExpiration { Days = 7 }
                }
            }
        };
        var settings = new AutoBucketCreationSettings
        {
            Enabled = true,
            Buckets = new()
            {
                new BucketConfiguration { Name = "lc-bucket", Lifecycle = lifecycle }
            }
        };
        _mockBucketStorage.Setup(x => x.CreateBucketAsync("lc-bucket", It.IsAny<CreateBucketRequest?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new Bucket { Name = "lc-bucket", CreationDate = DateTime.UtcNow });
        _mockBucketStorage.Setup(x => x.SetLifecycleConfigurationAsync("lc-bucket", It.IsAny<LifecycleConfiguration>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        await CreateService(settings).CreateConfiguredBucketsAsync();

        _mockBucketStorage.Verify(x => x.SetLifecycleConfigurationAsync(
            "lc-bucket",
            It.Is<LifecycleConfiguration>(c => c.Rules.Count == 1 && c.Rules[0].Id == "expire-logs"),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateConfiguredBucketsAsync_WithLifecycle_BucketAlreadyExists_StillApplies()
    {
        var lifecycle = new LifecycleConfiguration
        {
            Rules = new() { new LifecycleRule
            {
                Id = "r1",
                Status = LifecycleRuleStatus.Enabled,
                Filter = new LifecycleFilter { Prefix = "" },
                Expiration = new LifecycleExpiration { Days = 1 }
            }}
        };
        var settings = new AutoBucketCreationSettings
        {
            Enabled = true,
            Buckets = new() { new BucketConfiguration { Name = "existing", Lifecycle = lifecycle } }
        };
        _mockBucketStorage.Setup(x => x.CreateBucketAsync("existing", It.IsAny<CreateBucketRequest?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((Bucket?)null);
        _mockBucketStorage.Setup(x => x.SetLifecycleConfigurationAsync("existing", It.IsAny<LifecycleConfiguration>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        await CreateService(settings).CreateConfiguredBucketsAsync();

        _mockBucketStorage.Verify(x => x.SetLifecycleConfigurationAsync(
            "existing", It.IsAny<LifecycleConfiguration>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateConfiguredBucketsAsync_InvalidLifecycle_SkippedAndLogged()
    {
        // Days=0 is invalid per S3 spec.
        var lifecycle = new LifecycleConfiguration
        {
            Rules = new() { new LifecycleRule
            {
                Id = "bad",
                Status = LifecycleRuleStatus.Enabled,
                Filter = new LifecycleFilter { Prefix = "" },
                Expiration = new LifecycleExpiration { Days = 0 }
            }}
        };
        var settings = new AutoBucketCreationSettings
        {
            Enabled = true,
            Buckets = new() { new BucketConfiguration { Name = "bad-bucket", Lifecycle = lifecycle } }
        };
        _mockBucketStorage.Setup(x => x.CreateBucketAsync("bad-bucket", It.IsAny<CreateBucketRequest?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new Bucket { Name = "bad-bucket", CreationDate = DateTime.UtcNow });

        await CreateService(settings).CreateConfiguredBucketsAsync();

        _mockBucketStorage.Verify(x => x.SetLifecycleConfigurationAsync(
            It.IsAny<string>(), It.IsAny<LifecycleConfiguration>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CreateConfiguredBucketsAsync_NoLifecycle_DoesNotTouchLifecycleStorage()
    {
        var settings = new AutoBucketCreationSettings
        {
            Enabled = true,
            Buckets = new() { new BucketConfiguration { Name = "plain-bucket" } }
        };
        _mockBucketStorage.Setup(x => x.CreateBucketAsync("plain-bucket", It.IsAny<CreateBucketRequest?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new Bucket { Name = "plain-bucket", CreationDate = DateTime.UtcNow });

        await CreateService(settings).CreateConfiguredBucketsAsync();

        _mockBucketStorage.Verify(x => x.SetLifecycleConfigurationAsync(
            It.IsAny<string>(), It.IsAny<LifecycleConfiguration>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}