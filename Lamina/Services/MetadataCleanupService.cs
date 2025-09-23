using Lamina.Storage.Abstract;
using Microsoft.Extensions.DependencyInjection;

namespace Lamina.Services;

public class MetadataCleanupService : BackgroundService
{
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly ILogger<MetadataCleanupService> _logger;
    private readonly TimeSpan _cleanupInterval;
    private readonly int _batchSize;

    public MetadataCleanupService(
        IServiceScopeFactory serviceScopeFactory,
        ILogger<MetadataCleanupService> logger,
        IConfiguration configuration)
    {
        _serviceScopeFactory = serviceScopeFactory;
        _logger = logger;

        // Load configuration with defaults
        _cleanupInterval = TimeSpan.FromMinutes(configuration.GetValue("MetadataCleanup:CleanupIntervalMinutes", 120));
        _batchSize = configuration.GetValue("MetadataCleanup:BatchSize", 1000);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Metadata cleanup service started. Cleanup interval: {Interval}, Batch size: {BatchSize}",
            _cleanupInterval, _batchSize);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_cleanupInterval, stoppingToken);

                if (stoppingToken.IsCancellationRequested)
                    break;

                await CleanupStaleMetadataAsync(stoppingToken);
            }
            catch (TaskCanceledException)
            {
                // Expected when cancellation is requested
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during metadata cleanup");
            }
        }

        _logger.LogInformation("Metadata cleanup service stopped");
    }

    private async Task CleanupStaleMetadataAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceScopeFactory.CreateScope();

        var objectDataStorage = scope.ServiceProvider.GetRequiredService<IObjectDataStorage>();
        var objectMetadataStorage = scope.ServiceProvider.GetRequiredService<IObjectMetadataStorage>();

        _logger.LogDebug("Starting cleanup of stale metadata");

        var totalCleaned = 0;
        var totalProcessed = 0;
        var batch = new List<(string bucketName, string key)>();

        try
        {
            await foreach (var metadataEntry in objectMetadataStorage.ListAllMetadataKeysAsync(cancellationToken))
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                batch.Add(metadataEntry);
                totalProcessed++;

                // Process in batches to avoid memory issues and provide progress updates
                if (batch.Count >= _batchSize)
                {
                    var cleaned = await ProcessMetadataBatchAsync(objectDataStorage, objectMetadataStorage, batch, cancellationToken);
                    totalCleaned += cleaned;
                    batch.Clear();

                    _logger.LogDebug("Processed {ProcessedCount} metadata entries, cleaned {CleanedCount} so far",
                        totalProcessed, totalCleaned);
                }
            }

            // Process remaining items in final batch
            if (batch.Count > 0)
            {
                var cleaned = await ProcessMetadataBatchAsync(objectDataStorage, objectMetadataStorage, batch, cancellationToken);
                totalCleaned += cleaned;
            }

            if (totalCleaned > 0)
            {
                _logger.LogInformation("Cleaned up {CleanedCount} stale metadata entries out of {TotalCount} processed",
                    totalCleaned, totalProcessed);
            }
            else
            {
                _logger.LogDebug("No stale metadata found. Processed {TotalCount} entries", totalProcessed);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to perform metadata cleanup. Processed {ProcessedCount} entries, cleaned {CleanedCount}",
                totalProcessed, totalCleaned);
        }
    }

    private async Task<int> ProcessMetadataBatchAsync(
        IObjectDataStorage dataStorage,
        IObjectMetadataStorage metadataStorage,
        List<(string bucketName, string key)> batch,
        CancellationToken cancellationToken)
    {
        var cleanedCount = 0;

        foreach (var (bucketName, key) in batch)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            try
            {
                // Check if data exists for this metadata entry
                var dataExists = await dataStorage.DataExistsAsync(bucketName, key, cancellationToken);

                if (!dataExists)
                {
                    _logger.LogInformation("Found stale metadata without corresponding data: Bucket={Bucket}, Key={Key}",
                        bucketName, key);

                    // Delete the orphaned metadata
                    var deleted = await metadataStorage.DeleteMetadataAsync(bucketName, key, cancellationToken);
                    if (deleted)
                    {
                        cleanedCount++;
                        _logger.LogDebug("Successfully deleted stale metadata: Bucket={Bucket}, Key={Key}",
                            bucketName, key);
                    }
                    else
                    {
                        _logger.LogWarning("Failed to delete stale metadata: Bucket={Bucket}, Key={Key}",
                            bucketName, key);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing metadata entry: Bucket={Bucket}, Key={Key}",
                    bucketName, key);
            }
        }

        return cleanedCount;
    }
}