using Lamina.Storage.Core.Abstract;

namespace Lamina.WebApi.Services;

public class MultipartUploadCleanupService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<MultipartUploadCleanupService> _logger;
    private readonly TimeSpan _cleanupInterval;
    private readonly TimeSpan _uploadTimeout;

    public MultipartUploadCleanupService(
        IServiceProvider serviceProvider,
        ILogger<MultipartUploadCleanupService> logger,
        IConfiguration configuration)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;

        // Load configuration with defaults
        _cleanupInterval = TimeSpan.FromMinutes(configuration.GetValue("MultipartUploadCleanup:CleanupIntervalMinutes", 60));
        _uploadTimeout = TimeSpan.FromHours(configuration.GetValue("MultipartUploadCleanup:UploadTimeoutHours", 24));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Multipart upload cleanup service started. Cleanup interval: {Interval}, Upload timeout: {Timeout}",
            _cleanupInterval, _uploadTimeout);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_cleanupInterval, stoppingToken);

                if (stoppingToken.IsCancellationRequested)
                    break;

                await CleanupStaleUploadsAsync(stoppingToken);
            }
            catch (TaskCanceledException)
            {
                // Expected when cancellation is requested
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during multipart upload cleanup");
            }
        }

        _logger.LogInformation("Multipart upload cleanup service stopped");
    }

    private async Task CleanupStaleUploadsAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();

        var bucketServiceFacade = scope.ServiceProvider.GetRequiredService<IBucketStorageFacade>();
        var multipartUploadServiceFacade = scope.ServiceProvider.GetRequiredService<IMultipartUploadStorageFacade>();

        _logger.LogDebug("Starting cleanup of stale multipart uploads");

        var cutoffTime = DateTime.UtcNow.Subtract(_uploadTimeout);
        var totalCleaned = 0;

        try
        {
            // Get all buckets
            var bucketsResponse = await bucketServiceFacade.ListBucketsAsync(cancellationToken);

            foreach (var bucket in bucketsResponse.Buckets)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                try
                {
                    // List all multipart uploads for this bucket
                    var uploads = await multipartUploadServiceFacade.ListMultipartUploadsAsync(bucket.Name, cancellationToken);

                    foreach (var upload in uploads)
                    {
                        if (cancellationToken.IsCancellationRequested)
                            break;

                        // Check if upload is stale
                        if (upload.Initiated < cutoffTime)
                        {
                            try
                            {
                                _logger.LogInformation("Cleaning up stale multipart upload: Bucket={Bucket}, Key={Key}, UploadId={UploadId}, Initiated={Initiated}",
                                    upload.BucketName, upload.Key, upload.UploadId, upload.Initiated);

                                await multipartUploadServiceFacade.AbortMultipartUploadAsync(
                                    upload.BucketName,
                                    upload.Key,
                                    upload.UploadId,
                                    cancellationToken);

                                totalCleaned++;
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(ex, "Failed to cleanup multipart upload: Bucket={Bucket}, Key={Key}, UploadId={UploadId}",
                                    upload.BucketName, upload.Key, upload.UploadId);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to process bucket {Bucket} during cleanup", bucket.Name);
                }
            }

            if (totalCleaned > 0)
            {
                _logger.LogInformation("Cleaned up {Count} stale multipart uploads", totalCleaned);
            }
            else
            {
                _logger.LogDebug("No stale multipart uploads found");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to perform multipart upload cleanup");
        }
    }
}