using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;

namespace Lamina.WebApi.Services;

public class LifecycleExpirationService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<LifecycleExpirationService> _logger;
    private readonly TimeSpan _checkInterval;

    public LifecycleExpirationService(
        IServiceProvider serviceProvider,
        ILogger<LifecycleExpirationService> logger,
        IConfiguration configuration)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _checkInterval = TimeSpan.FromMinutes(configuration.GetValue("LifecycleExpiration:CheckIntervalMinutes", 60));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Lifecycle expiration service started. Check interval: {Interval}", _checkInterval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_checkInterval, stoppingToken);
                if (stoppingToken.IsCancellationRequested) break;

                await ExecuteCycleAsync(stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during lifecycle expiration cycle");
            }
        }

        _logger.LogInformation("Lifecycle expiration service stopped");
    }

    /// <summary>
    /// Runs one full expiration cycle across all buckets. Exposed for testing with an overridable clock.
    /// </summary>
    public async Task ExecuteCycleAsync(CancellationToken cancellationToken, DateTime? nowUtc = null)
    {
        using var scope = _serviceProvider.CreateScope();
        var bucketStorage = scope.ServiceProvider.GetRequiredService<IBucketStorageFacade>();
        var objectStorage = scope.ServiceProvider.GetRequiredService<IObjectStorageFacade>();
        var multipartStorage = scope.ServiceProvider.GetRequiredService<IMultipartUploadStorageFacade>();
        var objectMetadataStorage = scope.ServiceProvider.GetRequiredService<IObjectMetadataStorage>();

        var now = nowUtc ?? DateTime.UtcNow;
        var buckets = await bucketStorage.ListBucketsAsync(cancellationToken);

        foreach (var bucket in buckets.Buckets)
        {
            if (cancellationToken.IsCancellationRequested) break;

            var config = await bucketStorage.GetLifecycleConfigurationAsync(bucket.Name, cancellationToken);
            if (config == null || config.Rules.Count == 0) continue;

            var enabledRules = config.Rules.Where(r => r.Status == LifecycleRuleStatus.Enabled).ToList();
            if (enabledRules.Count == 0) continue;

            await ApplyExpirationAsync(bucket.Name, enabledRules, now, objectStorage, objectMetadataStorage, cancellationToken);
            await ApplyAbortMultipartAsync(bucket.Name, enabledRules, now, multipartStorage, cancellationToken);
        }
    }

    private async Task ApplyExpirationAsync(
        string bucketName,
        List<LifecycleRule> rules,
        DateTime now,
        IObjectStorageFacade objectStorage,
        IObjectMetadataStorage metadataStorage,
        CancellationToken cancellationToken)
    {
        var hasExpirationRule = rules.Any(r => r.Expiration != null);
        if (!hasExpirationRule) return;

        var keysToDelete = new List<string>();

        await foreach (var (b, key) in metadataStorage.ListAllMetadataKeysAsync(cancellationToken))
        {
            if (cancellationToken.IsCancellationRequested) break;
            if (b != bucketName) continue;

            var info = await metadataStorage.GetMetadataAsync(bucketName, key, cancellationToken);
            if (info == null) continue;

            foreach (var rule in rules)
            {
                if (rule.Expiration == null) continue;

                if (LifecycleRuleEvaluator.IsEligibleForExpiration(info, rule, now))
                {
                    keysToDelete.Add(key);
                    break;
                }
            }
        }

        foreach (var key in keysToDelete)
        {
            try
            {
                _logger.LogInformation("Lifecycle expiring object {Bucket}/{Key}", bucketName, key);
                await objectStorage.DeleteObjectAsync(bucketName, key, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to expire object {Bucket}/{Key}", bucketName, key);
            }
        }
    }

    private async Task ApplyAbortMultipartAsync(
        string bucketName,
        List<LifecycleRule> rules,
        DateTime now,
        IMultipartUploadStorageFacade multipartStorage,
        CancellationToken cancellationToken)
    {
        var abortRules = rules.Where(r => r.AbortIncompleteMultipartUpload != null).ToList();
        if (abortRules.Count == 0) return;

        var uploads = await multipartStorage.ListMultipartUploadsAsync(bucketName, cancellationToken);

        foreach (var upload in uploads)
        {
            if (cancellationToken.IsCancellationRequested) break;

            foreach (var rule in abortRules)
            {
                if (LifecycleRuleEvaluator.IsEligibleForMultipartAbort(upload, rule, now))
                {
                    try
                    {
                        _logger.LogInformation("Lifecycle aborting stale multipart upload {Bucket}/{Key} uploadId={UploadId}",
                            bucketName, upload.Key, upload.UploadId);
                        await multipartStorage.AbortMultipartUploadAsync(bucketName, upload.Key, upload.UploadId, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to abort multipart upload {Bucket}/{Key}", bucketName, upload.Key);
                    }
                    break;
                }
            }
        }
    }
}
