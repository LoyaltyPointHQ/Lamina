using Lamina.Configuration;
using Lamina.Models;
using Lamina.Storage.Abstract;
using Microsoft.Extensions.Options;

namespace Lamina.Services;

public interface IAutoBucketCreationService
{
    Task CreateConfiguredBucketsAsync(CancellationToken cancellationToken = default);
}

public class AutoBucketCreationService : IAutoBucketCreationService
{
    private readonly AutoBucketCreationSettings _settings;
    private readonly IBucketStorageFacade _bucketStorage;
    private readonly ILogger<AutoBucketCreationService> _logger;

    public AutoBucketCreationService(
        IOptions<AutoBucketCreationSettings> settings,
        IBucketStorageFacade bucketStorage,
        ILogger<AutoBucketCreationService> logger)
    {
        _settings = settings.Value;
        _bucketStorage = bucketStorage;
        _logger = logger;
    }

    public async Task CreateConfiguredBucketsAsync(CancellationToken cancellationToken = default)
    {
        if (!_settings.Enabled)
        {
            _logger.LogInformation("Auto bucket creation is disabled");
            return;
        }

        if (_settings.Buckets == null || !_settings.Buckets.Any())
        {
            _logger.LogInformation("No buckets configured for auto-creation");
            return;
        }

        _logger.LogInformation("Starting auto-creation of {BucketCount} configured buckets", _settings.Buckets.Count);

        foreach (var bucketConfig in _settings.Buckets)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(bucketConfig.Name))
                {
                    _logger.LogWarning("Skipping bucket with empty name");
                    continue;
                }

                if (!IsValidBucketName(bucketConfig.Name))
                {
                    _logger.LogWarning("Skipping bucket with invalid name: {BucketName}", bucketConfig.Name);
                    continue;
                }

                _logger.LogInformation("Creating bucket: {BucketName}", bucketConfig.Name);
                
                var bucket = await _bucketStorage.CreateBucketAsync(bucketConfig.Name, new CreateBucketRequest()
                {
                    Type = bucketConfig.Type
                }, cancellationToken);
                
                if (bucket == null)
                {
                    _logger.LogInformation("Bucket {BucketName} already exists, skipping creation", bucketConfig.Name);
                }
                else
                {
                    _logger.LogInformation("Successfully created bucket: {BucketName}", bucketConfig.Name);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to create bucket: {BucketName}", bucketConfig.Name);
            }
        }

        _logger.LogInformation("Completed auto-creation of configured buckets");
    }

    private static bool IsValidBucketName(string bucketName)
    {
        if (string.IsNullOrWhiteSpace(bucketName) || bucketName.Length < 3 || bucketName.Length > 63)
            return false;

        var regex = new System.Text.RegularExpressions.Regex(@"^[a-z0-9][a-z0-9.-]*[a-z0-9]$");
        if (!regex.IsMatch(bucketName))
            return false;

        if (bucketName.Contains("..") || bucketName.Contains(".-") || bucketName.Contains("-."))
            return false;

        var ipRegex = new System.Text.RegularExpressions.Regex(@"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$");
        if (ipRegex.IsMatch(bucketName))
            return false;

        string[] reservedPrefixes = { "xn--", "sthree-", "amzn-s3-demo-" };
        foreach (var prefix in reservedPrefixes)
        {
            if (bucketName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return true;
    }
}