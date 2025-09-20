using System.Text.RegularExpressions;
using Lamina.Models;

namespace Lamina.Storage.Abstract;

public class BucketStorageFacade : IBucketStorageFacade
{
    private readonly IBucketDataStorage _dataStorage;
    private readonly IBucketMetadataStorage _metadataStorage;
    private readonly ILogger<BucketStorageFacade> _logger;
    private static readonly Regex BucketNameRegex = new(@"^[a-z0-9][a-z0-9.-]*[a-z0-9]$", RegexOptions.Compiled);
    private static readonly Regex IpAddressRegex = new(@"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", RegexOptions.Compiled);

    public BucketStorageFacade(
        IBucketDataStorage dataStorage,
        IBucketMetadataStorage metadataStorage,
        ILogger<BucketStorageFacade> logger)
    {
        _dataStorage = dataStorage;
        _metadataStorage = metadataStorage;
        _logger = logger;
    }

    public async Task<Bucket?> CreateBucketAsync(string bucketName, CreateBucketRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (!IsValidBucketName(bucketName))
        {
            _logger.LogWarning("Invalid bucket name: {BucketName}", bucketName);
            return null;
        }

        // Create bucket in data service
        var created = await _dataStorage.CreateBucketAsync(bucketName, cancellationToken);
        if (!created)
        {
            return null;
        }

        // Store metadata
        var bucket = await _metadataStorage.StoreBucketMetadataAsync(bucketName, request, cancellationToken);
        if (bucket == null)
        {
            // Rollback data creation if metadata storage failed
            await _dataStorage.DeleteBucketAsync(bucketName, cancellationToken);
            _logger.LogError("Failed to store metadata for bucket {BucketName}", bucketName);
            return null;
        }

        return bucket;
    }

    public async Task<Bucket?> GetBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.GetBucketMetadataAsync(bucketName, cancellationToken);
    }

    public async Task<ListBucketsResponse> ListBucketsAsync(CancellationToken cancellationToken = default)
    {
        var buckets = await _metadataStorage.GetAllBucketsMetadataAsync(cancellationToken);

        return new ListBucketsResponse
        {
            Buckets = buckets
        };
    }

    public async Task<bool> DeleteBucketAsync(string bucketName, bool force = false, CancellationToken cancellationToken = default)
    {
        // Delete metadata first
        await _metadataStorage.DeleteBucketMetadataAsync(bucketName, cancellationToken);

        // Then delete the actual bucket
        return await _dataStorage.DeleteBucketAsync(bucketName, cancellationToken);
    }

    public async Task<bool> BucketExistsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await _dataStorage.BucketExistsAsync(bucketName, cancellationToken);
    }

    public async Task<Bucket?> UpdateBucketTagsAsync(string bucketName, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.UpdateBucketTagsAsync(bucketName, tags, cancellationToken);
    }

    private static bool IsValidBucketName(string bucketName)
    {
        if (string.IsNullOrWhiteSpace(bucketName))
            return false;

        if (bucketName.Length < 3 || bucketName.Length > 63)
            return false;

        if (!BucketNameRegex.IsMatch(bucketName))
            return false;

        if (bucketName.Contains("..") || bucketName.Contains(".-") || bucketName.Contains("-."))
            return false;

        if (IpAddressRegex.IsMatch(bucketName))
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