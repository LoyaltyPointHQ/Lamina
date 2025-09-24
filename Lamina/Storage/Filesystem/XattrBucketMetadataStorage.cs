using Lamina.Models;
using Lamina.Storage.Abstract;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

public class XattrBucketMetadataStorage : IBucketMetadataStorage
{
    private readonly string _dataDirectory;
    private readonly IBucketDataStorage _dataStorage;
    private readonly XattrHelper _xattrHelper;
    private readonly ILogger<XattrBucketMetadataStorage> _logger;

    private const string RegionAttributeName = "region";
    private const string TypeAttributeName = "type";
    private const string StorageClassAttributeName = "storage-class";
    private const string TagPrefix = "tag";

    public XattrBucketMetadataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        IBucketDataStorage dataStorage,
        ILogger<XattrBucketMetadataStorage> logger,
        ILoggerFactory loggerFactory)
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _dataStorage = dataStorage;
        _logger = logger;

        _xattrHelper = new XattrHelper(settings.XattrPrefix, loggerFactory.CreateLogger<XattrHelper>());

        if (!_xattrHelper.IsSupported)
        {
            throw new NotSupportedException("Extended attributes are not supported on this platform. Cannot use Xattr metadata storage mode.");
        }

        Directory.CreateDirectory(_dataDirectory);
    }

    public async Task<Bucket?> StoreBucketMetadataAsync(string bucketName, CreateBucketRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (!await _dataStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var bucketPath = Path.Combine(_dataDirectory, bucketName);
        if (!Directory.Exists(bucketPath))
        {
            _logger.LogError("Cannot store metadata for non-existent bucket directory: {BucketPath}", bucketPath);
            return null;
        }

        try
        {
            // Store region if provided
            var region = request?.Region ?? "us-east-1";
            if (!_xattrHelper.SetAttribute(bucketPath, RegionAttributeName, region))
            {
                _logger.LogWarning("Failed to store region attribute for bucket {BucketName}", bucketName);
            }

            // Store bucket type
            var bucketType = request?.Type ?? BucketType.GeneralPurpose;
            if (!_xattrHelper.SetAttribute(bucketPath, TypeAttributeName, bucketType.ToString()))
            {
                _logger.LogWarning("Failed to store type attribute for bucket {BucketName}", bucketName);
            }

            // Store storage class if provided
            if (!string.IsNullOrEmpty(request?.StorageClass))
            {
                if (!_xattrHelper.SetAttribute(bucketPath, StorageClassAttributeName, request.StorageClass))
                {
                    _logger.LogWarning("Failed to store storage class attribute for bucket {BucketName}", bucketName);
                }
            }

            // Get directory creation time
            var dirInfo = new DirectoryInfo(bucketPath);

            return new Bucket
            {
                Name = bucketName,
                CreationDate = dirInfo.CreationTimeUtc,
                Region = region,
                Type = bucketType,
                StorageClass = request?.StorageClass,
                Tags = new Dictionary<string, string>()
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to store metadata for bucket {BucketName}", bucketName);
            return null;
        }
    }

    public async Task<Bucket?> GetBucketMetadataAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        if (!await _dataStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var bucketPath = Path.Combine(_dataDirectory, bucketName);
        if (!Directory.Exists(bucketPath))
        {
            return null;
        }

        try
        {
            // Get directory creation time
            var dirInfo = new DirectoryInfo(bucketPath);

            // Get region from xattr, default to us-east-1
            var region = _xattrHelper.GetAttribute(bucketPath, RegionAttributeName) ?? "us-east-1";

            // Get bucket type from xattr, default to GeneralPurpose
            var typeString = _xattrHelper.GetAttribute(bucketPath, TypeAttributeName);
            var bucketType = BucketType.GeneralPurpose;
            if (!string.IsNullOrEmpty(typeString) && Enum.TryParse<BucketType>(typeString, out var parsedType))
            {
                bucketType = parsedType;
            }

            // Get storage class from xattr
            var storageClass = _xattrHelper.GetAttribute(bucketPath, StorageClassAttributeName);

            // Get tags from xattr
            var tags = GetBucketTags(bucketPath);

            return new Bucket
            {
                Name = bucketName,
                CreationDate = dirInfo.CreationTimeUtc,
                Region = region,
                Type = bucketType,
                StorageClass = storageClass,
                Tags = tags
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get metadata for bucket {BucketName}", bucketName);
            return null;
        }
    }

    public async Task<List<Bucket>> GetAllBucketsMetadataAsync(CancellationToken cancellationToken = default)
    {
        var bucketNames = await _dataStorage.ListBucketNamesAsync(cancellationToken);
        var buckets = new List<Bucket>();

        foreach (var bucketName in bucketNames)
        {
            var bucket = await GetBucketMetadataAsync(bucketName, cancellationToken);
            if (bucket != null)
            {
                buckets.Add(bucket);
            }
        }

        return buckets.OrderBy(b => b.Name).ToList();
    }

    public Task<bool> DeleteBucketMetadataAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var bucketPath = Path.Combine(_dataDirectory, bucketName);
        if (!Directory.Exists(bucketPath))
        {
            return Task.FromResult(true); // Consider it success if directory doesn't exist
        }

        try
        {
            return Task.FromResult(_xattrHelper.RemoveAllAttributes(bucketPath));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete metadata for bucket {BucketName}", bucketName);
            return Task.FromResult(false);
        }
    }

    public async Task<Bucket?> UpdateBucketTagsAsync(string bucketName, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        var bucket = await GetBucketMetadataAsync(bucketName, cancellationToken);
        if (bucket == null)
        {
            return null;
        }

        var bucketPath = Path.Combine(_dataDirectory, bucketName);

        try
        {
            // Remove all existing tag attributes
            RemoveAllTagAttributes(bucketPath);

            // Store new tags
            if (tags != null)
            {
                StoreBucketTags(bucketPath, tags, bucketName);
            }

            bucket.Tags = tags ?? new Dictionary<string, string>();
            return bucket;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update tags for bucket {BucketName}", bucketName);
            return null;
        }
    }

    private Dictionary<string, string> GetBucketTags(string bucketPath)
    {
        var tags = new Dictionary<string, string>();

        try
        {
            var attributes = _xattrHelper.ListAttributes(bucketPath);
            var tagPrefixDot = $"{TagPrefix}.";

            foreach (var attr in attributes)
            {
                if (attr.StartsWith(tagPrefixDot))
                {
                    var tagKey = attr[tagPrefixDot.Length..];
                    var tagValue = _xattrHelper.GetAttribute(bucketPath, attr);
                    if (tagValue != null)
                    {
                        tags[tagKey] = tagValue;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to retrieve bucket tags from {BucketPath}", bucketPath);
        }

        return tags;
    }

    private void StoreBucketTags(string bucketPath, Dictionary<string, string> tags, string bucketName)
    {
        foreach (var kvp in tags)
        {
            var attributeName = $"{TagPrefix}.{kvp.Key}";
            var success = _xattrHelper.SetAttribute(bucketPath, attributeName, kvp.Value);
            if (!success)
            {
                _logger.LogWarning("Failed to store tag attribute {AttributeName} for bucket {BucketName}",
                    attributeName, bucketName);
            }
        }
    }

    private void RemoveAllTagAttributes(string bucketPath)
    {
        try
        {
            var attributes = _xattrHelper.ListAttributes(bucketPath);
            var tagPrefixDot = $"{TagPrefix}.";

            foreach (var attr in attributes)
            {
                if (attr.StartsWith(tagPrefixDot))
                {
                    _xattrHelper.RemoveAttribute(bucketPath, attr);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to remove tag attributes from {BucketPath}", bucketPath);
        }
    }
}