using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.RegularExpressions;
using S3Test.Models;

namespace S3Test.Services;

public class FilesystemBucketService : IBucketService
{
    private readonly string _dataDirectory;
    private readonly string _metadataDirectory;
    private readonly ILogger<FilesystemBucketService> _logger;
    private readonly IFileSystemLockManager _lockManager;
    private static readonly Regex BucketNameRegex = new(@"^[a-z0-9][a-z0-9.-]*[a-z0-9]$", RegexOptions.Compiled);
    private static readonly Regex IpAddressRegex = new(@"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", RegexOptions.Compiled);

    public FilesystemBucketService(
        IConfiguration configuration,
        ILogger<FilesystemBucketService> logger,
        IFileSystemLockManager lockManager)
    {
        _dataDirectory = configuration["FilesystemStorage:DataDirectory"] ?? "/var/s3test/data";
        _metadataDirectory = configuration["FilesystemStorage:MetadataDirectory"] ?? "/var/s3test/metadata";
        _logger = logger;
        _lockManager = lockManager;

        Directory.CreateDirectory(_dataDirectory);
        Directory.CreateDirectory(_metadataDirectory);
    }

    private class BucketMetadata
    {
        public string? Region { get; set; }
        public Dictionary<string, string>? Tags { get; set; }
    }

    private async Task<Bucket?> LoadBucketFromDirectoryAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var bucketDataPath = Path.Combine(_dataDirectory, bucketName);
        if (!Directory.Exists(bucketDataPath))
        {
            return null;
        }

        var dirInfo = new DirectoryInfo(bucketDataPath);
        var bucket = new Bucket
        {
            Name = bucketName,
            CreationDate = dirInfo.CreationTimeUtc,
            Region = "us-east-1",
            Tags = new Dictionary<string, string>()
        };

        // Try to load metadata if it exists with lock
        var metadataFile = Path.Combine(_metadataDirectory, "_buckets", $"{bucketName}.json");
        if (File.Exists(metadataFile))
        {
            try
            {
                var metadata = await _lockManager.ReadFileAsync<BucketMetadata>(metadataFile, json =>
                {
                    return Task.FromResult(JsonSerializer.Deserialize<BucketMetadata>(json));
                }, cancellationToken);

                if (metadata != null)
                {
                    bucket.Region = metadata.Region ?? "us-east-1";
                    bucket.Tags = metadata.Tags ?? new Dictionary<string, string>();
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to load metadata for bucket {BucketName}, using defaults", bucketName);
            }
        }

        return bucket;
    }

    private async Task SaveBucketMetadataAsync(Bucket bucket, CancellationToken cancellationToken = default)
    {
        try
        {
            var bucketsMetadataPath = Path.Combine(_metadataDirectory, "_buckets");
            Directory.CreateDirectory(bucketsMetadataPath);

            var metadataFile = Path.Combine(bucketsMetadataPath, $"{bucket.Name}.json");
            var metadata = new BucketMetadata
            {
                Region = bucket.Region,
                Tags = bucket.Tags
            };

            // Write bucket metadata with lock
            await _lockManager.WriteFileAsync(metadataFile, _ =>
            {
                return Task.FromResult(JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true }));
            }, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save metadata for bucket {BucketName}", bucket.Name);
        }
    }

    public async Task<Bucket?> CreateBucketAsync(string bucketName, CreateBucketRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (!IsValidBucketName(bucketName))
        {
            _logger.LogWarning("Invalid bucket name: {BucketName}", bucketName);
            return null;
        }

        var bucketDataPath = Path.Combine(_dataDirectory, bucketName);

        // Check if bucket already exists
        if (Directory.Exists(bucketDataPath))
        {
            _logger.LogWarning("Bucket {BucketName} already exists", bucketName);
            return null;
        }

        try
        {
            var bucketMetadataPath = Path.Combine(_metadataDirectory, bucketName);

            Directory.CreateDirectory(bucketDataPath);
            Directory.CreateDirectory(bucketMetadataPath);

            // Load the bucket info from the newly created directory
            var bucket = await LoadBucketFromDirectoryAsync(bucketName, cancellationToken);
            if (bucket != null && request != null)
            {
                bucket.Region = request.Region ?? "us-east-1";
                await SaveBucketMetadataAsync(bucket, cancellationToken);
            }

            _logger.LogInformation("Created bucket {BucketName} with directories at {DataPath} and {MetadataPath}",
                bucketName, bucketDataPath, bucketMetadataPath);

            return bucket;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create bucket {BucketName} on filesystem", bucketName);

            // Try to clean up
            if (Directory.Exists(bucketDataPath) && !Directory.GetFileSystemEntries(bucketDataPath).Any())
            {
                try { Directory.Delete(bucketDataPath); } catch { }
            }

            return null;
        }
    }

    public async Task<Bucket?> GetBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var bucket = await LoadBucketFromDirectoryAsync(bucketName, cancellationToken);
        return bucket;
    }

    public async Task<ListBucketsResponse> ListBucketsAsync(CancellationToken cancellationToken = default)
    {
        var buckets = new List<Bucket>();

        try
        {
            // List all directories in the data directory as buckets
            foreach (var directory in Directory.GetDirectories(_dataDirectory))
            {
                var bucketName = Path.GetFileName(directory);
                var bucket = await LoadBucketFromDirectoryAsync(bucketName, cancellationToken);
                if (bucket != null)
                {
                    buckets.Add(bucket);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list buckets from filesystem");
        }

        var response = new ListBucketsResponse
        {
            Buckets = buckets.OrderBy(b => b.Name).ToList()
        };
        return response;
    }

    public async Task<bool> DeleteBucketAsync(string bucketName, bool force = false, CancellationToken cancellationToken = default)
    {
        var bucketDataPath = Path.Combine(_dataDirectory, bucketName);

        if (!Directory.Exists(bucketDataPath))
        {
            _logger.LogWarning("Bucket {BucketName} not found for deletion", bucketName);
            return false;
        }

        try
        {
            var bucketMetadataPath = Path.Combine(_metadataDirectory, bucketName);
            var bucketMetadataFile = Path.Combine(_metadataDirectory, "_buckets", $"{bucketName}.json");

            // If not forced, check if directories are empty
            if (!force)
            {
                // Check if data directory is empty
                if (Directory.GetFileSystemEntries(bucketDataPath).Any())
                {
                    _logger.LogWarning("Cannot delete non-empty bucket {BucketName}", bucketName);
                    return false;
                }

                // Check if metadata directory is empty
                if (Directory.Exists(bucketMetadataPath) && Directory.GetFileSystemEntries(bucketMetadataPath).Any())
                {
                    _logger.LogWarning("Cannot delete bucket {BucketName} with metadata", bucketName);
                    return false;
                }

                // Delete directories (non-recursive since they should be empty)
                Directory.Delete(bucketDataPath, false);

                if (Directory.Exists(bucketMetadataPath))
                {
                    Directory.Delete(bucketMetadataPath, false);
                }
            }
            else
            {
                // Force delete - remove all contents recursively
                Directory.Delete(bucketDataPath, recursive: true);

                if (Directory.Exists(bucketMetadataPath))
                {
                    Directory.Delete(bucketMetadataPath, recursive: true);
                }
            }

            if (File.Exists(bucketMetadataFile))
            {
                await _lockManager.DeleteFileAsync(bucketMetadataFile, cancellationToken);
            }

            _logger.LogInformation("Deleted bucket {BucketName} and its directories (force: {Force})", bucketName, force);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete bucket {BucketName} from filesystem", bucketName);
            return false;
        }
    }

    public Task<bool> BucketExistsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var bucketDataPath = Path.Combine(_dataDirectory, bucketName);
        return Task.FromResult(Directory.Exists(bucketDataPath));
    }

    public async Task<Bucket?> UpdateBucketTagsAsync(string bucketName, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        var bucket = await LoadBucketFromDirectoryAsync(bucketName, cancellationToken);
        if (bucket == null)
        {
            _logger.LogWarning("Bucket {BucketName} not found for tag update", bucketName);
            return null;
        }

        bucket.Tags = tags ?? new Dictionary<string, string>();
        await SaveBucketMetadataAsync(bucket, cancellationToken);

        _logger.LogInformation("Updated tags for bucket {BucketName}", bucketName);
        return bucket;
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