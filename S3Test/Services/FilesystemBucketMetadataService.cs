using System.Text.Json;
using S3Test.Models;

namespace S3Test.Services;

public class FilesystemBucketMetadataService : IBucketMetadataService
{
    private readonly string _dataDirectory;
    private readonly string _metadataDirectory;
    private readonly IFileSystemLockManager _lockManager;
    private readonly IBucketDataService _dataService;
    private readonly ILogger<FilesystemBucketMetadataService> _logger;

    public FilesystemBucketMetadataService(
        IConfiguration configuration,
        IFileSystemLockManager lockManager,
        IBucketDataService dataService,
        ILogger<FilesystemBucketMetadataService> logger
    )
    {
        _dataDirectory = configuration["FilesystemStorage:DataDirectory"] ?? "/var/s3test/data";
        _metadataDirectory = configuration["FilesystemStorage:MetadataDirectory"] ?? "/var/s3test/metadata";
        _lockManager = lockManager;
        _dataService = dataService;
        _logger = logger;

        Directory.CreateDirectory(_metadataDirectory);
        Directory.CreateDirectory(Path.Combine(_metadataDirectory, "_buckets"));
    }

    private class BucketMetadata
    {
        public string? Region { get; set; }
        public Dictionary<string, string>? Tags { get; set; }
    }

    public async Task<Bucket?> StoreBucketMetadataAsync(string bucketName, CreateBucketRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (!await _dataService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var bucketDataPath = Path.Combine(_dataDirectory, bucketName);
        var dirInfo = new DirectoryInfo(bucketDataPath);

        var bucket = new Bucket
        {
            Name = bucketName,
            CreationDate = dirInfo.CreationTimeUtc,
            Region = request?.Region ?? "us-east-1",
            Tags = new Dictionary<string, string>()
        };

        await SaveBucketMetadataAsync(bucket, cancellationToken);
        return bucket;
    }

    public async Task<Bucket?> GetBucketMetadataAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        if (!await _dataService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var bucketDataPath = Path.Combine(_dataDirectory, bucketName);
        var dirInfo = new DirectoryInfo(bucketDataPath);

        var bucket = new Bucket
        {
            Name = bucketName,
            CreationDate = dirInfo.CreationTimeUtc,
            Region = "us-east-1",
            Tags = new Dictionary<string, string>()
        };

        // Try to load metadata if it exists
        var metadataFile = Path.Combine(_metadataDirectory, "_buckets", $"{bucketName}.json");
        if (File.Exists(metadataFile))
        {
             var metadata = await _lockManager.ReadFileAsync(metadataFile, content => Task.FromResult(JsonSerializer.Deserialize<BucketMetadata>(content)), cancellationToken);

            if (metadata != null)
            {
                bucket.Region = metadata.Region ?? "us-east-1";
                bucket.Tags = metadata.Tags ?? new Dictionary<string, string>();
            }
        }

        return bucket;
    }

    public async Task<List<Bucket>> GetAllBucketsMetadataAsync(CancellationToken cancellationToken = default)
    {
        var bucketNames = await _dataService.ListBucketNamesAsync(cancellationToken);
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

    public async Task<bool> DeleteBucketMetadataAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        try
        {
            var metadataFile = Path.Combine(_metadataDirectory, "_buckets", $"{bucketName}.json");
            if (File.Exists(metadataFile))
            {
                return await _lockManager.DeleteFileAsync(metadataFile, cancellationToken);
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete bucket metadata: {BucketName}", bucketName);
            return false;
        }
    }

    public async Task<Bucket?> UpdateBucketTagsAsync(string bucketName, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        var bucket = await GetBucketMetadataAsync(bucketName, cancellationToken);
        if (bucket == null)
        {
            return null;
        }

        bucket.Tags = tags ?? new Dictionary<string, string>();
        await SaveBucketMetadataAsync(bucket, cancellationToken);
        return bucket;
    }

    private async Task SaveBucketMetadataAsync(Bucket bucket, CancellationToken cancellationToken = default)
    {
        try
        {
            var metadataFile = Path.Combine(_metadataDirectory, "_buckets", $"{bucket.Name}.json");
            var metadata = new BucketMetadata
            {
                Region = bucket.Region,
                Tags = bucket.Tags
            };

            var json = JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true });
            await _lockManager.WriteFileAsync(metadataFile, () => Task.FromResult(json), cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save metadata for bucket {BucketName}", bucket.Name);
        }
    }
}