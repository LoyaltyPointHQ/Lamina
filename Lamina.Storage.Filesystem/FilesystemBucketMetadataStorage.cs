using System.Text.Json;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Locking;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

public class FilesystemBucketMetadataStorage : IBucketMetadataStorage
{
    private readonly string _dataDirectory;
    private readonly string? _metadataDirectory;
    private readonly MetadataStorageMode _metadataMode;
    private readonly string _inlineMetadataDirectoryName;
    private readonly IFileSystemLockManager _lockManager;
    private readonly IBucketDataStorage _dataStorage;
    private readonly ILogger<FilesystemBucketMetadataStorage> _logger;

    public FilesystemBucketMetadataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        IFileSystemLockManager lockManager,
        IBucketDataStorage dataStorage,
        ILogger<FilesystemBucketMetadataStorage> logger
    )
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _metadataMode = settings.MetadataMode;
        _metadataDirectory = settings.MetadataDirectory;
        _inlineMetadataDirectoryName = settings.InlineMetadataDirectoryName;
        _lockManager = lockManager;
        _dataStorage = dataStorage;
        _logger = logger;

        Directory.CreateDirectory(_dataDirectory);

        if (_metadataMode == MetadataStorageMode.SeparateDirectory)
        {
            if (string.IsNullOrWhiteSpace(_metadataDirectory))
            {
                throw new InvalidOperationException("MetadataDirectory is required when using SeparateDirectory metadata mode");
            }
            Directory.CreateDirectory(_metadataDirectory);
            Directory.CreateDirectory(Path.Combine(_metadataDirectory, "_buckets"));
        }
        else
        {
            // Create the bucket metadata directory for inline mode
            var inlineBucketMetadataDir = Path.Combine(_dataDirectory, _inlineMetadataDirectoryName, "_buckets");
            Directory.CreateDirectory(inlineBucketMetadataDir);
        }
    }

    private class BucketMetadata
    {
        public BucketType Type { get; set; } = BucketType.GeneralPurpose;
        public string? StorageClass { get; set; }
        public Dictionary<string, string>? Tags { get; set; }
        public string? OwnerId { get; set; }
        public string? OwnerDisplayName { get; set; }
    }

    public async Task<Bucket?> StoreBucketMetadataAsync(string bucketName, CreateBucketRequest request, CancellationToken cancellationToken = default)
    {
        if (!await _dataStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var bucketDataPath = Path.Combine(_dataDirectory, bucketName);
        var dirInfo = new DirectoryInfo(bucketDataPath);

        var bucket = new Bucket
        {
            Name = bucketName,
            CreationDate = dirInfo.CreationTimeUtc,
            Type = request.Type ?? BucketType.GeneralPurpose,
            StorageClass = request.StorageClass,
            Tags = new Dictionary<string, string>(),
            OwnerId = request.OwnerId,
            OwnerDisplayName = request.OwnerDisplayName
        };

        await SaveBucketMetadataAsync(bucket, cancellationToken);
        return bucket;
    }

    public async Task<Bucket?> GetBucketMetadataAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        if (!await _dataStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var bucketDataPath = Path.Combine(_dataDirectory, bucketName);
        var dirInfo = new DirectoryInfo(bucketDataPath);

        var bucket = new Bucket
        {
            Name = bucketName,
            CreationDate = dirInfo.CreationTimeUtc,
            Type = BucketType.GeneralPurpose,
            Tags = new Dictionary<string, string>()
        };

        // Try to load metadata if it exists
        var metadataFile = GetBucketMetadataPath(bucketName);
        if (File.Exists(metadataFile))
        {
             var metadata = await _lockManager.ReadFileAsync(metadataFile, content => Task.FromResult(JsonSerializer.Deserialize<BucketMetadata>(content)), cancellationToken);

            if (metadata != null)
            {
                bucket.Type = metadata.Type;
                bucket.StorageClass = metadata.StorageClass;
                bucket.Tags = metadata.Tags ?? new Dictionary<string, string>();
                bucket.OwnerId = metadata.OwnerId;
                bucket.OwnerDisplayName = metadata.OwnerDisplayName;
            }
        }

        return bucket;
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
        try
        {
            var metadataFile = GetBucketMetadataPath(bucketName);
            if (File.Exists(metadataFile))
            {
                return _lockManager.DeleteFile(metadataFile);
            }

            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete bucket metadata: {BucketName}", bucketName);
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

        bucket.Tags = tags ?? new Dictionary<string, string>();
        await SaveBucketMetadataAsync(bucket, cancellationToken);
        return bucket;
    }

    private async Task SaveBucketMetadataAsync(Bucket bucket, CancellationToken cancellationToken = default)
    {
        try
        {
            var metadataFile = GetBucketMetadataPath(bucket.Name);
            var metadata = new BucketMetadata
            {
                Type = bucket.Type,
                StorageClass = bucket.StorageClass,
                Tags = bucket.Tags,
                OwnerId = bucket.OwnerId,
                OwnerDisplayName = bucket.OwnerDisplayName
            };

            var json = JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true });
            await _lockManager.WriteFileAsync(metadataFile, json, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save metadata for bucket {BucketName}", bucket.Name);
        }
    }

    private string GetBucketMetadataPath(string bucketName)
    {
        if (_metadataMode == MetadataStorageMode.SeparateDirectory)
        {
            return Path.Combine(_metadataDirectory!, "_buckets", $"{bucketName}.json");
        }
        else
        {
            return Path.Combine(_dataDirectory, _inlineMetadataDirectoryName, "_buckets", $"{bucketName}.json");
        }
    }
}