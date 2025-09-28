using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Filesystem.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

public class FilesystemBucketDataStorage : IBucketDataStorage
{
    private readonly string _dataDirectory;
    private readonly string _inlineMetadataDirectoryName;
    private readonly MetadataStorageMode _metadataMode;
    private readonly ILogger<FilesystemBucketDataStorage> _logger;

    public FilesystemBucketDataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        ILogger<FilesystemBucketDataStorage> logger)
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _inlineMetadataDirectoryName = settings.InlineMetadataDirectoryName;
        _metadataMode = settings.MetadataMode;
        _logger = logger;

        Directory.CreateDirectory(_dataDirectory);
    }

    public Task<bool> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        try
        {
            // Don't allow creating buckets with the metadata directory name
            if (_metadataMode == MetadataStorageMode.Inline && bucketName == _inlineMetadataDirectoryName)
            {
                return Task.FromResult(false);
            }

            var bucketPath = Path.Combine(_dataDirectory, bucketName);

            if (Directory.Exists(bucketPath))
            {
                return Task.FromResult(false);
            }

            Directory.CreateDirectory(bucketPath);
            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create bucket directory: {BucketName}", bucketName);
            return Task.FromResult(false);
        }
    }

    public Task<bool> DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        try
        {
            var bucketPath = Path.Combine(_dataDirectory, bucketName);

            if (!Directory.Exists(bucketPath))
            {
                return Task.FromResult(false);
            }

            // Check if bucket is empty
            if (Directory.EnumerateFileSystemEntries(bucketPath).Any())
            {
                // Try to delete recursively (force delete)
                Directory.Delete(bucketPath, recursive: true);
            }
            else
            {
                Directory.Delete(bucketPath);
            }

            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete bucket directory: {BucketName}", bucketName);
            return Task.FromResult(false);
        }
    }

    public Task<bool> BucketExistsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        // Don't allow bucket names that match the metadata directory
        if (_metadataMode == MetadataStorageMode.Inline && bucketName == _inlineMetadataDirectoryName)
        {
            return Task.FromResult(false);
        }

        var bucketPath = Path.Combine(_dataDirectory, bucketName);
        return Task.FromResult(Directory.Exists(bucketPath));
    }

    public Task<List<string>> ListBucketNamesAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            if (!Directory.Exists(_dataDirectory))
            {
                return Task.FromResult(new List<string>());
            }

            var buckets = Directory.GetDirectories(_dataDirectory)
                .Select(Path.GetFileName)
                .Where(name => !string.IsNullOrEmpty(name))
                // Filter out metadata directory in inline mode
                .Where(name => _metadataMode != MetadataStorageMode.Inline || name != _inlineMetadataDirectoryName)
                .Select(name => name!)
                .OrderBy(name => name)
                .ToList();

            return Task.FromResult(buckets);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list bucket directories");
            return Task.FromResult(new List<string>());
        }
    }
}