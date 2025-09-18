namespace S3Test.Services;

public class FilesystemBucketDataService : IBucketDataService
{
    private readonly string _dataDirectory;
    private readonly ILogger<FilesystemBucketDataService> _logger;

    public FilesystemBucketDataService(
        IConfiguration configuration,
        ILogger<FilesystemBucketDataService> logger)
    {
        _dataDirectory = configuration["FilesystemStorage:DataDirectory"] ?? "/var/s3test/data";
        _logger = logger;

        Directory.CreateDirectory(_dataDirectory);
    }

    public Task<bool> CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        try
        {
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