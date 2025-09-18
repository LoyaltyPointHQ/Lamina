using System.IO.Pipelines;
using System.Security.Cryptography;

namespace S3Test.Services;

public class FilesystemObjectDataService : IObjectDataService
{
    private readonly string _dataDirectory;
    private readonly ILogger<FilesystemObjectDataService> _logger;
    private readonly IFileSystemLockManager _lockManager;

    public FilesystemObjectDataService(
        IConfiguration configuration,
        ILogger<FilesystemObjectDataService> logger,
        IFileSystemLockManager lockManager)
    {
        _dataDirectory = configuration["FilesystemStorage:DataDirectory"] ?? "/var/s3test/data";
        _logger = logger;
        _lockManager = lockManager;

        Directory.CreateDirectory(_dataDirectory);
    }

    public async Task<(byte[] data, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);
        var dataDir = Path.GetDirectoryName(dataPath)!;
        Directory.CreateDirectory(dataDir);

        using var memoryStream = new MemoryStream();
        while (!cancellationToken.IsCancellationRequested)
        {
            var result = await dataReader.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            if (buffer.IsEmpty && result.IsCompleted)
            {
                break;
            }

            foreach (var segment in buffer)
            {
                await memoryStream.WriteAsync(segment, cancellationToken);
            }

            dataReader.AdvanceTo(buffer.End);

            if (result.IsCompleted)
            {
                break;
            }
        }

        await dataReader.CompleteAsync();

        var data = memoryStream.ToArray();
        var etag = ComputeETag(data);

        await _lockManager.WriteFileAsync(dataPath, async _ =>
        {
            await File.WriteAllBytesAsync(dataPath, data, cancellationToken);
            return dataPath;
        }, cancellationToken);

        return (data, etag);
    }

    public async Task<byte[]?> GetDataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);

        if (!File.Exists(dataPath))
        {
            return null;
        }

        return await _lockManager.ReadFileAsync(dataPath, async path =>
        {
            return await File.ReadAllBytesAsync(path, cancellationToken);
        }, cancellationToken);
    }

    public async Task<bool> WriteDataToPipeAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);

        if (!File.Exists(dataPath))
        {
            return false;
        }

        return await _lockManager.ReadFileAsync(dataPath, async path =>
        {
            await using var fileStream = File.OpenRead(path);
            const int bufferSize = 4096;
            var buffer = new byte[bufferSize];
            int bytesRead;

            while ((bytesRead = await fileStream.ReadAsync(buffer, cancellationToken)) > 0)
            {
                var memory = writer.GetMemory(bytesRead);
                buffer.AsMemory(0, bytesRead).CopyTo(memory);
                writer.Advance(bytesRead);
                await writer.FlushAsync(cancellationToken);
            }

            await writer.CompleteAsync();
            return true;
        }, cancellationToken);
    }

    public async Task<bool> DeleteDataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);
        var result = await _lockManager.DeleteFileAsync(dataPath, cancellationToken);

        // Clean up empty directories
        try
        {
            var directory = Path.GetDirectoryName(dataPath);
            while (!string.IsNullOrEmpty(directory) &&
                   directory.StartsWith(_dataDirectory) &&
                   directory != _dataDirectory)
            {
                if (Directory.Exists(directory) && !Directory.EnumerateFileSystemEntries(directory).Any())
                {
                    Directory.Delete(directory);
                    directory = Path.GetDirectoryName(directory);
                }
                else
                {
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to clean up empty directories for path: {DataPath}", dataPath);
        }

        return result;
    }

    public Task<bool> DataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);
        return Task.FromResult(File.Exists(dataPath));
    }

    public Task<long?> GetDataSizeAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);

        if (!File.Exists(dataPath))
        {
            return Task.FromResult<long?>(null);
        }

        var fileInfo = new FileInfo(dataPath);
        return Task.FromResult<long?>(fileInfo.Length);
    }

    private string GetDataPath(string bucketName, string key)
    {
        return Path.Combine(_dataDirectory, bucketName, key);
    }

    private static string ComputeETag(byte[] data)
    {
        using var md5 = MD5.Create();
        var hash = md5.ComputeHash(data);
        return Convert.ToHexString(hash).ToLower();
    }
}