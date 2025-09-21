using System.IO.Pipelines;
using Lamina.Helpers;
using Lamina.Storage.Abstract;

namespace Lamina.Storage.Filesystem;

public class FilesystemObjectDataStorage : IObjectDataStorage
{
    private readonly string _dataDirectory;
    private readonly ILogger<FilesystemObjectDataStorage> _logger;

    public FilesystemObjectDataStorage(
        IConfiguration configuration,
        ILogger<FilesystemObjectDataStorage> logger)
    {
        _dataDirectory = configuration["FilesystemStorage:DataDirectory"]
            ?? throw new InvalidOperationException("FilesystemStorage:DataDirectory configuration is required when using Filesystem storage");
        _logger = logger;

        Directory.CreateDirectory(_dataDirectory);
    }

    public async Task<(long size, string etag)> StoreDataAsync(string bucketName, string key, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);
        var dataDir = Path.GetDirectoryName(dataPath)!;
        Directory.CreateDirectory(dataDir);

        // Create a temporary file in the same directory to ensure atomic move
        var tempPath = Path.Combine(dataDir, $".tmp_{Guid.NewGuid():N}");
        long bytesWritten = 0;

        try
        {
            // Write the data to temp file, ensuring proper disposal before computing ETag
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

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
                        await fileStream.WriteAsync(segment, cancellationToken);
                        bytesWritten += segment.Length;
                    }

                    dataReader.AdvanceTo(buffer.End);

                    if (result.IsCompleted)
                    {
                        break;
                    }
                }

                await dataReader.CompleteAsync();
                await fileStream.FlushAsync(cancellationToken);
            } // FileStream is fully disposed here

            // Now compute ETag from the temp file on disk with a new file handle
            var etag = await ETagHelper.ComputeETagFromFileAsync(tempPath);

            // Atomically move the temp file to the final location
            File.Move(tempPath, dataPath, overwrite: true);

            return (bytesWritten, etag);
        }
        catch
        {
            // Clean up temp file if something went wrong
            try
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to clean up temporary file: {TempPath}", tempPath);
            }
            throw;
        }
    }

    public async Task<(long size, string etag)> StoreMultipartDataAsync(string bucketName, string key, IEnumerable<PipeReader> partReaders, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);
        var dataDir = Path.GetDirectoryName(dataPath)!;
        Directory.CreateDirectory(dataDir);

        // Create a temporary file in the same directory to ensure atomic move
        var tempPath = Path.Combine(dataDir, $".tmp_{Guid.NewGuid():N}");
        long totalBytesWritten = 0;

        try
        {
            // Write all parts to the temp file, ensuring proper disposal before computing ETag
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true);

                foreach (var reader in partReaders)
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var result = await reader.ReadAsync(cancellationToken);
                        var buffer = result.Buffer;

                        if (buffer.IsEmpty && result.IsCompleted)
                        {
                            break;
                        }

                        foreach (var segment in buffer)
                        {
                            await fileStream.WriteAsync(segment, cancellationToken);
                            totalBytesWritten += segment.Length;
                        }

                        reader.AdvanceTo(buffer.End);

                        if (result.IsCompleted)
                        {
                            break;
                        }
                    }
                    await reader.CompleteAsync();
                }

                await fileStream.FlushAsync(cancellationToken);
            } // FileStream is fully disposed here

            // Now compute ETag from the completed temp file with a new file handle
            var etag = await ETagHelper.ComputeETagFromFileAsync(tempPath);

            // Atomically move the temp file to the final location
            File.Move(tempPath, dataPath, overwrite: true);

            return (totalBytesWritten, etag);
        }
        catch
        {
            // Clean up temp file if something went wrong
            try
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to clean up temporary file: {TempPath}", tempPath);
            }
            throw;
        }
    }

    public async Task<bool> WriteDataToPipeAsync(string bucketName, string key, PipeWriter writer, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);

        if (!File.Exists(dataPath))
        {
            return false;
        }

        await using var fileStream = File.OpenRead(dataPath);
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
    }

    public Task<bool> DeleteDataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return Task.FromResult(false);
        }

        File.Delete(dataPath);

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

        return Task.FromResult(true);
    }

    private string GetDataPath(string bucketName, string key)
    {
        return Path.Combine(_dataDirectory, bucketName, key);
    }

}