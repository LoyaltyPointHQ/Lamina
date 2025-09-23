using System.Runtime.CompilerServices;
using Lamina.Storage.Filesystem.Configuration;
using Microsoft.Extensions.Options;

namespace Lamina.Services;

public class TempFileCleanupService : BackgroundService
{
    private readonly ILogger<TempFileCleanupService> _logger;
    private readonly TimeSpan _cleanupInterval;
    private readonly TimeSpan _tempFileAge;
    private readonly int _batchSize;
    private readonly string? _dataDirectory;
    private readonly string? _tempFilePrefix;

    public TempFileCleanupService(
        ILogger<TempFileCleanupService> logger,
        IConfiguration configuration,
        IOptions<FilesystemStorageSettings>? filesystemSettings = null)
    {
        _logger = logger;

        // Load configuration with defaults
        _cleanupInterval = TimeSpan.FromMinutes(configuration.GetValue("TempFileCleanup:CleanupIntervalMinutes", 60));
        _tempFileAge = TimeSpan.FromMinutes(configuration.GetValue("TempFileCleanup:TempFileAgeMinutes", 30));
        _batchSize = configuration.GetValue("TempFileCleanup:BatchSize", 100);

        // Get filesystem settings if available
        if (filesystemSettings?.Value != null)
        {
            _dataDirectory = filesystemSettings.Value.DataDirectory;
            _tempFilePrefix = filesystemSettings.Value.TempFilePrefix;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Only run if we have filesystem storage configured
        if (string.IsNullOrEmpty(_dataDirectory) || string.IsNullOrEmpty(_tempFilePrefix))
        {
            _logger.LogInformation("Temp file cleanup service disabled - not using filesystem storage");
            return;
        }

        _logger.LogInformation("Temp file cleanup service started. Cleanup interval: {Interval}, Temp file age threshold: {Age}, Batch size: {BatchSize}",
            _cleanupInterval, _tempFileAge, _batchSize);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_cleanupInterval, stoppingToken);

                if (stoppingToken.IsCancellationRequested)
                    break;

                CleanupStaleTempFilesAsync(stoppingToken);
            }
            catch (TaskCanceledException)
            {
                // Expected when cancellation is requested
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during temp file cleanup");
            }
        }

        _logger.LogInformation("Temp file cleanup service stopped");
    }

    private void CleanupStaleTempFilesAsync(CancellationToken cancellationToken)
    {
        if (!Directory.Exists(_dataDirectory))
        {
            _logger.LogWarning("Data directory does not exist: {DataDirectory}", _dataDirectory);
            return;
        }

        _logger.LogDebug("Starting cleanup of stale temporary files in {DataDirectory}", _dataDirectory);

        var totalCleaned = 0;
        var totalProcessed = 0;
        var batch = new List<string>();
        var cutoffTime = DateTime.UtcNow - _tempFileAge;

        try
        {
            foreach (var tempFilePath in FindTempFilesAsync(_dataDirectory!, _tempFilePrefix!, cancellationToken))
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                batch.Add(tempFilePath);
                totalProcessed++;

                // Process in batches to avoid memory issues and provide progress updates
                if (batch.Count >= _batchSize)
                {
                    var cleaned = ProcessTempFileBatchAsync(batch, cutoffTime, cancellationToken);
                    totalCleaned += cleaned;
                    batch.Clear();

                    _logger.LogDebug("Processed {ProcessedCount} temp files, cleaned {CleanedCount} so far",
                        totalProcessed, totalCleaned);
                }
            }

            // Process remaining items in final batch
            if (batch.Count > 0)
            {
                var cleaned = ProcessTempFileBatchAsync(batch, cutoffTime, cancellationToken);
                totalCleaned += cleaned;
            }

            if (totalCleaned > 0)
            {
                _logger.LogInformation("Cleaned up {CleanedCount} stale temp files out of {TotalCount} processed",
                    totalCleaned, totalProcessed);
            }
            else
            {
                _logger.LogDebug("No stale temp files found. Processed {TotalCount} files", totalProcessed);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to perform temp file cleanup. Processed {ProcessedCount} files, cleaned {CleanedCount}",
                totalProcessed, totalCleaned);
        }
    }

    private IEnumerable<string> FindTempFilesAsync(string directory, string tempFilePrefix, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var searchPattern = $"{tempFilePrefix}*";

        foreach (var filePath in EnumerateFilesRecursivelyAsync(directory, searchPattern, cancellationToken))
        {
            yield return filePath;
        }
    }

    private IEnumerable<string> EnumerateFilesRecursivelyAsync(string directory, string searchPattern, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            yield break;

        IEnumerable<string> files;
        IEnumerable<string> directories;

        try
        {
            files = Directory.EnumerateFiles(directory, searchPattern, SearchOption.TopDirectoryOnly);
            directories = Directory.EnumerateDirectories(directory);
        }
        catch (UnauthorizedAccessException ex)
        {
            _logger.LogWarning(ex, "Access denied to directory: {Directory}", directory);
            yield break;
        }
        catch (DirectoryNotFoundException ex)
        {
            _logger.LogWarning(ex, "Directory not found: {Directory}", directory);
            yield break;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error accessing directory: {Directory}", directory);
            yield break;
        }

        // Yield files in current directory
        foreach (var file in files)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            yield return file;
        }

        // Recursively process subdirectories
        foreach (var subDirectory in directories)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            foreach (var file in EnumerateFilesRecursivelyAsync(subDirectory, searchPattern, cancellationToken))
            {
                yield return file;
            }
        }
    }

    private int ProcessTempFileBatchAsync(
        List<string> batch,
        DateTime cutoffTime,
        CancellationToken cancellationToken)
    {
        var cleanedCount = 0;

        foreach (var filePath in batch)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            try
            {
                var fileInfo = new FileInfo(filePath);
                if (!fileInfo.Exists)
                {
                    // File was already deleted or moved
                    continue;
                }

                // Check if file is older than the cutoff time
                if (fileInfo.LastWriteTimeUtc < cutoffTime)
                {
                    _logger.LogDebug("Deleting stale temp file: {FilePath} (last modified: {LastModified})",
                        filePath, fileInfo.LastWriteTimeUtc);

                    File.Delete(filePath);
                    cleanedCount++;

                    _logger.LogDebug("Successfully deleted stale temp file: {FilePath}", filePath);
                }
                else
                {
                    _logger.LogDebug("Temp file is not stale, keeping: {FilePath} (last modified: {LastModified})",
                        filePath, fileInfo.LastWriteTimeUtc);
                }
            }
            catch (IOException ex)
            {
                _logger.LogWarning(ex, "IO error processing temp file (file may be in use): {FilePath}", filePath);
            }
            catch (UnauthorizedAccessException ex)
            {
                _logger.LogWarning(ex, "Access denied to temp file: {FilePath}", filePath);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing temp file: {FilePath}", filePath);
            }
        }

        return cleanedCount;
    }
}