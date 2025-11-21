using System.Runtime.InteropServices;
using Lamina.Storage.Filesystem.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace Lamina.Storage.Filesystem.Helpers;

public class NetworkFileSystemHelper
{
    private readonly NetworkFileSystemMode _mode;
    private readonly ILogger<NetworkFileSystemHelper> _logger;
    private readonly ResiliencePipeline? _resiliencePipeline;

    public NetworkFileSystemHelper(
        IOptions<FilesystemStorageSettings> settingsOptions,
        ILogger<NetworkFileSystemHelper> logger)
    {
        var settings = settingsOptions.Value;
        _mode = settings.NetworkMode;
        _logger = logger;

        // Only create pipeline for network filesystems
        if (_mode != NetworkFileSystemMode.None)
        {
            _resiliencePipeline = BuildResiliencePipeline(settings);
        }
    }

    private ResiliencePipeline BuildResiliencePipeline(FilesystemStorageSettings settings)
    {
        var retryOptions = new RetryStrategyOptions
        {
            MaxRetryAttempts = settings.RetryCount,
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true,
            Delay = TimeSpan.FromMilliseconds(settings.RetryDelayMs),

            // Custom exception filtering using existing ShouldRetry logic
            ShouldHandle = new PredicateBuilder().Handle<IOException>(ex => ShouldRetry(ex))
                .Handle<UnauthorizedAccessException>(_ => _mode == NetworkFileSystemMode.CIFS),

            // Logging on retry attempts
            OnRetry = args =>
            {
                var attemptNumber = args.AttemptNumber + 1; // Polly uses 0-based attempt numbers
                var totalAttempts = settings.RetryCount + 1;

                _logger.LogWarning(
                    args.Outcome.Exception,
                    "Network filesystem operation failed: {Message}. Retrying (attempt {Attempt}/{MaxAttempts}) after {Delay}ms.",
                    args.Outcome.Exception?.Message,
                    attemptNumber,
                    totalAttempts,
                    args.RetryDelay.TotalMilliseconds);

                return ValueTask.CompletedTask;
            }
        };

        // Build pipeline with retry strategy
        // Note: Telemetry is handled through OnRetry callback and logging
        return new ResiliencePipelineBuilder()
            .AddRetry(retryOptions)
            .Build();
    }

    public async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation, string operationName)
    {
        if (_resiliencePipeline == null)
        {
            // No network mode, execute directly
            return await operation();
        }

        try
        {
            return await _resiliencePipeline.ExecuteAsync(async ct => await operation(), CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Network filesystem operation {OperationName} failed after all retries", operationName);
            throw;
        }
    }

    public async Task ExecuteWithRetryAsync(Func<Task> operation, string operationName)
    {
        await ExecuteWithRetryAsync(async () =>
        {
            await operation();
            return true;
        }, operationName);
    }

    /// <summary>
    /// Ensures a directory exists, creating it if necessary.
    /// Validates that no file exists at the path. Synchronous version for use in constructors.
    /// </summary>
    /// <param name="directoryPath">The path to the directory to create.</param>
    /// <exception cref="InvalidOperationException">Thrown when a file exists at the directory path.</exception>
    public void EnsureDirectoryExists(string directoryPath)
    {
        // Fast path: directory already exists
        if (Directory.Exists(directoryPath))
        {
            return;
        }

        // Check if a FILE exists with this name (conflict scenario)
        if (File.Exists(directoryPath))
        {
            throw new InvalidOperationException(
                $"Cannot create directory at '{directoryPath}' because a file already exists at that path. " +
                "Please remove the file or configure a different directory path.");
        }

        // Create the directory
        Directory.CreateDirectory(directoryPath);
    }

    /// <summary>
    /// Ensures a directory exists, creating it if necessary.
    /// Validates that no file exists at the path and handles network filesystem transient errors.
    /// Async version with retry logic for use in runtime operations.
    /// </summary>
    /// <param name="directoryPath">The path to the directory to create.</param>
    /// <param name="operationName">Optional operation name for logging (defaults to "EnsureDirectory").</param>
    /// <exception cref="InvalidOperationException">Thrown when a file exists at the directory path.</exception>
    public async Task EnsureDirectoryExistsAsync(string directoryPath, string operationName = "EnsureDirectory")
    {
        await ExecuteWithRetryAsync(() =>
        {
            // Fast path: directory already exists
            if (Directory.Exists(directoryPath))
            {
                return Task.CompletedTask;
            }

            // Check if a FILE exists with this name (conflict scenario)
            // This must be checked before Directory.CreateDirectory, which would throw a generic IOException
            if (File.Exists(directoryPath))
            {
                throw new InvalidOperationException(
                    $"Cannot create directory at '{directoryPath}' because a file already exists at that path. " +
                    "Please remove the file or configure a different directory path.");
            }

            // Create the directory
            // This is safe even if the directory was created by another process between our checks
            Directory.CreateDirectory(directoryPath);

            return Task.CompletedTask;
        }, operationName);
    }

    public async Task<bool> AtomicMoveAsync(string sourcePath, string destPath, bool overwrite = false)
    {
        if (_mode == NetworkFileSystemMode.CIFS && overwrite && File.Exists(destPath))
        {
            // For CIFS with overwrite, use a two-step approach to be safer
            var tempBackup = $"{destPath}.backup_{Guid.NewGuid():N}";

            try
            {
                // First, rename existing file to backup
                await ExecuteWithRetryAsync(() =>
                {
                    File.Move(destPath, tempBackup);
                    return Task.CompletedTask;
                }, "BackupExistingFile");

                try
                {
                    // Then move new file to destination
                    await ExecuteWithRetryAsync(() =>
                    {
                        File.Move(sourcePath, destPath);
                        return Task.CompletedTask;
                    }, "MoveNewFile");

                    // Finally, delete backup
                    try
                    {
                        File.Delete(tempBackup);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to delete backup file {BackupPath}", tempBackup);
                    }

                    return true;
                }
                catch
                {
                    // If move fails, restore backup
                    try
                    {
                        if (!File.Exists(destPath) && File.Exists(tempBackup))
                        {
                            File.Move(tempBackup, destPath);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to restore backup file {BackupPath} to {DestPath}", tempBackup, destPath);
                    }
                    throw;
                }
            }
            catch
            {
                // Clean up backup if initial rename failed
                if (File.Exists(tempBackup))
                {
                    try { File.Delete(tempBackup); } catch { }
                }
                throw;
            }
        }
        else
        {
            // For NFS or non-overwrite scenarios, use standard atomic move
            await ExecuteWithRetryAsync(() =>
            {
                File.Move(sourcePath, destPath, overwrite);
                return Task.CompletedTask;
            }, "AtomicMove");
            return true;
        }
    }

    public async Task<bool> DeleteDirectoryIfEmptyAsync(string directoryPath, string? stopAtDirectory = null)
    {
        if (!Directory.Exists(directoryPath))
        {
            return false;
        }

        return await ExecuteWithRetryAsync(() =>
        {
            var directory = directoryPath;

            while (!string.IsNullOrEmpty(directory) &&
                   directory != stopAtDirectory)
            {
                if (Directory.Exists(directory) && !Directory.EnumerateFileSystemEntries(directory).Any())
                {
                    // For CIFS, we'll let the retry mechanism handle "directory not empty" errors
                    Directory.Delete(directory);
                    directory = Path.GetDirectoryName(directory);
                }
                else
                {
                    break;
                }
            }

            return Task.FromResult(true);
        }, "DirectoryCleanup");
    }

    private bool ShouldRetry(IOException ex)
    {
        if (_mode == NetworkFileSystemMode.None)
        {
            return false;
        }

        // Check for ESTALE error on NFS (Unix/Linux)
        if (_mode == NetworkFileSystemMode.NFS && RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            // ESTALE has errno 116 on Linux
            const int ESTALE = 116;
            var hResult = ex.HResult;

            // On Unix, HResult encodes errno in lower 16 bits
            if ((hResult & 0xFFFF) == ESTALE)
            {
                _logger.LogWarning("Detected ESTALE error on NFS, will retry");
                return true;
            }
        }

        // Common retryable patterns
        var message = ex.Message.ToLowerInvariant();

        var retryablePatterns = _mode switch
        {
            NetworkFileSystemMode.CIFS => new[]
            {
                "being used by another process",
                "network path was not found",
                "access is denied",
                "the process cannot access",
                "sharing violation",
                "specified network name is no longer available",
                "directory not empty",
                "the directory is not empty"
            },
            NetworkFileSystemMode.NFS => new[]
            {
                "stale file handle",
                "stale nfs file handle",
                "input/output error",
                "no such file or directory" // Can happen with stale handles
            },
            _ => Array.Empty<string>()
        };

        return retryablePatterns.Any(pattern => message.Contains(pattern));
    }
}
