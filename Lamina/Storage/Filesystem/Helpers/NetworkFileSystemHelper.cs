using System.Runtime.InteropServices;
using Lamina.Storage.Filesystem.Configuration;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem.Helpers;

public class NetworkFileSystemHelper
{
    private readonly NetworkFileSystemMode _mode;
    private readonly int _retryCount;
    private readonly int _retryDelayMs;
    private readonly int _directoryCleanupDelayMs;
    private readonly ILogger<NetworkFileSystemHelper> _logger;

    public NetworkFileSystemHelper(
        IOptions<FilesystemStorageSettings> settingsOptions,
        ILogger<NetworkFileSystemHelper> logger)
    {
        var settings = settingsOptions.Value;
        _mode = settings.NetworkMode;
        _retryCount = settings.RetryCount;
        _retryDelayMs = settings.RetryDelayMs;
        _directoryCleanupDelayMs = settings.DirectoryCleanupDelayMs;
        _logger = logger;
    }

    public async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation, string operationName)
    {
        if (_mode == NetworkFileSystemMode.None)
        {
            return await operation();
        }

        var lastException = default(Exception);

        for (int attempt = 0; attempt <= _retryCount; attempt++)
        {
            try
            {
                if (attempt > 0)
                {
                    var delay = _retryDelayMs * Math.Pow(2, attempt - 1); // Exponential backoff
                    _logger.LogDebug("Retrying {OperationName} after {Delay}ms (attempt {Attempt}/{MaxAttempts})",
                        operationName, delay, attempt + 1, _retryCount + 1);
                    await Task.Delay(TimeSpan.FromMilliseconds(delay));
                }

                return await operation();
            }
            catch (IOException ex) when (ShouldRetry(ex) && attempt < _retryCount)
            {
                lastException = ex;
                _logger.LogWarning("Network filesystem operation {OperationName} failed: {Message}. Will retry.",
                    operationName, ex.Message);
            }
            catch (UnauthorizedAccessException ex) when (_mode == NetworkFileSystemMode.CIFS && attempt < _retryCount)
            {
                // CIFS can throw UnauthorizedAccessException for transient lock issues
                lastException = ex;
                _logger.LogWarning("CIFS operation {OperationName} failed with access error: {Message}. Will retry.",
                    operationName, ex.Message);
            }
        }

        _logger.LogError(lastException, "Network filesystem operation {OperationName} failed after {RetryCount} retries",
            operationName, _retryCount);
        throw lastException!;
    }

    public async Task ExecuteWithRetryAsync(Func<Task> operation, string operationName)
    {
        await ExecuteWithRetryAsync(async () =>
        {
            await operation();
            return true;
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