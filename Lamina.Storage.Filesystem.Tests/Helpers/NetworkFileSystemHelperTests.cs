using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Tests.TestHelpers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;
using Xunit.Abstractions;

namespace Lamina.Storage.Filesystem.Tests.Helpers;

/// <summary>
/// Unit tests for NetworkFileSystemHelper retry logic and exception handling.
/// </summary>
public class NetworkFileSystemHelperTests
{
    private readonly ITestOutputHelper _output;

    public NetworkFileSystemHelperTests(ITestOutputHelper output)
    {
        _output = output;
    }

    #region Configuration Tests

    [Fact]
    public async Task ExecuteWithRetry_NoneMode_DoesNotRetry()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.None,
            RetryCount = 3,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            throw TransientFailureSimulator.CreateCIFSProcessInUseException();
        };

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(() => helper.ExecuteWithRetryAsync(operation, "TestOp"));
        Assert.Equal(1, attemptCount); // Should not retry
    }

    [Fact]
    public async Task ExecuteWithRetry_CIFSMode_EnablesRetry()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 3,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 3)
                throw TransientFailureSimulator.CreateCIFSProcessInUseException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(3, attemptCount); // Initial + 2 retries
    }

    [Fact]
    public async Task ExecuteWithRetry_NFSMode_EnablesRetry()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.NFS,
            RetryCount = 3,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 2)
                throw TransientFailureSimulator.CreateNFSStaleFileHandleException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(2, attemptCount); // Initial + 1 retry
    }

    #endregion

    #region Retry Logic Tests

    [Fact]
    public async Task ExecuteWithRetry_SucceedsOnFirstAttempt_NoRetry()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 3,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<int>> operation = () =>
        {
            attemptCount++;
            return Task.FromResult(42);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.Equal(42, result);
        Assert.Equal(1, attemptCount);
    }

    [Fact]
    public async Task ExecuteWithRetry_FailsOnce_RetriesAndSucceeds()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 3,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<string>> operation = () =>
        {
            attemptCount++;
            if (attemptCount == 1)
                throw TransientFailureSimulator.CreateCIFSProcessInUseException();
            return Task.FromResult("success");
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.Equal("success", result);
        Assert.Equal(2, attemptCount);
    }

    [Fact]
    public async Task ExecuteWithRetry_ExhaustsRetries_ThrowsLastException()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 2, // Will attempt 3 times total (initial + 2 retries)
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            throw TransientFailureSimulator.CreateCIFSProcessInUseException();
        };

        // Act & Assert
        var exception = await Assert.ThrowsAsync<IOException>(() =>
            helper.ExecuteWithRetryAsync(operation, "TestOp"));

        Assert.Equal(3, attemptCount); // Initial + 2 retries
        Assert.Contains("being used by another process", exception.Message);
    }

    [Fact]
    public async Task ExecuteWithRetry_ExponentialBackoff_DelaysCorrectly()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 3,
            RetryDelayMs = 50 // Base delay
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;
        var stopwatch = Stopwatch.StartNew();

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 4)
                throw TransientFailureSimulator.CreateCIFSProcessInUseException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");
        stopwatch.Stop();

        // Assert
        Assert.True(result);
        Assert.Equal(4, attemptCount);

        // With Polly's exponential backoff + jitter, delays are randomized around the base
        // Expected base delays: 50ms (2^0) + 100ms (2^1) + 200ms (2^2) = 350ms
        // Jitter can significantly reduce this, so we just verify it takes some time but not too much
        Assert.True(stopwatch.ElapsedMilliseconds >= 50,
            $"Expected at least 50ms with exponential backoff + jitter, got {stopwatch.ElapsedMilliseconds}ms");

        // Should take less than a full second with jitter
        Assert.True(stopwatch.ElapsedMilliseconds < 1500,
            $"Expected less than 1500ms, got {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public async Task ExecuteWithRetry_RespectMaxRetryCount()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.NFS,
            RetryCount = 5,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            throw TransientFailureSimulator.CreateNFSStaleFileHandleException();
        };

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(() => helper.ExecuteWithRetryAsync(operation, "TestOp"));
        Assert.Equal(6, attemptCount); // Initial + 5 retries
    }

    #endregion

    #region CIFS Exception Filtering Tests

    [Fact]
    public async Task CIFS_IOException_ProcessInUse_Retries()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 2,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 2)
                throw TransientFailureSimulator.CreateCIFSProcessInUseException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(2, attemptCount);
    }

    [Fact]
    public async Task CIFS_IOException_NetworkPathNotFound_Retries()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 2,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 2)
                throw TransientFailureSimulator.CreateCIFSNetworkPathNotFoundException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(2, attemptCount);
    }

    [Fact]
    public async Task CIFS_IOException_AccessDenied_Retries()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 2,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 2)
                throw TransientFailureSimulator.CreateCIFSAccessDeniedException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(2, attemptCount);
    }

    [Fact]
    public async Task CIFS_IOException_SharingViolation_Retries()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 2,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 2)
                throw TransientFailureSimulator.CreateCIFSSharingViolationException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(2, attemptCount);
    }

    [Fact]
    public async Task CIFS_IOException_NetworkNameUnavailable_Retries()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 2,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 2)
                throw TransientFailureSimulator.CreateCIFSNetworkNameUnavailableException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(2, attemptCount);
    }

    [Fact]
    public async Task CIFS_IOException_DirectoryNotEmpty_Retries()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 2,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 2)
                throw TransientFailureSimulator.CreateCIFSDirectoryNotEmptyException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(2, attemptCount);
    }

    [Fact]
    public async Task CIFS_UnauthorizedAccessException_Retries()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 2,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 2)
                throw TransientFailureSimulator.CreateCIFSUnauthorizedAccessException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(2, attemptCount);
    }

    [Fact]
    public async Task CIFS_NonTransientIOException_DoesNotRetry()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 3,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            throw TransientFailureSimulator.CreateNonTransientException();
        };

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(() => helper.ExecuteWithRetryAsync(operation, "TestOp"));
        Assert.Equal(1, attemptCount); // Should not retry
    }

    #endregion

    #region NFS Exception Filtering Tests

    [Fact]
    public async Task NFS_IOException_StaleFileHandle_Retries()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.NFS,
            RetryCount = 2,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 2)
                throw TransientFailureSimulator.CreateNFSStaleFileHandleException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(2, attemptCount);
    }

    [Fact]
    public async Task NFS_IOException_InputOutputError_Retries()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.NFS,
            RetryCount = 2,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 2)
                throw TransientFailureSimulator.CreateNFSInputOutputErrorException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(2, attemptCount);
    }

    [Fact]
    public async Task NFS_IOException_NoSuchFile_Retries()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.NFS,
            RetryCount = 2,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 2)
                throw TransientFailureSimulator.CreateNFSNoSuchFileException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(2, attemptCount);
    }

    [Fact]
    public async Task NFS_IOException_ESTALE_OnLinux_Retries()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.NFS,
            RetryCount = 2,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 2)
                throw TransientFailureSimulator.CreateNFSESTALEException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(2, attemptCount);
    }

    [Fact]
    public async Task NFS_NonTransientIOException_DoesNotRetry()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.NFS,
            RetryCount = 3,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            throw TransientFailureSimulator.CreateNonTransientException();
        };

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(() => helper.ExecuteWithRetryAsync(operation, "TestOp"));
        Assert.Equal(1, attemptCount); // Should not retry
    }

    [Fact]
    public async Task NFS_UnauthorizedAccessException_DoesNotRetry()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.NFS,
            RetryCount = 3,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            throw TransientFailureSimulator.CreateCIFSUnauthorizedAccessException();
        };

        // Act & Assert - UnauthorizedAccessException should NOT be retried for NFS
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            helper.ExecuteWithRetryAsync(operation, "TestOp"));
        Assert.Equal(1, attemptCount);
    }

    #endregion

    #region Helper Methods

    private NetworkFileSystemHelper CreateHelper(FilesystemStorageSettings settings)
    {
        return new NetworkFileSystemHelper(
            Options.Create(settings),
            new NullLogger<NetworkFileSystemHelper>()
        );
    }

    #endregion
}
