using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Tests.TestHelpers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace Lamina.Storage.Filesystem.Tests.Helpers;

/// <summary>
/// Tests specific to Polly ResiliencePipeline integration in NetworkFileSystemHelper.
/// </summary>
public class NetworkFileSystemHelperPollyTests
{
    #region ResiliencePipeline Configuration Tests

    [Fact]
    public async Task ResiliencePipeline_CIFS_ConfiguredCorrectly()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 3,
            RetryDelayMs = 100
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<int>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 3)
                throw TransientFailureSimulator.CreateCIFSProcessInUseException();
            return Task.FromResult(42);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.Equal(42, result);
        Assert.Equal(3, attemptCount); // Should retry as configured
    }

    [Fact]
    public async Task ResiliencePipeline_NFS_ConfiguredCorrectly()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.NFS,
            RetryCount = 2,
            RetryDelayMs = 50
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<string>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 3)
                throw TransientFailureSimulator.CreateNFSStaleFileHandleException();
            return Task.FromResult("success");
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.Equal("success", result);
        Assert.Equal(3, attemptCount); // Initial + 2 retries
    }

    [Fact]
    public async Task ResiliencePipeline_None_NotCreated()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.None,
            RetryCount = 3,
            RetryDelayMs = 100
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
        Assert.Equal(1, attemptCount); // Should not retry when mode is None
    }

    [Fact]
    public async Task ResiliencePipeline_RetryCount_HonorsSettings()
    {
        // Arrange - Test with custom retry count
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 5, // Custom count
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
        Assert.Equal(6, attemptCount); // Initial + 5 retries
    }

    #endregion

    #region Polly Jitter Tests

    [Fact]
    public async Task Jitter_AddsRandomness_WithinBounds()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 3,
            RetryDelayMs = 100
        };

        var helper = CreateHelper(settings);
        var delays = new System.Collections.Generic.List<long>();

        // Run the same operation multiple times to observe jitter
        for (int i = 0; i < 5; i++)
        {
            var attemptCount = 0;
            var stopwatch = Stopwatch.StartNew();

            Func<Task<bool>> operation = () =>
            {
                attemptCount++;
                if (attemptCount < 4) // Will retry 3 times
                    throw TransientFailureSimulator.CreateCIFSProcessInUseException();
                return Task.FromResult(true);
            };

            await helper.ExecuteWithRetryAsync(operation, "TestOp");
            stopwatch.Stop();
            delays.Add(stopwatch.ElapsedMilliseconds);
        }

        // Assert - With jitter, delays should vary
        var uniqueDelays = delays.Distinct().Count();

        // With 5 runs and jitter enabled, we should see variation
        // (though theoretically could get same values, very unlikely)
        Assert.True(uniqueDelays >= 2,
            $"Expected variation in delays due to jitter, but got delays: {string.Join(", ", delays)}");

        // All delays should be within reasonable bounds
        // Base calculation: 100ms + 200ms + 400ms = 700ms
        // With jitter (-25% to +25%), range is roughly 525ms to 875ms
        foreach (var delay in delays)
        {
            Assert.True(delay >= 200 && delay < 1500,
                $"Delay {delay}ms outside expected range with jitter");
        }
    }

    [Fact]
    public async Task Jitter_MultipleRetries_DifferentDelays()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.NFS,
            RetryCount = 10, // More retries for better statistical sample
            RetryDelayMs = 50
        };

        var helper = CreateHelper(settings);
        var attemptTimes = new System.Collections.Generic.List<long>();
        var baseTime = Stopwatch.GetTimestamp();

        Func<Task<bool>> operation = () =>
        {
            var elapsed = Stopwatch.GetElapsedTime(baseTime);
            attemptTimes.Add((long)elapsed.TotalMilliseconds);
            throw TransientFailureSimulator.CreateNFSStaleFileHandleException();
        };

        // Act
        await Assert.ThrowsAsync<IOException>(() =>
            helper.ExecuteWithRetryAsync(operation, "TestOp"));

        // Assert
        Assert.Equal(11, attemptTimes.Count); // Initial + 10 retries

        // Calculate delays between attempts
        var delays = new System.Collections.Generic.List<long>();
        for (int i = 1; i < attemptTimes.Count; i++)
        {
            delays.Add(attemptTimes[i] - attemptTimes[i - 1]);
        }

        // With jitter, delays should increase exponentially but with variation
        // First delay should be around 50ms ± jitter, but could be very small with jitter
        Assert.True(delays[0] >= 0 && delays[0] <= 200,
            $"First delay {delays[0]}ms outside expected range");

        // Later delays should generally be larger than earlier ones (exponential)
        var avgFirstThree = delays.Take(3).Average();
        var avgLastThree = delays.Skip(delays.Count - 3).Average();
        Assert.True(avgLastThree > avgFirstThree,
            "Expected exponential backoff with later delays larger than earlier ones");
    }

    #endregion

    #region Exception Predicate Integration Tests

    [Fact]
    public async Task ExceptionPredicate_CIFS_FiltersCorrectly()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 2,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);

        // Test that CIFS-specific exceptions are retried
        var cifsExceptions = new Func<IOException>[]
        {
            TransientFailureSimulator.CreateCIFSProcessInUseException,
            TransientFailureSimulator.CreateCIFSNetworkPathNotFoundException,
            TransientFailureSimulator.CreateCIFSAccessDeniedException,
            TransientFailureSimulator.CreateCIFSSharingViolationException,
            TransientFailureSimulator.CreateCIFSDirectoryNotEmptyException
        };

        foreach (var createException in cifsExceptions)
        {
            var attemptCount = 0;
            Func<Task<bool>> operation = () =>
            {
                attemptCount++;
                if (attemptCount == 1)
                    throw createException();
                return Task.FromResult(true);
            };

            // Act
            var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

            // Assert
            Assert.True(result);
            Assert.Equal(2, attemptCount); // Should have retried
        }
    }

    [Fact]
    public async Task ExceptionPredicate_NFS_FiltersCorrectly()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.NFS,
            RetryCount = 2,
            RetryDelayMs = 10
        };

        var helper = CreateHelper(settings);

        // Test that NFS-specific exceptions are retried
        var nfsExceptions = new Func<IOException>[]
        {
            TransientFailureSimulator.CreateNFSStaleFileHandleException,
            TransientFailureSimulator.CreateNFSInputOutputErrorException,
            TransientFailureSimulator.CreateNFSNoSuchFileException,
            TransientFailureSimulator.CreateNFSESTALEException
        };

        foreach (var createException in nfsExceptions)
        {
            var attemptCount = 0;
            Func<Task<bool>> operation = () =>
            {
                attemptCount++;
                if (attemptCount == 1)
                    throw createException();
                return Task.FromResult(true);
            };

            // Act
            var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

            // Assert
            Assert.True(result);
            Assert.Equal(2, attemptCount); // Should have retried
        }
    }

    [Fact]
    public async Task ExceptionPredicate_UnexpectedException_DoesNotRetry()
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
            throw new InvalidOperationException("Not a transient error");
        };

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            helper.ExecuteWithRetryAsync(operation, "TestOp"));

        Assert.Equal(1, attemptCount); // Should not retry non-IO exceptions
    }

    [Fact]
    public async Task ExceptionPredicate_CIFS_UnauthorizedAccess_Retries()
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
        Assert.Equal(2, attemptCount); // Should retry UnauthorizedAccessException for CIFS
    }

    [Fact]
    public async Task ExceptionPredicate_NFS_UnauthorizedAccess_DoesNotRetry()
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

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            helper.ExecuteWithRetryAsync(operation, "TestOp"));

        Assert.Equal(1, attemptCount); // Should NOT retry UnauthorizedAccessException for NFS
    }

    #endregion

    #region Edge Cases

    [Fact]
    public async Task ResiliencePipeline_OneRetry_ExecutesTwice()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.CIFS,
            RetryCount = 1, // Single retry (Polly requires MaxRetryAttempts >= 1)
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
        Assert.Equal(2, attemptCount); // Initial + 1 retry
    }

    [Fact]
    public async Task ResiliencePipeline_VeryShortDelay_StillRetries()
    {
        // Arrange
        var settings = new FilesystemStorageSettings
        {
            NetworkMode = NetworkFileSystemMode.NFS,
            RetryCount = 2,
            RetryDelayMs = 1 // Very short delay
        };

        var helper = CreateHelper(settings);
        var attemptCount = 0;

        Func<Task<bool>> operation = () =>
        {
            attemptCount++;
            if (attemptCount < 3)
                throw TransientFailureSimulator.CreateNFSStaleFileHandleException();
            return Task.FromResult(true);
        };

        // Act
        var result = await helper.ExecuteWithRetryAsync(operation, "TestOp");

        // Assert
        Assert.True(result);
        Assert.Equal(3, attemptCount); // Should still retry even with tiny delay
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
