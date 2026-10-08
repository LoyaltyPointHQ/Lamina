using Lamina.Storage.Core.Configuration;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem.Tests;

public class RedisSettingsTests
{
    [Fact]
    public void DefaultTimeout_PreservesLegacy300Milliseconds()
    {
        var settings = new RedisSettings();
        settings.Validate();
        Assert.Equal(TimeSpan.FromMilliseconds(300), settings.GetAcquisitionTimeout());
    }

    [Fact]
    public void LegacyTimeout_IsUsedUnlessExplicitlyOverridden()
    {
        var settings = new RedisSettings { RetryCount = 7, RetryDelayMs = 20 };
        Assert.Equal(TimeSpan.FromMilliseconds(140), settings.GetAcquisitionTimeout());
        settings.AcquisitionTimeoutMs = 42;
        Assert.Equal(TimeSpan.FromMilliseconds(42), settings.GetAcquisitionTimeout());
    }

    [Fact]
    public void InvalidOptions_AreRejected()
    {
        RedisSettings[] invalid = [
            new() { AcquisitionTimeoutMs = -1 }, new() { LockExpirySeconds = 0 },
            new() { LockExpirySeconds = int.MaxValue }, new() { RetryCount = -1 },
            new() { RetryDelayMs = -1 }, new() { RetryCount = int.MaxValue, RetryDelayMs = int.MaxValue },
            new() { MinBusyWaitSleepTimeMs = -1 }, new() { MaxBusyWaitSleepTimeMs = 0 },
            new() { MinBusyWaitSleepTimeMs = 100, MaxBusyWaitSleepTimeMs = 10 },
            new() { Database = -1 }, new() { LockKeyPrefix = " " }
        ];
        foreach (var settings in invalid) Assert.Throws<OptionsValidationException>(settings.Validate);
    }

    [Fact]
    public void ZeroTimeout_AllowsSingleAttempt()
    {
        var settings = new RedisSettings { AcquisitionTimeoutMs = 0 };
        settings.Validate();
        Assert.Equal(TimeSpan.Zero, settings.GetAcquisitionTimeout());
    }
}
