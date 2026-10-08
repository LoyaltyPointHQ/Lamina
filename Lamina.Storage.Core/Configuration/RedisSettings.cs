using Microsoft.Extensions.Options;

namespace Lamina.Storage.Core.Configuration;

public class RedisSettings
{
    public string ConnectionString { get; set; } = "localhost:6379";
    public int LockExpirySeconds { get; set; } = 30;
    // Legacy settings only determine the acquisition timeout, not the polling cadence.
    public int RetryCount { get; set; } = 3;
    public int RetryDelayMs { get; set; } = 100;
    public int? AcquisitionTimeoutMs { get; set; }
    // Null preserves DistributedLock.Redis's default 10–800 ms randomized polling.
    public int? MinBusyWaitSleepTimeMs { get; set; }
    public int? MaxBusyWaitSleepTimeMs { get; set; }
    public int Database { get; set; } = 0;
    public string LockKeyPrefix { get; set; } = "lamina:lock";

    public TimeSpan GetAcquisitionTimeout() => TimeSpan.FromMilliseconds(
        AcquisitionTimeoutMs ?? (long)RetryCount * RetryDelayMs);

    public void Validate()
    {
        var errors = new List<string>();
        if (LockExpirySeconds <= 0 || (long)LockExpirySeconds * 1000 > int.MaxValue)
            errors.Add("LockExpirySeconds must be positive and represent at most Int32.MaxValue milliseconds.");
        if (RetryCount < 0 || RetryDelayMs < 0)
            errors.Add("Legacy retry settings cannot be negative.");
        var timeout = AcquisitionTimeoutMs ?? (long)RetryCount * RetryDelayMs;
        if (timeout < 0 || timeout > int.MaxValue)
            errors.Add("The acquisition timeout must be between zero and Int32.MaxValue milliseconds.");
        var min = MinBusyWaitSleepTimeMs ?? 10;
        var max = MaxBusyWaitSleepTimeMs ?? 800;
        if (min <= 0 || max < min)
            errors.Add("Busy-wait intervals must be positive and maximum must be at least minimum.");
        if (Database < 0) errors.Add("Database cannot be negative.");
        if (string.IsNullOrWhiteSpace(LockKeyPrefix)) errors.Add("LockKeyPrefix cannot be empty.");
        if (errors.Count > 0) throw new OptionsValidationException(nameof(RedisSettings), typeof(RedisSettings), errors);
    }
}
