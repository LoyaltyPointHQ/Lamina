namespace Lamina.Configuration;

public class RedisSettings
{
    public string ConnectionString { get; set; } = "localhost:6379";
    public int LockExpirySeconds { get; set; } = 30;
    public int RetryCount { get; set; } = 3;
    public int RetryDelayMs { get; set; } = 100;
    public int Database { get; set; } = 0;
    public string LockKeyPrefix { get; set; } = "lamina:lock";
}