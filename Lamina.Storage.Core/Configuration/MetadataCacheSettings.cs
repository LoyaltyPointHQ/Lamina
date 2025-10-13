namespace Lamina.Storage.Core.Configuration;

public class MetadataCacheSettings
{
    /// <summary>
    /// Whether metadata caching is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Maximum size limit in bytes for the cache.
    /// Default is 100MB (104857600 bytes).
    /// </summary>
    public long SizeLimit { get; set; } = 104_857_600;

    /// <summary>
    /// Absolute expiration time in minutes.
    /// Cache entries will be removed after this time regardless of access.
    /// </summary>
    public int? AbsoluteExpirationMinutes { get; set; } = 60;

    /// <summary>
    /// Sliding expiration time in minutes.
    /// Cache entries will be removed if not accessed within this time.
    /// </summary>
    public int? SlidingExpirationMinutes { get; set; } = 15;
}
