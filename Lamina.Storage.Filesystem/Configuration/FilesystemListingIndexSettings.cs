namespace Lamina.Storage.Filesystem.Configuration;

/// <summary>Enabled-by-default, per-instance cache of projected filesystem listing names.</summary>
public sealed class FilesystemListingIndexSettings
{
    public bool Enabled { get; set; } = true;
    public int AbsoluteExpirationSeconds { get; set; } = 300;
    public int SlidingExpirationSeconds { get; set; } = 10;
    public long SizeLimit { get; set; } = 128 * 1024 * 1024;
    public int MaxConcurrentBuilds { get; set; } = 2;
}
