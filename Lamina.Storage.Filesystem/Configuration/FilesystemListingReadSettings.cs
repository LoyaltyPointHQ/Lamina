namespace Lamina.Storage.Filesystem.Configuration;

public sealed class FilesystemListingReadSettings
{
    public int MaxConcurrency { get; set; } = 8;

    public void Validate() => ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxConcurrency);
}
