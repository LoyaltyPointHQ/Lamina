namespace Lamina.Storage.Filesystem.Configuration;

public class FilesystemStorageSettings
{
    public string DataDirectory { get; set; } = "/data";
    public string? MetadataDirectory { get; set; } = "/metadata";
    public MetadataStorageMode MetadataMode { get; set; } = MetadataStorageMode.Inline;
    public string InlineMetadataDirectoryName { get; set; } = ".lamina-meta";

    // Network filesystem configuration
    public NetworkFileSystemMode NetworkMode { get; set; } = NetworkFileSystemMode.None;
    public int RetryCount { get; set; } = 3;
    public int RetryDelayMs { get; set; } = 100;
}

public enum MetadataStorageMode
{
    SeparateDirectory,
    Inline
}

public enum NetworkFileSystemMode
{
    None,
    CIFS,
    NFS
}