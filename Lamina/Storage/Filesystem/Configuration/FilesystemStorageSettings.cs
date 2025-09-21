namespace Lamina.Storage.Filesystem.Configuration;

public class FilesystemStorageSettings
{
    public string DataDirectory { get; set; } = "/tmp/lamina";
    public string? MetadataDirectory { get; set; }
    public MetadataStorageMode MetadataMode { get; set; } = MetadataStorageMode.Inline;
    public string InlineMetadataDirectoryName { get; set; } = ".lamina-meta";
}

public enum MetadataStorageMode
{
    SeparateDirectory,
    Inline
}