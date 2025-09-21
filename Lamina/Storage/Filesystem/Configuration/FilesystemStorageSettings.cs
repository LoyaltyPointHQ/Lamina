namespace Lamina.Storage.Filesystem.Configuration;

public class FilesystemStorageSettings
{
    public string DataDirectory { get; set; } = "/tmp/lamina/data";
    public string? MetadataDirectory { get; set; }
    public MetadataStorageMode MetadataMode { get; set; } = MetadataStorageMode.SeparateDirectory;
    public string InlineMetadataDirectoryName { get; set; } = ".lamina-meta";
}

public enum MetadataStorageMode
{
    SeparateDirectory,
    Inline
}