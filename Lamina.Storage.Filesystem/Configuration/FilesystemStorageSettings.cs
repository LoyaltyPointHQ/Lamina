namespace Lamina.Storage.Filesystem.Configuration;

public class FilesystemStorageSettings
{
    public string DataDirectory { get; set; } = "/data";
    public string? MetadataDirectory { get; set; } = "/metadata";
    public MetadataStorageMode MetadataMode { get; set; } = MetadataStorageMode.Inline;
    public string InlineMetadataDirectoryName { get; set; } = ".lamina-meta";
    public string TempFilePrefix { get; set; } = ".lamina-tmp-";
    public string XattrPrefix { get; set; } = "user.lamina";

    // Network filesystem configuration
    public NetworkFileSystemMode NetworkMode { get; set; } = NetworkFileSystemMode.None;
    public int RetryCount { get; set; } = 3;
    public int RetryDelayMs { get; set; } = 100;

    // Escape hatch: disables kernel-side copy (copy_file_range) during CompleteMultipartUpload and
    // forces the old userspace PipeReader path. Leave true unless a specific deployment misbehaves.
    public bool UseZeroCopyCompleteMultipart { get; set; } = true;
}

public enum MetadataStorageMode
{
    SeparateDirectory,
    Inline,
    Xattr
}

public enum NetworkFileSystemMode
{
    None,
    CIFS,
    NFS
}