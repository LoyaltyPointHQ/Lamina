using Lamina.Storage.Filesystem.Configuration;

namespace Lamina.Storage.Filesystem;

public static class FilesystemStorageHelper
{
    public static bool IsMetadataPath(string path, MetadataStorageMode mode, string inlineMetadataDirectoryName)
    {
        if (mode != MetadataStorageMode.Inline)
        {
            return false;
        }

        // Check if the path contains the metadata directory component
        var separator = Path.DirectorySeparatorChar;
        var metaDirPattern = $"{separator}{inlineMetadataDirectoryName}{separator}";

        // Also check if the path ends with the metadata directory
        var metaDirEnd = $"{separator}{inlineMetadataDirectoryName}";

        return path.Contains(metaDirPattern) || path.EndsWith(metaDirEnd);
    }

    public static bool ShouldExcludeFromListing(string path, MetadataStorageMode mode, string inlineMetadataDirectoryName)
    {
        return IsMetadataPath(path, mode, inlineMetadataDirectoryName);
    }
}