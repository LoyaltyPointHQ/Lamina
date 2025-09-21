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

        // Normalize path to use forward slashes for consistent checking
        var normalizedPath = path.Replace('\\', '/');

        // Check if the path contains or starts with the metadata directory component
        var metaDirPattern = $"/{inlineMetadataDirectoryName}/";
        var metaDirStart = $"{inlineMetadataDirectoryName}/";
        var metaDirEnd = $"/{inlineMetadataDirectoryName}";

        return normalizedPath.Contains(metaDirPattern) ||
               normalizedPath.StartsWith(metaDirStart) ||
               normalizedPath.EndsWith(metaDirEnd) ||
               normalizedPath == inlineMetadataDirectoryName;
    }

    public static bool ShouldExcludeFromListing(string path, MetadataStorageMode mode, string inlineMetadataDirectoryName)
    {
        return IsMetadataPath(path, mode, inlineMetadataDirectoryName);
    }
}