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

    public static bool IsTemporaryFile(string fileName, string tempFilePrefix)
    {
        if (string.IsNullOrEmpty(fileName) || string.IsNullOrEmpty(tempFilePrefix))
        {
            return false;
        }

        return fileName.StartsWith(tempFilePrefix, StringComparison.OrdinalIgnoreCase);
    }

    public static bool IsKeyForbidden(string key, string tempFilePrefix, MetadataStorageMode mode, string inlineMetadataDirectoryName)
    {
        if (string.IsNullOrEmpty(key))
        {
            return false;
        }

        // Check if the key would conflict with temporary files
        var keyParts = key.Split('/');
        foreach (var part in keyParts)
        {
            if (IsTemporaryFile(part, tempFilePrefix))
            {
                return true;
            }
        }

        // Also check existing metadata path restrictions
        return IsMetadataPath(key, mode, inlineMetadataDirectoryName);
    }
}