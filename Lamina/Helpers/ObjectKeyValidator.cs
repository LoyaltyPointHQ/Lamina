using Lamina.Storage.Filesystem.Configuration;

namespace Lamina.Helpers;

public static class ObjectKeyValidator
{
    public static bool IsValidObjectKey(string key, MetadataStorageMode mode, string inlineMetadataDirectoryName)
    {
        // Check if key is null or empty
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        // In inline mode, check that the key doesn't contain metadata directory patterns
        if (mode == MetadataStorageMode.Inline)
        {
            var separator = '/';  // S3 keys use forward slashes
            var metaDirPattern = $"{separator}{inlineMetadataDirectoryName}{separator}";
            var metaDirEnd = $"{separator}{inlineMetadataDirectoryName}";

            // Check if the key contains or ends with the metadata directory
            if (key.Contains(metaDirPattern) || key.EndsWith(metaDirEnd))
            {
                return false;
            }

            // Also check each segment of the path
            var segments = key.Split(separator);
            foreach (var segment in segments)
            {
                if (segment == inlineMetadataDirectoryName)
                {
                    return false;
                }
            }
        }

        return true;
    }
}