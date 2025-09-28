namespace Lamina.WebApi.Services;

public static class ConfigurationValidator
{
    public static void ValidateConfiguration(IConfiguration configuration)
    {
        var storageType = configuration["StorageType"] ?? "InMemory";

        if (storageType.Equals("Filesystem", StringComparison.OrdinalIgnoreCase))
        {
            var dataDirectory = configuration["FilesystemStorage:DataDirectory"];
            var metadataDirectory = configuration["FilesystemStorage:MetadataDirectory"];
            var metadataMode = configuration["FilesystemStorage:MetadataMode"] ?? "SeparateDirectory";
            var inlineMetadataDirectoryName = configuration["FilesystemStorage:InlineMetadataDirectoryName"] ?? ".lamina-meta";

            if (string.IsNullOrWhiteSpace(dataDirectory))
            {
                throw new InvalidOperationException(
                    "Configuration error: FilesystemStorage:DataDirectory is required when StorageType is 'Filesystem'. " +
                    "Please configure it in appsettings.json or through environment variables.");
            }

            // Only require MetadataDirectory in SeparateDirectory mode
            if (metadataMode.Equals("SeparateDirectory", StringComparison.OrdinalIgnoreCase))
            {
                if (string.IsNullOrWhiteSpace(metadataDirectory))
                {
                    throw new InvalidOperationException(
                        "Configuration error: FilesystemStorage:MetadataDirectory is required when using SeparateDirectory metadata mode. " +
                        "Please configure it in appsettings.json or through environment variables.");
                }

                // Validate paths are not the same
                if (dataDirectory.Equals(metadataDirectory, StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(
                        "Configuration error: FilesystemStorage:DataDirectory and FilesystemStorage:MetadataDirectory must be different paths.");
                }

                // Log the configuration
                Console.WriteLine($"Filesystem Storage Configuration:");
                Console.WriteLine($"  Mode: SeparateDirectory");
                Console.WriteLine($"  Data Directory: {dataDirectory}");
                Console.WriteLine($"  Metadata Directory: {metadataDirectory}");
            }
            else if (metadataMode.Equals("Inline", StringComparison.OrdinalIgnoreCase))
            {
                // Log the configuration for inline mode
                Console.WriteLine($"Filesystem Storage Configuration:");
                Console.WriteLine($"  Mode: Inline");
                Console.WriteLine($"  Data Directory: {dataDirectory}");
                Console.WriteLine($"  Inline Metadata Directory Name: {inlineMetadataDirectoryName}");
            }
            else if (metadataMode.Equals("Xattr", StringComparison.OrdinalIgnoreCase))
            {
                var xattrPrefix = configuration["FilesystemStorage:XattrPrefix"] ?? "user.lamina";

                // Check platform compatibility
                var isLinuxOrMacOS = OperatingSystem.IsLinux() || OperatingSystem.IsMacOS();
                if (!isLinuxOrMacOS)
                {
                    throw new InvalidOperationException(
                        "Configuration error: Xattr metadata mode is only supported on Linux and macOS platforms. " +
                        "Windows does not support POSIX extended attributes.");
                }

                // Log the configuration for xattr mode
                Console.WriteLine($"Filesystem Storage Configuration:");
                Console.WriteLine($"  Mode: Xattr (POSIX Extended Attributes)");
                Console.WriteLine($"  Data Directory: {dataDirectory}");
                Console.WriteLine($"  Xattr Prefix: {xattrPrefix}");
                Console.WriteLine($"  Platform: {Environment.OSVersion.Platform}");
            }
            else
            {
                throw new InvalidOperationException(
                    $"Configuration error: Invalid MetadataMode '{metadataMode}'. Valid values are 'SeparateDirectory', 'Inline', or 'Xattr'.");
            }
        }
        else if (!storageType.Equals("InMemory", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Configuration error: Invalid StorageType '{storageType}'. Valid values are 'InMemory' or 'Filesystem'.");
        }

        // Validate authentication configuration if enabled
        var authEnabled = configuration.GetValue<bool>("Authentication:Enabled", false);
        if (authEnabled)
        {
            var users = configuration.GetSection("Authentication:Users").GetChildren().ToList();
            if (!users.Any())
            {
                throw new InvalidOperationException(
                    "Configuration error: Authentication is enabled but no users are configured. " +
                    "Please configure users in Authentication:Users section.");
            }

            foreach (var user in users)
            {
                var accessKeyId = user["AccessKeyId"];
                var secretAccessKey = user["SecretAccessKey"];

                if (string.IsNullOrWhiteSpace(accessKeyId))
                {
                    throw new InvalidOperationException(
                        "Configuration error: User configured without AccessKeyId in Authentication:Users.");
                }

                if (string.IsNullOrWhiteSpace(secretAccessKey))
                {
                    throw new InvalidOperationException(
                        $"Configuration error: User '{accessKeyId}' configured without SecretAccessKey in Authentication:Users.");
                }
            }

            Console.WriteLine($"Authentication enabled with {users.Count} user(s) configured");
        }

        // Validate multipart cleanup configuration
        var cleanupEnabled = configuration.GetValue<bool>("MultipartUploadCleanup:Enabled", true);
        if (cleanupEnabled)
        {
            var cleanupInterval = configuration.GetValue<int>("MultipartUploadCleanup:CleanupIntervalMinutes", 60);
            var uploadTimeout = configuration.GetValue<int>("MultipartUploadCleanup:UploadTimeoutHours", 24);

            if (cleanupInterval <= 0)
            {
                throw new InvalidOperationException(
                    "Configuration error: MultipartUploadCleanup:CleanupIntervalMinutes must be greater than 0.");
            }

            if (uploadTimeout <= 0)
            {
                throw new InvalidOperationException(
                    "Configuration error: MultipartUploadCleanup:UploadTimeoutHours must be greater than 0.");
            }

            Console.WriteLine($"Multipart Cleanup Configuration:");
            Console.WriteLine($"  Cleanup Interval: {cleanupInterval} minutes");
            Console.WriteLine($"  Upload Timeout: {uploadTimeout} hours");
        }
    }
}