using System.Runtime.CompilerServices;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

/// <summary>
/// Filesystem metadata storage where JSON files live alongside the data files in a
/// <c>.lamina-meta/</c> sibling directory. The <c>_dataDirectory</c> field is only used as a
/// root for placing metadata JSON (this mode inherently fixes the metadata layout to a
/// filesystem path); all actual reads of object data still go through <see cref="IObjectDataStorage"/>.
/// </summary>
public class InlineObjectMetadataStorage : FilesystemJsonObjectMetadataStorageBase
{
    private readonly string _dataDirectory;
    private readonly string _inlineMetadataDirectoryName;

    public InlineObjectMetadataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        IOptions<MetadataCacheSettings> cacheSettingsOptions,
        IBucketStorageFacade bucketStorage,
        IObjectDataStorage dataStorage,
        IFileSystemLockManager lockManager,
        NetworkFileSystemHelper networkHelper,
        ILogger<InlineObjectMetadataStorage> logger,
        IMemoryCache? cache = null)
        : base(bucketStorage, dataStorage, lockManager, networkHelper, cacheSettingsOptions.Value, logger, cache)
    {
        var settings = settingsOptions.Value;

        _dataDirectory = settings.DataDirectory;
        _inlineMetadataDirectoryName = settings.InlineMetadataDirectoryName;
        _networkHelper.EnsureDirectoryExists(_dataDirectory);
    }

    protected override string GetMetadataPath(string bucketName, string key)
    {
        // /bucket/path/to/object.zip -> /bucket/path/to/.lamina-meta/object.zip.json
        var dataPath = Path.Combine(_dataDirectory, bucketName, key);
        var directory = Path.GetDirectoryName(dataPath) ?? Path.Combine(_dataDirectory, bucketName);
        var fileName = Path.GetFileName(dataPath);
        return Path.Combine(directory, _inlineMetadataDirectoryName, $"{fileName}.json");
    }

    protected override string GetStorageRootDirectory() => _dataDirectory;

    protected override string GetBucketDirectory(string bucketName) => Path.Combine(_dataDirectory, bucketName);

    public override bool IsValidObjectKey(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        const char separator = '/'; // S3 keys use forward slashes
        var metaDirPattern = $"{separator}{_inlineMetadataDirectoryName}{separator}";
        var metaDirEnd = $"{separator}{_inlineMetadataDirectoryName}";

        if (key.Contains(metaDirPattern) || key.EndsWith(metaDirEnd))
        {
            return false;
        }

        foreach (var segment in key.Split(separator))
        {
            if (segment == _inlineMetadataDirectoryName)
            {
                return false;
            }
        }

        return true;
    }

    protected override IAsyncEnumerable<string> EnumerateKeysForBucketAsync(
        string bucketDirectory,
        CancellationToken cancellationToken)
    {
        return EnumerateInlineMetadataFilesAsync(bucketDirectory, keyPrefix: "", cancellationToken);
    }

    private async IAsyncEnumerable<string> EnumerateInlineMetadataFilesAsync(
        string currentDirectory,
        string keyPrefix,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var metadataDir = Path.Combine(currentDirectory, _inlineMetadataDirectoryName);
        if (Directory.Exists(metadataDir))
        {
            foreach (var file in Directory.EnumerateFiles(metadataDir, "*.json"))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var fileName = Path.GetFileName(file);
                var objectName = fileName.EndsWith(".json") ? fileName[..^5] : fileName;
                var key = string.IsNullOrEmpty(keyPrefix) ? objectName : $"{keyPrefix}/{objectName}";

                yield return key;
            }
        }

        foreach (var subdir in Directory.GetDirectories(currentDirectory))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var dirName = Path.GetFileName(subdir);
            if (dirName == _inlineMetadataDirectoryName)
            {
                continue;
            }

            var newPrefix = string.IsNullOrEmpty(keyPrefix) ? dirName : $"{keyPrefix}/{dirName}";
            await foreach (var key in EnumerateInlineMetadataFilesAsync(subdir, newPrefix, cancellationToken))
            {
                yield return key;
            }
        }

        await Task.CompletedTask;
    }
}
