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
/// Filesystem metadata storage where JSON files live in a dedicated metadata directory,
/// completely separate from the object data. This implementation does not touch the data
/// backend's storage layout - all interaction with object data goes through
/// <see cref="IObjectDataStorage"/>, so it works with any data backend (filesystem, in-memory, …).
/// </summary>
public class SeparateDirectoryObjectMetadataStorage : FilesystemJsonObjectMetadataStorageBase
{
    private readonly string _metadataDirectory;

    public SeparateDirectoryObjectMetadataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        IOptions<MetadataCacheSettings> cacheSettingsOptions,
        IBucketStorageFacade bucketStorage,
        IObjectDataStorage dataStorage,
        IFileSystemLockManager lockManager,
        NetworkFileSystemHelper networkHelper,
        ILogger<SeparateDirectoryObjectMetadataStorage> logger,
        IMemoryCache? cache = null)
        : base(bucketStorage, dataStorage, lockManager, networkHelper, cacheSettingsOptions.Value, logger, cache)
    {
        var settings = settingsOptions.Value;

        if (string.IsNullOrWhiteSpace(settings.MetadataDirectory))
        {
            throw new InvalidOperationException(
                $"{nameof(SeparateDirectoryObjectMetadataStorage)} requires FilesystemStorage:MetadataDirectory to be configured");
        }

        _metadataDirectory = settings.MetadataDirectory;
        _networkHelper.EnsureDirectoryExists(_metadataDirectory);
    }

    protected override string GetMetadataPath(string bucketName, string key)
    {
        return Path.Combine(_metadataDirectory, bucketName, $"{key}.json");
    }

    protected override string GetStorageRootDirectory() => _metadataDirectory;

    protected override string GetBucketDirectory(string bucketName) => Path.Combine(_metadataDirectory, bucketName);

    public override bool IsValidObjectKey(string key) => !string.IsNullOrWhiteSpace(key);

    protected override async IAsyncEnumerable<string> EnumerateKeysForBucketAsync(
        string bucketDirectory,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var file in Directory.EnumerateFiles(bucketDirectory, "*.json", SearchOption.AllDirectories))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var relativePath = Path.GetRelativePath(bucketDirectory, file);
            var key = relativePath.EndsWith(".json") ? relativePath[..^5] : relativePath;
            key = key.Replace(Path.DirectorySeparatorChar, '/');

            yield return key;
        }

        await Task.CompletedTask;
    }
}
