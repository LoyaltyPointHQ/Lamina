using System.Runtime.CompilerServices;
using System.Text.Json;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

public class XattrObjectMetadataStorage : IObjectMetadataStorage
{
    private readonly string _dataDirectory;
    private readonly IBucketStorageFacade _bucketStorage;
    private readonly XattrHelper _xattrHelper;
    private readonly ILogger<XattrObjectMetadataStorage> _logger;

    private const string ETagAttributeName = "etag";
    private const string ContentTypeAttributeName = "content-type";
    private const string MetadataPrefix = "metadata";
    private const string OwnerIdAttributeName = "owner-id";
    private const string OwnerDisplayNameAttributeName = "owner-display-name";

    public XattrObjectMetadataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        IBucketStorageFacade bucketStorage,
        ILogger<XattrObjectMetadataStorage> logger,
        ILoggerFactory loggerFactory)
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _bucketStorage = bucketStorage;
        _logger = logger;

        _xattrHelper = new XattrHelper(settings.XattrPrefix, loggerFactory.CreateLogger<XattrHelper>());

        if (!_xattrHelper.IsSupported)
        {
            throw new NotSupportedException("Extended attributes are not supported on this platform. Cannot use Xattr metadata storage mode.");
        }

        Directory.CreateDirectory(_dataDirectory);
    }

    public async Task<S3Object?> StoreMetadataAsync(string bucketName, string key, string etag, long size, PutObjectRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (!await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            _logger.LogError("Cannot store metadata for non-existent data file: {DataPath}", dataPath);
            return null;
        }

        try
        {
            // Store ETag
            if (!_xattrHelper.SetAttribute(dataPath, ETagAttributeName, etag))
            {
                _logger.LogError("Failed to store ETag attribute for {Key} in bucket {BucketName}", key, bucketName);
                return null;
            }

            // Store Content-Type if provided
            var contentType = request?.ContentType ?? "application/octet-stream";
            if (!_xattrHelper.SetAttribute(dataPath, ContentTypeAttributeName, contentType))
            {
                _logger.LogWarning("Failed to store Content-Type attribute for {Key} in bucket {BucketName}", key, bucketName);
            }

            // Store owner information
            if (!string.IsNullOrEmpty(request?.OwnerId))
            {
                _xattrHelper.SetAttribute(dataPath, OwnerIdAttributeName, request.OwnerId);
            }
            if (!string.IsNullOrEmpty(request?.OwnerDisplayName))
            {
                _xattrHelper.SetAttribute(dataPath, OwnerDisplayNameAttributeName, request.OwnerDisplayName);
            }

            // Store user metadata
            if (request?.Metadata != null)
            {
                StoreUserMetadata(dataPath, request.Metadata, key, bucketName);
            }

            // Get the actual last modified time from the filesystem
            var fileInfo = new FileInfo(dataPath);

            return new S3Object
            {
                Key = key,
                BucketName = bucketName,
                Size = size,
                LastModified = fileInfo.LastWriteTimeUtc,
                ETag = etag,
                ContentType = contentType,
                Metadata = request?.Metadata ?? new Dictionary<string, string>(),
                OwnerId = request?.OwnerId,
                OwnerDisplayName = request?.OwnerDisplayName
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to store metadata for {Key} in bucket {BucketName}", key, bucketName);
            return null;
        }
    }

    public Task<S3ObjectInfo?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return Task.FromResult<S3ObjectInfo?>(null);
        }

        try
        {
            // Get ETag from xattr
            var etag = _xattrHelper.GetAttribute(dataPath, ETagAttributeName);
            if (string.IsNullOrEmpty(etag))
            {
                // No metadata stored in xattr
                return Task.FromResult<S3ObjectInfo?>(null);
            }

            // Get Content-Type from xattr
            var contentType = _xattrHelper.GetAttribute(dataPath, ContentTypeAttributeName) ?? "application/octet-stream";

            // Get owner information from xattr
            var ownerId = _xattrHelper.GetAttribute(dataPath, OwnerIdAttributeName);
            var ownerDisplayName = _xattrHelper.GetAttribute(dataPath, OwnerDisplayNameAttributeName);

            // Get user metadata
            var userMetadata = GetUserMetadata(dataPath);

            // Always get size and last modified from filesystem
            var fileInfo = new FileInfo(dataPath);

            return Task.FromResult<S3ObjectInfo?>(new S3ObjectInfo
            {
                Key = key,
                LastModified = fileInfo.LastWriteTimeUtc,
                ETag = etag,
                Size = fileInfo.Length,
                ContentType = contentType,
                Metadata = userMetadata,
                OwnerId = ownerId,
                OwnerDisplayName = ownerDisplayName
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get metadata for {Key} in bucket {BucketName}", key, bucketName);
            return Task.FromResult<S3ObjectInfo?>(null);
        }
    }

    public Task<bool> DeleteMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return Task.FromResult(true); // Consider it success if file doesn't exist
        }

        try
        {
            return Task.FromResult(_xattrHelper.RemoveAllAttributes(dataPath));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete metadata for {Key} in bucket {BucketName}", key, bucketName);
            return Task.FromResult(false);
        }
    }

    public Task<bool> MetadataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataPath = GetDataPath(bucketName, key);
        if (!File.Exists(dataPath))
        {
            return Task.FromResult(false);
        }

        try
        {
            // Check if ETag attribute exists (this indicates we have metadata)
            var etag = _xattrHelper.GetAttribute(dataPath, ETagAttributeName);
            return Task.FromResult(!string.IsNullOrEmpty(etag));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to check metadata existence for {Key} in bucket {BucketName}", key, bucketName);
            return Task.FromResult(false);
        }
    }

    public async IAsyncEnumerable<(string bucketName, string key)> ListAllMetadataKeysAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(_dataDirectory))
        {
            yield break;
        }

        var bucketDirectories = Directory.GetDirectories(_dataDirectory);

        foreach (var bucketDir in bucketDirectories)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var bucketName = Path.GetFileName(bucketDir);

            await foreach (var key in EnumerateKeysWithMetadataAsync(bucketDir, "", cancellationToken))
            {
                yield return (bucketName, key);
            }
        }
    }

    private async IAsyncEnumerable<string> EnumerateKeysWithMetadataAsync(string currentDirectory, string keyPrefix, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Check files in current directory
        var files = Directory.GetFiles(currentDirectory);
        foreach (var file in files)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var fileName = Path.GetFileName(file);
            var key = string.IsNullOrEmpty(keyPrefix) ? fileName : $"{keyPrefix}/{fileName}";

            // Check if this file has metadata in xattr
            var etag = _xattrHelper.GetAttribute(file, ETagAttributeName);
            if (!string.IsNullOrEmpty(etag))
            {
                yield return key;
            }
        }

        // Recursively check subdirectories
        var subdirectories = Directory.GetDirectories(currentDirectory);
        foreach (var subdir in subdirectories)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var dirName = Path.GetFileName(subdir);
            var newPrefix = string.IsNullOrEmpty(keyPrefix) ? dirName : $"{keyPrefix}/{dirName}";

            await foreach (var key in EnumerateKeysWithMetadataAsync(subdir, newPrefix, cancellationToken))
            {
                yield return key;
            }
        }

        await Task.CompletedTask; // Satisfy async requirement
    }

    public bool IsValidObjectKey(string key)
    {
        // For xattr mode, we don't have any specific key restrictions
        // Unlike inline mode, we don't need to worry about metadata directory conflicts
        return !string.IsNullOrWhiteSpace(key);
    }

    private void StoreUserMetadata(string dataPath, Dictionary<string, string> userMetadata, string key, string bucketName)
    {
        foreach (var kvp in userMetadata)
        {
            var attributeName = $"{MetadataPrefix}.{kvp.Key}";
            var success = _xattrHelper.SetAttribute(dataPath, attributeName, kvp.Value);
            if (!success)
            {
                _logger.LogWarning("Failed to store user metadata attribute {AttributeName} for {Key} in bucket {BucketName}",
                    attributeName, key, bucketName);
            }
        }
    }

    private Dictionary<string, string> GetUserMetadata(string dataPath)
    {
        var userMetadata = new Dictionary<string, string>();

        try
        {
            var attributes = _xattrHelper.ListAttributes(dataPath);
            var metadataPrefixDot = $"{MetadataPrefix}.";

            foreach (var attr in attributes)
            {
                if (attr.StartsWith(metadataPrefixDot))
                {
                    var metadataKey = attr[metadataPrefixDot.Length..];
                    var value = _xattrHelper.GetAttribute(dataPath, attr);
                    if (value != null)
                    {
                        userMetadata[metadataKey] = value;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to retrieve user metadata from {DataPath}", dataPath);
        }

        return userMetadata;
    }

    private string GetDataPath(string bucketName, string key)
    {
        return Path.Combine(_dataDirectory, bucketName, key);
    }
}