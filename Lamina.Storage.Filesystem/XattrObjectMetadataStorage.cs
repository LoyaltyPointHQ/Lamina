using System.Runtime.CompilerServices;
using System.Text.Json;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Lamina.Storage.Filesystem;

public class XattrObjectMetadataStorage : IObjectMetadataStorage, IRequiresDataFileForMetadata
{
    private readonly string _dataDirectory;
    private readonly IBucketStorageFacade _bucketStorage;
    private readonly IObjectDataStorage _dataStorage;
    private readonly XattrHelper _xattrHelper;
    private readonly ILogger<XattrObjectMetadataStorage> _logger;

    private const string ETagAttributeName = "etag";
    private const string ContentTypeAttributeName = "content-type";
    private const string MetadataPrefix = "metadata";
    private const string TagPrefix = "tag";
    private const string OwnerIdAttributeName = "owner-id";
    private const string OwnerDisplayNameAttributeName = "owner-display-name";
    // Data mtime snapshotted when metadata was last written. A later mtime on the data means
    // the file was modified outside the API (data-first) and the ETag is no longer trustworthy.
    private const string MetadataTimestampAttributeName = "metadata-ts";

    public XattrObjectMetadataStorage(
        IOptions<FilesystemStorageSettings> settingsOptions,
        IBucketStorageFacade bucketStorage,
        IObjectDataStorage dataStorage,
        ILogger<XattrObjectMetadataStorage> logger,
        ILoggerFactory loggerFactory)
    {
        var settings = settingsOptions.Value;
        _dataDirectory = settings.DataDirectory;
        _bucketStorage = bucketStorage;
        _dataStorage = dataStorage;
        _logger = logger;

        _xattrHelper = new XattrHelper(settings.XattrPrefix, loggerFactory.CreateLogger<XattrHelper>());

        if (!_xattrHelper.IsSupported)
        {
            throw new NotSupportedException("Extended attributes are not supported on this platform. Cannot use Xattr metadata storage mode.");
        }

        Directory.CreateDirectory(_dataDirectory);
    }

    public async Task<S3Object?> StoreMetadataAsync(string bucketName, string key, string etag, long size, PutObjectRequest? request = null, Dictionary<string, string>? calculatedChecksums = null, DateTime? lastModified = null, CancellationToken cancellationToken = default)
    {
        if (!await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        var dataInfo = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
        if (dataInfo == null)
        {
            _logger.LogError("Cannot store metadata for non-existent data: {Bucket}/{Key}", bucketName, key);
            return null;
        }

        // xattr physically lives on the data file - we still need the path to set attributes.
        var dataPath = GetDataPath(bucketName, key);

        try
        {
            // Store ETag
            if (!_xattrHelper.SetAttribute(dataPath, ETagAttributeName, etag))
            {
                _logger.LogError("Failed to store ETag attribute for {Key} in bucket {BucketName}", key, bucketName);
                return null;
            }

            // Snapshot of the data mtime at write time; used later to detect external modifications.
            _xattrHelper.SetAttribute(dataPath, MetadataTimestampAttributeName,
                dataInfo.Value.lastModified.ToString("O", System.Globalization.CultureInfo.InvariantCulture));

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

            // Store tags
            if (request?.Tags != null)
            {
                StoreTags(dataPath, request.Tags, key, bucketName);
            }

            var s3Object = new S3Object
            {
                Key = key,
                BucketName = bucketName,
                Size = size,
                LastModified = lastModified ?? dataInfo.Value.lastModified,
                ETag = etag,
                ContentType = contentType,
                Metadata = request?.Metadata ?? new Dictionary<string, string>(),
                Tags = request?.Tags ?? new Dictionary<string, string>(),
                OwnerId = request?.OwnerId,
                OwnerDisplayName = request?.OwnerDisplayName
            };

            // Populate checksum fields from calculated checksums (these take precedence)
            if (calculatedChecksums != null)
            {
                if (calculatedChecksums.TryGetValue("CRC32", out var crc32))
                    s3Object.ChecksumCRC32 = crc32;
                if (calculatedChecksums.TryGetValue("CRC32C", out var crc32c))
                    s3Object.ChecksumCRC32C = crc32c;
                if (calculatedChecksums.TryGetValue("CRC64NVME", out var crc64nvme))
                    s3Object.ChecksumCRC64NVME = crc64nvme;
                if (calculatedChecksums.TryGetValue("SHA1", out var sha1))
                    s3Object.ChecksumSHA1 = sha1;
                if (calculatedChecksums.TryGetValue("SHA256", out var sha256))
                    s3Object.ChecksumSHA256 = sha256;
            }

            return s3Object;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to store metadata for {Key} in bucket {BucketName}", key, bucketName);
            return null;
        }
    }

    public async Task<S3ObjectInfo?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var dataInfo = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
        if (dataInfo == null)
        {
            return null;
        }

        var dataPath = GetDataPath(bucketName, key);

        try
        {
            var etag = _xattrHelper.GetAttribute(dataPath, ETagAttributeName);
            if (string.IsNullOrEmpty(etag))
            {
                return null;
            }

            // Staleness detection: compare the mtime snapshotted when metadata was last written
            // against the current data mtime. If data is newer, the file was modified outside the
            // API (data-first) and ETag must be recomputed.
            var recordedTsRaw = _xattrHelper.GetAttribute(dataPath, MetadataTimestampAttributeName);
            if (!string.IsNullOrEmpty(recordedTsRaw)
                && DateTime.TryParse(recordedTsRaw, System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.RoundtripKind, out var recordedTs)
                && dataInfo.Value.lastModified > recordedTs
                && !ETagHelper.IsMultipartETag(etag))
            {
                _logger.LogInformation("Detected stale xattr metadata for {Key} in bucket {BucketName} (data mtime: {DataTime}, recorded: {RecordedTime}), recomputing ETag",
                    key, bucketName, dataInfo.Value.lastModified, recordedTs);

                var recomputed = await _dataStorage.ComputeETagAsync(bucketName, key, cancellationToken);
                if (!string.IsNullOrEmpty(recomputed))
                {
                    etag = recomputed;
                    // Refresh the snapshot so subsequent reads don't re-recompute.
                    _xattrHelper.SetAttribute(dataPath, ETagAttributeName, etag);
                    _xattrHelper.SetAttribute(dataPath, MetadataTimestampAttributeName,
                        dataInfo.Value.lastModified.ToString("O", System.Globalization.CultureInfo.InvariantCulture));
                }
            }

            var contentType = _xattrHelper.GetAttribute(dataPath, ContentTypeAttributeName) ?? "application/octet-stream";
            var ownerId = _xattrHelper.GetAttribute(dataPath, OwnerIdAttributeName);
            var ownerDisplayName = _xattrHelper.GetAttribute(dataPath, OwnerDisplayNameAttributeName);
            var userMetadata = GetUserMetadata(dataPath);
            var tags = GetTags(dataPath);

            return new S3ObjectInfo
            {
                Key = key,
                LastModified = dataInfo.Value.lastModified,
                ETag = etag,
                Size = dataInfo.Value.size,
                ContentType = contentType,
                Metadata = userMetadata,
                Tags = tags,
                OwnerId = ownerId,
                OwnerDisplayName = ownerDisplayName
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get metadata for {Key} in bucket {BucketName}", key, bucketName);
            return null;
        }
    }

    public async Task<bool> DeleteMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (!await _dataStorage.DataExistsAsync(bucketName, key, cancellationToken))
        {
            return true; // Consider it success if file doesn't exist
        }

        var dataPath = GetDataPath(bucketName, key);
        try
        {
            return _xattrHelper.RemoveAllAttributes(dataPath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete metadata for {Key} in bucket {BucketName}", key, bucketName);
            return false;
        }
    }

    public async Task<bool> MetadataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (!await _dataStorage.DataExistsAsync(bucketName, key, cancellationToken))
        {
            return false;
        }

        var dataPath = GetDataPath(bucketName, key);
        try
        {
            // Check if ETag attribute exists (this indicates we have metadata)
            var etag = _xattrHelper.GetAttribute(dataPath, ETagAttributeName);
            return !string.IsNullOrEmpty(etag);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to check metadata existence for {Key} in bucket {BucketName}", key, bucketName);
            return false;
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
        foreach (var file in Directory.EnumerateFiles(currentDirectory))
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

    public async Task<Dictionary<string, string>?> GetObjectTagsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        if (!await _dataStorage.DataExistsAsync(bucketName, key, cancellationToken))
        {
            return null;
        }

        var dataPath = GetDataPath(bucketName, key);
        var etag = _xattrHelper.GetAttribute(dataPath, ETagAttributeName);
        if (string.IsNullOrEmpty(etag))
        {
            return null;
        }

        return GetTags(dataPath);
    }

    public async Task<bool> SetObjectTagsAsync(string bucketName, string key, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        if (!await _dataStorage.DataExistsAsync(bucketName, key, cancellationToken))
        {
            return false;
        }

        var dataPath = GetDataPath(bucketName, key);
        var etag = _xattrHelper.GetAttribute(dataPath, ETagAttributeName);
        if (string.IsNullOrEmpty(etag))
        {
            return false;
        }

        RemoveAllTags(dataPath);
        StoreTags(dataPath, tags, key, bucketName);
        return true;
    }

    public Task<bool> DeleteObjectTagsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        return SetObjectTagsAsync(bucketName, key, new Dictionary<string, string>(), cancellationToken);
    }

    // dataPath is still needed: xattr operations physically act on the data file. The
    // _dataDirectory dependency is therefore inherent to Xattr mode (see IRequiresDataFileForMetadata).

    private void StoreTags(string dataPath, Dictionary<string, string> tags, string key, string bucketName)
    {
        foreach (var kvp in tags)
        {
            var attributeName = $"{TagPrefix}.{kvp.Key}";
            var success = _xattrHelper.SetAttribute(dataPath, attributeName, kvp.Value);
            if (!success)
            {
                _logger.LogWarning("Failed to store tag attribute {AttributeName} for {Key} in bucket {BucketName}",
                    attributeName, key, bucketName);
            }
        }
    }

    private Dictionary<string, string> GetTags(string dataPath)
    {
        var tags = new Dictionary<string, string>();

        try
        {
            var attributes = _xattrHelper.ListAttributes(dataPath);
            var tagPrefixDot = $"{TagPrefix}.";

            foreach (var attr in attributes)
            {
                if (attr.StartsWith(tagPrefixDot))
                {
                    var tagKey = attr[tagPrefixDot.Length..];
                    var value = _xattrHelper.GetAttribute(dataPath, attr);
                    if (value != null)
                    {
                        tags[tagKey] = value;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to retrieve tags from {DataPath}", dataPath);
        }

        return tags;
    }

    private void RemoveAllTags(string dataPath)
    {
        try
        {
            var attributes = _xattrHelper.ListAttributes(dataPath);
            var tagPrefixDot = $"{TagPrefix}.";

            foreach (var attr in attributes)
            {
                if (attr.StartsWith(tagPrefixDot))
                {
                    _xattrHelper.RemoveAttribute(dataPath, attr);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to remove tags from {DataPath}", dataPath);
        }
    }
}