using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.Sql.Context;
using Lamina.Storage.Sql.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Lamina.Storage.Sql;

public class SqlObjectMetadataStorage : IObjectMetadataStorage
{
    private readonly LaminaDbContext _context;
    private readonly IObjectDataStorage _dataStorage;
    private readonly ILogger<SqlObjectMetadataStorage> _logger;

    public SqlObjectMetadataStorage(
        LaminaDbContext context,
        IObjectDataStorage dataStorage,
        ILogger<SqlObjectMetadataStorage> logger)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _dataStorage = dataStorage ?? throw new ArgumentNullException(nameof(dataStorage));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<S3Object?> StoreMetadataAsync(string bucketName, string key, string etag, long size, PutObjectRequest? request = null, Dictionary<string, string>? calculatedChecksums = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(etag);

        if (!IsValidObjectKey(key))
        {
            throw new ArgumentException("Invalid object key", nameof(key));
        }

        var s3Object = new S3Object
        {
            Key = key,
            BucketName = bucketName,
            Size = size,
            LastModified = DateTime.UtcNow,
            ETag = etag,
            ContentType = request?.ContentType ?? "application/octet-stream",
            Metadata = request?.Metadata ?? new Dictionary<string, string>(),
            Data = Array.Empty<byte>(), // SQL storage doesn't store data directly
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

        var entity = ObjectEntity.FromS3Object(s3Object);

        // Check if object already exists and update or insert
        var existing = await _context.Objects
            .FirstOrDefaultAsync(o => o.BucketName == bucketName && o.Key == key, cancellationToken);

        if (existing != null)
        {
            existing.Size = size;
            existing.LastModified = s3Object.LastModified;
            existing.ETag = etag;
            existing.ContentType = s3Object.ContentType;
            existing.Metadata = s3Object.Metadata;
            existing.OwnerId = s3Object.OwnerId;
            existing.OwnerDisplayName = s3Object.OwnerDisplayName;
            existing.ChecksumCRC32 = s3Object.ChecksumCRC32;
            existing.ChecksumCRC32C = s3Object.ChecksumCRC32C;
            existing.ChecksumCRC64NVME = s3Object.ChecksumCRC64NVME;
            existing.ChecksumSHA1 = s3Object.ChecksumSHA1;
            existing.ChecksumSHA256 = s3Object.ChecksumSHA256;
        }
        else
        {
            _context.Objects.Add(entity);
        }

        await _context.SaveChangesAsync(cancellationToken);
        return s3Object;
    }

    public async Task<S3ObjectInfo?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);
        ArgumentException.ThrowIfNullOrEmpty(key);

        var entity = await _context.Objects
            .FirstOrDefaultAsync(o => o.BucketName == bucketName && o.Key == key, cancellationToken);

        if (entity == null)
        {
            return null;
        }

        // Check if the data file has been modified after the metadata was stored
        var dataInfo = await _dataStorage.GetDataInfoAsync(bucketName, key, cancellationToken);
        if (dataInfo == null)
        {
            // Data doesn't exist, metadata is orphaned
            return null;
        }

        // If data is newer than metadata, recompute ETag and checksums
        if (dataInfo.Value.lastModified > entity.LastModified)
        {
            _logger.LogInformation("Detected stale metadata for {Key} in bucket {BucketName} (data mtime: {DataTime}, metadata mtime: {MetadataTime}), recomputing checksums",
                key, bucketName, dataInfo.Value.lastModified, entity.LastModified);

            var recomputed = await RecomputeStaleMetadataAsync(entity, bucketName, key, cancellationToken);
            entity.ETag = recomputed.etag;
            entity.ChecksumCRC32 = recomputed.checksums.GetValueOrDefault("CRC32");
            entity.ChecksumCRC32C = recomputed.checksums.GetValueOrDefault("CRC32C");
            entity.ChecksumCRC64NVME = recomputed.checksums.GetValueOrDefault("CRC64NVME");
            entity.ChecksumSHA1 = recomputed.checksums.GetValueOrDefault("SHA1");
            entity.ChecksumSHA256 = recomputed.checksums.GetValueOrDefault("SHA256");
        }

        return entity.ToS3ObjectInfo();
    }

    public async Task<bool> DeleteMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);
        ArgumentException.ThrowIfNullOrEmpty(key);

        var entity = await _context.Objects
            .FirstOrDefaultAsync(o => o.BucketName == bucketName && o.Key == key, cancellationToken);

        if (entity == null)
        {
            return false;
        }

        _context.Objects.Remove(entity);
        await _context.SaveChangesAsync(cancellationToken);
        return true;
    }

    public async Task<bool> MetadataExistsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);
        ArgumentException.ThrowIfNullOrEmpty(key);

        return await _context.Objects
            .AnyAsync(o => o.BucketName == bucketName && o.Key == key, cancellationToken);
    }

    public async IAsyncEnumerable<(string bucketName, string key)> ListAllMetadataKeysAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var obj in _context.Objects
            .AsNoTracking()
            .Select(o => new { o.BucketName, o.Key })
            .AsAsyncEnumerable()
            .WithCancellation(cancellationToken))
        {
            yield return (obj.BucketName, obj.Key);
        }
    }

    public bool IsValidObjectKey(string key)
    {
        if (string.IsNullOrEmpty(key))
            return false;

        if (key.Length > 1024)
            return false;

        // Keys cannot contain certain characters
        if (key.Contains('\0') || key.Contains('\r') || key.Contains('\n'))
            return false;

        // Keys cannot start with '/'
        if (key.StartsWith('/'))
            return false;

        return true;
    }

    /// <summary>
    /// Recomputes ETag for stale metadata and clears checksums.
    /// For SQL storage, we only recompute the ETag and set checksums to null,
    /// since we don't have direct file path access and checksums are expensive.
    /// </summary>
    private async Task<(string etag, Dictionary<string, string> checksums)> RecomputeStaleMetadataAsync(
        ObjectEntity entity,
        string bucketName,
        string key,
        CancellationToken cancellationToken)
    {
        // Always recompute ETag (it's relatively cheap and essential)
        var etag = await _dataStorage.ComputeETagAsync(bucketName, key, cancellationToken);
        if (etag == null)
        {
            // If we can't compute ETag, return the existing one
            _logger.LogWarning("Failed to recompute ETag for {Key} in bucket {BucketName}, using cached value", key, bucketName);
            return (entity.ETag, new Dictionary<string, string>());
        }

        // For SQL storage, we clear checksums when metadata is stale
        // Checksums are expensive to recompute and we don't have easy file path access
        // They will be null, indicating they need to be revalidated if required
        var hadChecksums = !string.IsNullOrEmpty(entity.ChecksumCRC32) ||
                          !string.IsNullOrEmpty(entity.ChecksumCRC32C) ||
                          !string.IsNullOrEmpty(entity.ChecksumCRC64NVME) ||
                          !string.IsNullOrEmpty(entity.ChecksumSHA1) ||
                          !string.IsNullOrEmpty(entity.ChecksumSHA256);

        if (hadChecksums)
        {
            _logger.LogInformation("Clearing checksums for stale metadata of {Key} in bucket {BucketName} - checksums would need to be recalculated if needed", key, bucketName);
        }

        // Return empty dictionary which will cause all checksums to be set to null
        return (etag, new Dictionary<string, string>());
    }
}