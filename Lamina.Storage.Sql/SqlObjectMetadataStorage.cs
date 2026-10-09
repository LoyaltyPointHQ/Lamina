using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Integrity;
using Lamina.Storage.Sql.Context;
using Lamina.Storage.Sql.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Lamina.Storage.Sql;

public class SqlObjectMetadataStorage : IObjectMetadataStorage, IBatchObjectMetadataStorage, IConditionalObjectIntegrityStorage
{
    private readonly LaminaDbContext _context;

    public SqlObjectMetadataStorage(
        LaminaDbContext context,
        ILogger<SqlObjectMetadataStorage> logger)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        ArgumentNullException.ThrowIfNull(logger);
    }

    public async Task<S3Object?> StoreMetadataAsync(string bucketName, string key, string etag, long size, PutObjectRequest? request = null, Dictionary<string, string>? calculatedChecksums = null, DateTime? lastModified = null, CancellationToken cancellationToken = default)
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
            LastModified = NormalizeTimestamp(lastModified ?? DateTime.UtcNow),
            ETag = etag,
            ContentType = string.IsNullOrEmpty(request?.ContentType) ? "application/octet-stream" : request.ContentType,
            Metadata = request?.Metadata ?? new Dictionary<string, string>(),
            Tags = request?.Tags ?? new Dictionary<string, string>(),
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

        if (calculatedChecksums == null && request != null)
        {
            s3Object.ChecksumCRC32 = request.ChecksumCRC32;
            s3Object.ChecksumCRC32C = request.ChecksumCRC32C;
            s3Object.ChecksumCRC64NVME = request.ChecksumCRC64NVME;
            s3Object.ChecksumSHA1 = request.ChecksumSHA1;
            s3Object.ChecksumSHA256 = request.ChecksumSHA256;
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
            existing.Tags = s3Object.Tags;
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

    public async Task<ObjectMetadataSnapshot?> GetMetadataAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);
        ArgumentException.ThrowIfNullOrEmpty(key);
        var entity = await _context.Objects.AsNoTracking()
            .FirstOrDefaultAsync(o => o.BucketName == bucketName && o.Key == key, cancellationToken);
        return entity == null ? null : new ObjectMetadataSnapshot(entity.ToS3ObjectInfo(), entity.LastModified);
    }

    public async Task<bool> UpdateIntegrityAsync(string bucketName, string key, string etag, long size, DateTime lastModified, Dictionary<string, string> checksums, CancellationToken cancellationToken = default)
    {
        var entity = await _context.Objects.FirstOrDefaultAsync(o => o.BucketName == bucketName && o.Key == key, cancellationToken);
        if (entity == null) return false;
        entity.ETag = etag;
        entity.Size = size;
        entity.LastModified = NormalizeTimestamp(lastModified);
        entity.ChecksumCRC32 = checksums.GetValueOrDefault("CRC32");
        entity.ChecksumCRC32C = checksums.GetValueOrDefault("CRC32C");
        entity.ChecksumCRC64NVME = checksums.GetValueOrDefault("CRC64NVME");
        entity.ChecksumSHA1 = checksums.GetValueOrDefault("SHA1");
        entity.ChecksumSHA256 = checksums.GetValueOrDefault("SHA256");
        await _context.SaveChangesAsync(cancellationToken);
        return true;
    }

    private DateTime NormalizeTimestamp(DateTime value) => _context.Database.IsNpgsql() && value.Ticks % 10 != 0
        ? new DateTime(Math.Min(DateTime.MaxValue.Ticks, value.Ticks + 10 - value.Ticks % 10), value.Kind) : value;

    // Caller holds the logical object publication lock, also used by API mutations.
    public async Task<IntegrityWriteResult> TryWriteIntegrityAsync(ObjectIntegrityWrite write, CancellationToken cancellationToken = default)
    {
        var entity = await _context.Objects.FirstOrDefaultAsync(
            x => x.BucketName == write.BucketName && x.Key == write.Key, cancellationToken);
        if (entity != null) await _context.Entry(entity).ReloadAsync(cancellationToken);
        var current = entity == null ? null : ObjectIntegrityState.FromSnapshot(new ObjectMetadataSnapshot(entity.ToS3ObjectInfo(), entity.LastModified));
        if (current != write.Expected) return IntegrityWriteResult.Conflict;
        var result = entity == null ? IntegrityWriteResult.Created : IntegrityWriteResult.Updated;
        if (entity == null)
        {
            entity = new ObjectEntity { BucketName = write.BucketName, Key = write.Key, ContentType = write.ContentType };
            _context.Objects.Add(entity);
        }
        entity.ETag = write.ETag;
        entity.Size = write.Size;
        entity.LastModified = NormalizeTimestamp(write.DataLastModified);
        entity.ChecksumCRC32 = write.Checksums.GetValueOrDefault("CRC32");
        entity.ChecksumCRC32C = write.Checksums.GetValueOrDefault("CRC32C");
        entity.ChecksumCRC64NVME = write.Checksums.GetValueOrDefault("CRC64NVME");
        entity.ChecksumSHA1 = write.Checksums.GetValueOrDefault("SHA1");
        entity.ChecksumSHA256 = write.Checksums.GetValueOrDefault("SHA256");
        await _context.SaveChangesAsync(cancellationToken);
        return result;
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

    public async Task<Dictionary<string, string>?> GetObjectTagsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);
        ArgumentException.ThrowIfNullOrEmpty(key);

        var entity = await _context.Objects
            .FirstOrDefaultAsync(o => o.BucketName == bucketName && o.Key == key, cancellationToken);

        return entity?.Tags;
    }

    public async Task<bool> SetObjectTagsAsync(string bucketName, string key, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);
        ArgumentException.ThrowIfNullOrEmpty(key);

        var entity = await _context.Objects
            .FirstOrDefaultAsync(o => o.BucketName == bucketName && o.Key == key, cancellationToken);

        if (entity == null)
        {
            return false;
        }

        entity.Tags = tags;
        await _context.SaveChangesAsync(cancellationToken);
        return true;
    }

    public Task<bool> DeleteObjectTagsAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        return SetObjectTagsAsync(bucketName, key, new Dictionary<string, string>(), cancellationToken);
    }

    public async Task<Dictionary<string, ObjectMetadataSnapshot?>> GetMetadataBatchAsync(
        string bucketName,
        IEnumerable<string> keys,
        CancellationToken cancellationToken = default)
    {
        var keyList = keys.ToList();
        var entities = await _context.Objects.AsNoTracking()
            .Where(o => o.BucketName == bucketName && keyList.Contains(o.Key))
            .ToListAsync(cancellationToken);

        var byKey = entities.ToDictionary(e => e.Key, e => (ObjectMetadataSnapshot?)new ObjectMetadataSnapshot(e.ToS3ObjectInfo(), e.LastModified));
        return keyList.ToDictionary(k => k, k => byKey.GetValueOrDefault(k));
    }
}
