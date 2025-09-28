using Lamina.Core.Models;
using Microsoft.EntityFrameworkCore;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Sql.Context;
using Lamina.Storage.Sql.Entities;

namespace Lamina.Storage.Sql;

public class SqlObjectMetadataStorage : IObjectMetadataStorage
{
    private readonly LaminaDbContext _context;

    public SqlObjectMetadataStorage(LaminaDbContext context)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    public async Task<S3Object?> StoreMetadataAsync(string bucketName, string key, string etag, long size, PutObjectRequest? request = null, CancellationToken cancellationToken = default)
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

        return entity?.ToS3ObjectInfo();
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
}