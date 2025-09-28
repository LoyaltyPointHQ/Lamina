using Lamina.Core.Models;
using Microsoft.EntityFrameworkCore;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Sql.Context;
using Lamina.Storage.Sql.Entities;

namespace Lamina.Storage.Sql;

public class SqlBucketMetadataStorage : IBucketMetadataStorage
{
    private readonly LaminaDbContext _context;

    public SqlBucketMetadataStorage(LaminaDbContext context)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    public async Task<Bucket?> StoreBucketMetadataAsync(string bucketName, CreateBucketRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);
        ArgumentNullException.ThrowIfNull(request);

        var bucket = new Bucket
        {
            Name = bucketName,
            CreationDate = DateTime.UtcNow,
            Type = request.Type ?? BucketType.Directory,
            StorageClass = request.StorageClass,
            Tags = new Dictionary<string, string>(),
            OwnerId = request.OwnerId,
            OwnerDisplayName = request.OwnerDisplayName
        };

        var entity = BucketEntity.FromBucket(bucket);

        // Check if bucket already exists
        var existingBucket = await _context.Buckets
            .FirstOrDefaultAsync(b => b.Name == bucketName, cancellationToken);

        if (existingBucket != null)
        {
            return null; // Bucket already exists
        }

        try
        {
            _context.Buckets.Add(entity);
            await _context.SaveChangesAsync(cancellationToken);
            return bucket;
        }
        catch (DbUpdateException)
        {
            // Bucket already exists (race condition)
            return null;
        }
    }

    public async Task<Bucket?> GetBucketMetadataAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);

        var entity = await _context.Buckets
            .FirstOrDefaultAsync(b => b.Name == bucketName, cancellationToken);

        return entity?.ToBucket();
    }

    public async Task<List<Bucket>> GetAllBucketsMetadataAsync(CancellationToken cancellationToken = default)
    {
        var entities = await _context.Buckets
            .OrderBy(b => b.Name)
            .ToListAsync(cancellationToken);

        return entities.Select(e => e.ToBucket()).ToList();
    }

    public async Task<bool> DeleteBucketMetadataAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);

        var entity = await _context.Buckets
            .FirstOrDefaultAsync(b => b.Name == bucketName, cancellationToken);

        if (entity == null)
        {
            return false;
        }

        _context.Buckets.Remove(entity);
        await _context.SaveChangesAsync(cancellationToken);
        return true;
    }

    public async Task<Bucket?> UpdateBucketTagsAsync(string bucketName, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);
        ArgumentNullException.ThrowIfNull(tags);

        var entity = await _context.Buckets
            .FirstOrDefaultAsync(b => b.Name == bucketName, cancellationToken);

        if (entity == null)
        {
            return null;
        }

        entity.Tags = tags;
        await _context.SaveChangesAsync(cancellationToken);
        return entity.ToBucket();
    }
}