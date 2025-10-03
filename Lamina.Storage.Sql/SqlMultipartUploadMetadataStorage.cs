using Lamina.Core.Models;
using Microsoft.EntityFrameworkCore;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Sql.Context;
using Lamina.Storage.Sql.Entities;

namespace Lamina.Storage.Sql;

public class SqlMultipartUploadMetadataStorage : IMultipartUploadMetadataStorage
{
    private readonly LaminaDbContext _context;

    public SqlMultipartUploadMetadataStorage(LaminaDbContext context)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    public async Task<MultipartUpload> InitiateUploadAsync(string bucketName, string key, InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(request);

        var uploadId = Guid.NewGuid().ToString();
        var upload = new MultipartUpload
        {
            UploadId = uploadId,
            BucketName = bucketName,
            Key = key,
            Initiated = DateTime.UtcNow,
            ContentType = request.ContentType,
            Metadata = request.Metadata ?? new Dictionary<string, string>()
        };

        var entity = MultipartUploadEntity.FromMultipartUpload(upload);
        _context.MultipartUploads.Add(entity);
        await _context.SaveChangesAsync(cancellationToken);

        return upload;
    }

    public async Task<MultipartUpload?> GetUploadMetadataAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(uploadId);

        var entity = await _context.MultipartUploads
            .FirstOrDefaultAsync(u => u.BucketName == bucketName && u.Key == key && u.UploadId == uploadId, cancellationToken);

        return entity?.ToMultipartUpload();
    }

    public async Task<bool> DeleteUploadMetadataAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(uploadId);

        var entity = await _context.MultipartUploads
            .FirstOrDefaultAsync(u => u.BucketName == bucketName && u.Key == key && u.UploadId == uploadId, cancellationToken);

        if (entity == null)
        {
            return false;
        }

        _context.MultipartUploads.Remove(entity);
        await _context.SaveChangesAsync(cancellationToken);
        return true;
    }

    public async Task<List<MultipartUpload>> ListUploadsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(bucketName);

        var entities = await _context.MultipartUploads
            .Where(u => u.BucketName == bucketName)
            .OrderBy(u => u.Initiated)
            .ToListAsync(cancellationToken);

        return entities.Select(e => e.ToMultipartUpload()).ToList();
    }
}