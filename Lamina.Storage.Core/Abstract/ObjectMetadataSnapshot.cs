using Lamina.Core.Models;

namespace Lamina.Storage.Core.Abstract;

/// <summary>Persisted metadata and the data modification time it describes; no data-side refresh.</summary>
public sealed record ObjectMetadataSnapshot(S3ObjectInfo Metadata, DateTime? DataLastModified)
{
    public ObjectMetadataSnapshot DetachedCopy() => new(CloneMetadata(Metadata), DataLastModified);

    public static S3ObjectInfo CloneMetadata(S3ObjectInfo value) => new()
    {
        Key = value.Key,
        Size = value.Size,
        LastModified = value.LastModified,
        ETag = value.ETag,
        ContentType = value.ContentType,
        Metadata = new(value.Metadata),
        Tags = new(value.Tags),
        OwnerId = value.OwnerId,
        OwnerDisplayName = value.OwnerDisplayName,
        ChecksumCRC32 = value.ChecksumCRC32,
        ChecksumCRC32C = value.ChecksumCRC32C,
        ChecksumCRC64NVME = value.ChecksumCRC64NVME,
        ChecksumSHA1 = value.ChecksumSHA1,
        ChecksumSHA256 = value.ChecksumSHA256
    };
}
