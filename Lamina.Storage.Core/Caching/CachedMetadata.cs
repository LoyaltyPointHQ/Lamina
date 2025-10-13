using Lamina.Core.Models;

namespace Lamina.Storage.Core.Caching;

/// <summary>
/// Represents cached metadata with staleness tracking.
/// </summary>
internal class CachedMetadata
{
    public required S3ObjectInfo Metadata { get; init; }

    /// <summary>
    /// Last modified time of the data at the time this metadata was cached.
    /// Used for staleness detection.
    /// </summary>
    public required DateTime DataLastModified { get; init; }

    /// <summary>
    /// Estimates the size of this cache entry in bytes.
    /// Used by MemoryCache for size-based eviction.
    /// </summary>
    public long EstimateSize()
    {
        long size = 200; // Base overhead for object structure, DateTimes, and primitive fields

        // String fields (UTF-16, so 2 bytes per character)
        size += (Metadata.Key?.Length ?? 0) * 2;
        size += (Metadata.ETag?.Length ?? 0) * 2;
        size += (Metadata.ContentType?.Length ?? 0) * 2;
        size += (Metadata.OwnerId?.Length ?? 0) * 2;
        size += (Metadata.OwnerDisplayName?.Length ?? 0) * 2;

        // Metadata dictionary
        if (Metadata.Metadata != null)
        {
            foreach (var kvp in Metadata.Metadata)
            {
                size += (kvp.Key.Length + kvp.Value.Length) * 2;
                size += 32; // Dictionary entry overhead
            }
        }

        // Checksum strings (each typically 20-64 characters)
        size += (Metadata.ChecksumCRC32?.Length ?? 0) * 2;
        size += (Metadata.ChecksumCRC32C?.Length ?? 0) * 2;
        size += (Metadata.ChecksumCRC64NVME?.Length ?? 0) * 2;
        size += (Metadata.ChecksumSHA1?.Length ?? 0) * 2;
        size += (Metadata.ChecksumSHA256?.Length ?? 0) * 2;

        return size;
    }
}
