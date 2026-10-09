using Lamina.Storage.Core.Abstract;

namespace Lamina.Storage.Core.Integrity;

public sealed record ObjectIntegrityState(string ETag, long Size, DateTime? DataLastModified,
    string? ChecksumCRC32, string? ChecksumCRC32C, string? ChecksumCRC64NVME, string? ChecksumSHA1, string? ChecksumSHA256)
{
    public static ObjectIntegrityState FromSnapshot(ObjectMetadataSnapshot snapshot) => new(snapshot.Metadata.ETag,
        snapshot.Metadata.Size, snapshot.DataLastModified, snapshot.Metadata.ChecksumCRC32, snapshot.Metadata.ChecksumCRC32C,
        snapshot.Metadata.ChecksumCRC64NVME, snapshot.Metadata.ChecksumSHA1, snapshot.Metadata.ChecksumSHA256);
}

public sealed record ObjectIntegrityWrite(string BucketName, string Key, long Size, DateTime DataLastModified,
    ObjectIntegrityState? Expected, string ETag, IReadOnlyDictionary<string, string> Checksums, string ContentType);

public enum IntegrityWriteResult { Created, Updated, Conflict }

public interface IConditionalObjectIntegrityStorage
{
    /// <summary>
    /// Caller must hold the logical object publication lock and validate the current data size/mtime.
    /// Creates only while metadata remain absent, or replaces matching integrity fields while preserving user fields.
    /// </summary>
    Task<IntegrityWriteResult> TryWriteIntegrityAsync(ObjectIntegrityWrite write, CancellationToken cancellationToken = default);
}

public interface IObjectPublicationLock
{
    ValueTask<IAsyncDisposable> AcquireAsync(string bucketName, string key, CancellationToken cancellationToken = default);
}

public sealed class IntegrityPersistenceSettings
{
    public bool Enabled { get; set; }
    public int Capacity { get; set; } = 4096;
    public int Workers { get; set; } = 2;
}
