using Lamina.Core.Models;

namespace Lamina.Storage.Core.Abstract;

/// Optional capability for metadata backends that can fetch metadata for multiple keys
/// in a single operation (e.g. SQL IN query). Backends that cannot batch (filesystem)
/// do not implement this interface; the facade falls back to per-key reads.
public interface IBatchObjectMetadataStorage
{
    Task<Dictionary<string, S3ObjectInfo?>> GetMetadataBatchAsync(
        string bucketName,
        IEnumerable<string> keys,
        CancellationToken cancellationToken = default);
}
