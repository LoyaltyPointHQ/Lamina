using Lamina.Core.Models;

namespace Lamina.Storage.Core.Abstract;

/// <summary>
/// Optional capability marker for multipart data storage backends that expose part files directly
/// on a local/network filesystem. When both the multipart data storage and the object data storage
/// implement their respective file-backed interfaces, Complete can use kernel-side copy
/// (copy_file_range / server-side SMB copy / NFSv4.2 COPY) instead of streaming bytes through
/// userspace. Non-file-backed backends (InMemory, Sql-only, etc.) simply don't implement this.
/// </summary>
public interface IFileBackedMultipartPartSource
{
    /// <summary>
    /// Resolves absolute filesystem paths for the requested parts in part-number order.
    /// Returns <c>false</c> if any part is missing, so the caller can fall back to streaming.
    /// </summary>
    bool TryGetPartFilePaths(
        string bucketName,
        string key,
        string uploadId,
        IReadOnlyList<CompletedPart> parts,
        out IReadOnlyList<string> paths);
}
