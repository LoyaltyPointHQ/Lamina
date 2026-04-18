namespace Lamina.Storage.Core.Abstract;

/// <summary>
/// Optional capability marker for object data storage backends that can assemble a multipart object
/// from an ordered list of source file paths using kernel-side copy primitives. Pairs with
/// <see cref="IFileBackedMultipartPartSource"/>. When both sides implement these interfaces the
/// Complete facade uses a fast path that avoids routing bytes through userspace pipes.
/// </summary>
public interface IFileBackedObjectDataStorage
{
    /// <summary>
    /// Assembles the final object by concatenating the provided part files (in order) into a temp
    /// file and returning a <see cref="PreparedData"/> ready for commit.
    /// Returns <c>null</c> if the backend has opted out of the fast path for this call (feature
    /// flag disabled, unsupported platform, etc.) - callers must then fall back to the regular
    /// streaming path.
    /// </summary>
    Task<PreparedData?> PrepareMultipartDataFromFilesAsync(
        string bucketName,
        string key,
        IReadOnlyList<string> partPaths,
        CancellationToken cancellationToken = default);
}
