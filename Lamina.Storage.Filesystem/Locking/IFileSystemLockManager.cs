namespace Lamina.Storage.Filesystem.Locking;

public interface IFileSystemLockManager
{
    Task<T?> ReadFileAsync<T>(string filePath, Func<string, Task<T>> readOperation, CancellationToken cancellationToken = default);
    Task WriteFileAsync(string filePath, string content, CancellationToken cancellationToken = default);
    Task<bool> DeleteFile(string filePath);

    /// <summary>
    /// Atomically reads, transforms, and writes back file contents under a writer lock.
    /// Transform receives current contents (null if file does not exist) and returns new contents,
    /// or null to indicate no write should happen.
    /// </summary>
    Task<bool> UpdateFileAsync(string filePath, Func<string?, Task<string?>> transform, CancellationToken cancellationToken = default);
}