namespace Lamina.Storage.Filesystem.Locking;

public interface IFileSystemLockManager
{
    Task<T?> ReadFileAsync<T>(string filePath, Func<string, Task<T>> readOperation, CancellationToken cancellationToken = default);
    Task WriteFileAsync(string filePath, string content, CancellationToken cancellationToken = default);
    Task<bool> DeleteFile(string filePath);
}