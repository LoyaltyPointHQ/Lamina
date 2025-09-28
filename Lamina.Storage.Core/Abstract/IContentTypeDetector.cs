namespace Lamina.Storage.Core.Abstract
{
    /// <summary>
    /// Abstraction for detecting content types based on file paths or extensions
    /// </summary>
    public interface IContentTypeDetector
    {
        /// <summary>
        /// Attempts to determine the content type for a given file path
        /// </summary>
        /// <param name="path">The file path or name</param>
        /// <param name="contentType">The detected content type, if successful</param>
        /// <returns>True if a content type was detected, false otherwise</returns>
        bool TryGetContentType(string path, out string? contentType);
    }
}