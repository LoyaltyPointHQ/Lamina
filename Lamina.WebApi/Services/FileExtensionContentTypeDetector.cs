using Lamina.Storage.Core.Abstract;
using Microsoft.AspNetCore.StaticFiles;

namespace Lamina.WebApi.Services
{
    /// <summary>
    /// Implementation of IContentTypeDetector using ASP.NET Core's FileExtensionContentTypeProvider
    /// </summary>
    public class FileExtensionContentTypeDetector : IContentTypeDetector
    {
        private readonly IContentTypeProvider _contentTypeProvider;

        public FileExtensionContentTypeDetector()
        {
            _contentTypeProvider = new FileExtensionContentTypeProvider();
        }

        public bool TryGetContentType(string path, out string? contentType)
        {
            return _contentTypeProvider.TryGetContentType(path, out contentType);
        }
    }
}