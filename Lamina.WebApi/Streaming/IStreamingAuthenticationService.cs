using Lamina.Core.Models;
using Lamina.Core.Streaming;

namespace Lamina.WebApi.Streaming
{
    /// <summary>
    /// Interface for handling streaming authentication with chunk signature verification
    /// </summary>
    public interface IStreamingAuthenticationService
    {
        /// <summary>
        /// Creates a streaming validator for STREAMING-AWS4-HMAC-SHA256-PAYLOAD requests
        /// </summary>
        IChunkSignatureValidator? CreateChunkValidator(HttpRequest request, S3User user);
    }
}