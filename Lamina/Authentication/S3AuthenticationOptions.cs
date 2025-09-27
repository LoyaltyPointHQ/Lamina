using Microsoft.AspNetCore.Authentication;

namespace Lamina.Authentication
{
    /// <summary>
    /// Configuration options for S3 authentication.
    /// </summary>
    public class S3AuthenticationOptions : AuthenticationSchemeOptions
    {
        /// <summary>
        /// Gets or sets whether authentication is enabled.
        /// When disabled, all requests are treated as anonymous and authorized.
        /// </summary>
        public bool Enabled { get; set; } = false;

        /// <summary>
        /// Gets or sets the path to skip authentication for health checks.
        /// </summary>
        public string HealthCheckPath { get; set; } = "/health";

        /// <summary>
        /// Gets or sets the maximum allowed time skew for request timestamps.
        /// </summary>
        public TimeSpan MaxTimeSkew { get; set; } = TimeSpan.FromMinutes(15);

        /// <summary>
        /// Gets or sets whether to enable detailed authentication logging.
        /// </summary>
        public bool EnableDetailedLogging { get; set; } = false;
    }
}