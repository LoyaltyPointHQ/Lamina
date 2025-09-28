namespace Lamina.WebApi.Authentication
{
    /// <summary>
    /// Default values used by S3 authentication scheme.
    /// </summary>
    public static class S3AuthenticationDefaults
    {
        /// <summary>
        /// The default authentication scheme name for S3 authentication.
        /// </summary>
        public const string AuthenticationScheme = "S3";

        /// <summary>
        /// The display name for the S3 authentication scheme.
        /// </summary>
        public const string DisplayName = "S3 Authentication";

        /// <summary>
        /// Default authentication header name.
        /// </summary>
        public const string AuthorizationHeaderName = "Authorization";

        /// <summary>
        /// S3 authentication method identifier.
        /// </summary>
        public const string AuthenticationMethod = "AWS4-HMAC-SHA256";

        /// <summary>
        /// S3 date header name.
        /// </summary>
        public const string DateHeaderName = "x-amz-date";

        /// <summary>
        /// S3 content SHA256 header name.
        /// </summary>
        public const string ContentSha256HeaderName = "x-amz-content-sha256";

        /// <summary>
        /// Special value for streaming payload.
        /// </summary>
        public const string StreamingPayload = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

        /// <summary>
        /// Special value for unsigned payload.
        /// </summary>
        public const string UnsignedPayload = "UNSIGNED-PAYLOAD";

        /// <summary>
        /// S3 service name.
        /// </summary>
        public const string ServiceName = "s3";

        /// <summary>
        /// AWS request terminator.
        /// </summary>
        public const string RequestTerminator = "aws4_request";
    }
}