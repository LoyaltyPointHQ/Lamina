using System.Collections.Generic;

namespace S3Test.Models
{
    public class S3User
    {
        public string AccessKeyId { get; set; } = string.Empty;
        public string SecretAccessKey { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public List<BucketPermission> BucketPermissions { get; set; } = new();
    }

    public class BucketPermission
    {
        public string BucketName { get; set; } = string.Empty;
        public List<string> Permissions { get; set; } = new();
    }

    public class AuthenticationSettings
    {
        public bool Enabled { get; set; } = false;
        public List<S3User> Users { get; set; } = new();
    }

    public class SignatureV4Request
    {
        public string Method { get; set; } = string.Empty;
        public string CanonicalUri { get; set; } = string.Empty;
        public string CanonicalQueryString { get; set; } = string.Empty;
        public Dictionary<string, string> Headers { get; set; } = new();
        public string Payload { get; set; } = string.Empty;
        public string Region { get; set; } = string.Empty;
        public string Service { get; set; } = "s3";
        public DateTime RequestDateTime { get; set; }
        public string AccessKeyId { get; set; } = string.Empty;
        public string Signature { get; set; } = string.Empty;
        public string SignedHeaders { get; set; } = string.Empty;
    }
}