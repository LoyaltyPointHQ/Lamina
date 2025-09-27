using Microsoft.AspNetCore.Authorization;

namespace Lamina.Authorization
{
    /// <summary>
    /// Attribute for declarative S3 authorization.
    /// </summary>
    public class S3AuthorizeAttribute : AuthorizeAttribute
    {
        /// <summary>
        /// Initializes a new instance of the S3AuthorizeAttribute class for bucket operations.
        /// </summary>
        /// <param name="operation">The required S3 operation (read, write, delete, list, etc.)</param>
        /// <param name="resourceType">The type of resource (bucket or object)</param>
        public S3AuthorizeAttribute(string operation, S3ResourceType resourceType = S3ResourceType.Bucket)
        {
            Operation = operation ?? throw new ArgumentNullException(nameof(operation));
            ResourceType = resourceType;
            Policy = GetPolicyName(operation, resourceType);
        }

        /// <summary>
        /// Gets the required S3 operation.
        /// </summary>
        public string Operation { get; }

        /// <summary>
        /// Gets the type of S3 resource.
        /// </summary>
        public S3ResourceType ResourceType { get; }

        /// <summary>
        /// Gets the policy name for the specified operation and resource type.
        /// </summary>
        private static string GetPolicyName(string operation, S3ResourceType resourceType)
        {
            return resourceType switch
            {
                S3ResourceType.Bucket => $"S3Bucket{operation}",
                S3ResourceType.Object => $"S3Object{operation}",
                _ => throw new ArgumentOutOfRangeException(nameof(resourceType))
            };
        }
    }

    /// <summary>
    /// Enumeration of S3 resource types.
    /// </summary>
    public enum S3ResourceType
    {
        /// <summary>
        /// S3 bucket resource.
        /// </summary>
        Bucket,

        /// <summary>
        /// S3 object resource.
        /// </summary>
        Object
    }

    /// <summary>
    /// Common S3 operations.
    /// </summary>
    public static class S3Operations
    {
        /// <summary>
        /// Read operation (GET, HEAD).
        /// </summary>
        public const string Read = "read";

        /// <summary>
        /// Write operation (PUT, POST).
        /// </summary>
        public const string Write = "write";

        /// <summary>
        /// Delete operation (DELETE).
        /// </summary>
        public const string Delete = "delete";

        /// <summary>
        /// List operation (GET for buckets/objects listing).
        /// </summary>
        public const string List = "list";

        /// <summary>
        /// All operations (wildcard).
        /// </summary>
        public const string All = "*";
    }
}