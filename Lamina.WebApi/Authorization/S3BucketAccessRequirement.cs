using Microsoft.AspNetCore.Authorization;

namespace Lamina.WebApi.Authorization
{
    /// <summary>
    /// Authorization requirement for S3 bucket access.
    /// </summary>
    public class S3BucketAccessRequirement : IAuthorizationRequirement
    {
        /// <summary>
        /// Initializes a new instance of the S3BucketAccessRequirement class.
        /// </summary>
        /// <param name="operation">The required operation (read, write, delete, list, etc.)</param>
        public S3BucketAccessRequirement(string operation)
        {
            Operation = operation ?? throw new ArgumentNullException(nameof(operation));
        }

        /// <summary>
        /// Gets the required operation.
        /// </summary>
        public string Operation { get; }
    }
}