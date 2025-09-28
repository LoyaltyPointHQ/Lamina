using Microsoft.AspNetCore.Authorization;

namespace Lamina.WebApi.Authorization
{
    /// <summary>
    /// Authorization requirement for S3 object access.
    /// </summary>
    public class S3ObjectAccessRequirement : IAuthorizationRequirement
    {
        /// <summary>
        /// Initializes a new instance of the S3ObjectAccessRequirement class.
        /// </summary>
        /// <param name="operation">The required operation (read, write, delete, etc.)</param>
        public S3ObjectAccessRequirement(string operation)
        {
            Operation = operation ?? throw new ArgumentNullException(nameof(operation));
        }

        /// <summary>
        /// Gets the required operation.
        /// </summary>
        public string Operation { get; }
    }
}