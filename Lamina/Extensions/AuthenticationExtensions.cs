using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Lamina.Authentication;
using Lamina.Authorization;
using Lamina.Models;

namespace Lamina.Extensions
{
    /// <summary>
    /// Extension methods for configuring S3 authentication and authorization.
    /// </summary>
    public static class AuthenticationExtensions
    {
        /// <summary>
        /// Adds S3 authentication to the service collection.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="configureOptions">Optional configuration for S3 authentication options.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddS3Authentication(
            this IServiceCollection services,
            Action<S3AuthenticationOptions>? configureOptions = null)
        {
            // Add authentication with S3 scheme
            services.AddAuthentication(S3AuthenticationDefaults.AuthenticationScheme)
                .AddScheme<S3AuthenticationOptions, S3AuthenticationHandler>(
                    S3AuthenticationDefaults.AuthenticationScheme,
                    S3AuthenticationDefaults.DisplayName,
                    configureOptions);

            return services;
        }

        /// <summary>
        /// Adds S3 authorization to the service collection.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddS3Authorization(this IServiceCollection services)
        {
            services.AddAuthorization(options =>
            {
                // Define policies for different S3 operations
                AddS3Policy(options, "S3BucketRead", S3Operations.Read, S3ResourceType.Bucket);
                AddS3Policy(options, "S3BucketWrite", S3Operations.Write, S3ResourceType.Bucket);
                AddS3Policy(options, "S3BucketDelete", S3Operations.Delete, S3ResourceType.Bucket);
                AddS3Policy(options, "S3BucketList", S3Operations.List, S3ResourceType.Bucket);
                AddS3Policy(options, "S3Bucket*", S3Operations.All, S3ResourceType.Bucket);

                AddS3Policy(options, "S3ObjectRead", S3Operations.Read, S3ResourceType.Object);
                AddS3Policy(options, "S3ObjectWrite", S3Operations.Write, S3ResourceType.Object);
                AddS3Policy(options, "S3ObjectDelete", S3Operations.Delete, S3ResourceType.Object);
                AddS3Policy(options, "S3ObjectList", S3Operations.List, S3ResourceType.Object);
                AddS3Policy(options, "S3Object*", S3Operations.All, S3ResourceType.Object);

                // Set default policy to allow anonymous when authentication is disabled
                options.DefaultPolicy = new AuthorizationPolicyBuilder()
                    .AddAuthenticationSchemes(S3AuthenticationDefaults.AuthenticationScheme)
                    .RequireAuthenticatedUser()
                    .Build();

                // Fallback policy allows anonymous access when authentication is disabled
                options.FallbackPolicy = new AuthorizationPolicyBuilder()
                    .AddAuthenticationSchemes(S3AuthenticationDefaults.AuthenticationScheme)
                    .RequireAssertion(context =>
                    {
                        // Always allow when authentication is disabled
                        var authSettings = context.Resource is HttpContext httpContext
                            ? httpContext.RequestServices.GetService<IOptions<AuthenticationSettings>>()?.Value
                            : null;

                        return authSettings?.Enabled == false || context.User.Identity?.IsAuthenticated == true;
                    })
                    .Build();
            });

            // Register the authorization handler
            services.AddScoped<IAuthorizationHandler, S3AuthorizationHandler>();

            return services;
        }

        /// <summary>
        /// Adds complete S3 authentication and authorization to the service collection.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="configureOptions">Optional configuration for S3 authentication options.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddS3Security(
            this IServiceCollection services,
            Action<S3AuthenticationOptions>? configureOptions = null)
        {
            return services
                .AddS3Authentication(configureOptions)
                .AddS3Authorization();
        }

        /// <summary>
        /// Adds an S3 authorization policy.
        /// </summary>
        private static void AddS3Policy(
            AuthorizationOptions options,
            string policyName,
            string operation,
            S3ResourceType resourceType)
        {
            options.AddPolicy(policyName, policy =>
            {
                policy.AddAuthenticationSchemes(S3AuthenticationDefaults.AuthenticationScheme);
                
                if (resourceType == S3ResourceType.Bucket)
                {
                    policy.AddRequirements(new S3BucketAccessRequirement(operation));
                }
                else
                {
                    policy.AddRequirements(new S3ObjectAccessRequirement(operation));
                }
            });
        }
    }
}