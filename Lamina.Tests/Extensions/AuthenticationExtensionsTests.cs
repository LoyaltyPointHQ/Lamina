using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Lamina.Extensions;
using Lamina.Authentication;
using Lamina.Authorization;
using Lamina.Models;
using Microsoft.AspNetCore.Http;
using Lamina.Streaming.Validation;

namespace Lamina.Tests.Extensions
{
    public class MockAuthenticationService : Lamina.Services.IAuthenticationService
    {
        public Task<(bool isValid, S3User? user, string? error)> ValidateRequestAsync(HttpRequest request, string bucketName, string? objectKey = null, string? operation = null)
        {
            return Task.FromResult<(bool isValid, S3User? user, string? error)>((true, new S3User { AccessKeyId = "test", Name = "test" }, null));
        }

        public bool IsAuthenticationEnabled() => true;

        public S3User? GetUserByAccessKey(string accessKeyId) => new S3User { AccessKeyId = accessKeyId, Name = "test" };

        public bool UserHasAccessToBucket(S3User user, string bucketName, string? operation = null) => true;
    }

    public class MockStreamingAuthenticationService : Lamina.Streaming.IStreamingAuthenticationService
    {
        public Lamina.Streaming.Validation.IChunkSignatureValidator? CreateChunkValidator(HttpRequest request, S3User user)
        {
            return null;
        }
    }

    public class AuthenticationExtensionsTests
    {
        [Fact]
        public void AddS3Authentication_RegistersServices()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddS3Authentication();

            // Assert
            var serviceProvider = services.BuildServiceProvider();
            var authenticationService = serviceProvider.GetService<Microsoft.AspNetCore.Authentication.IAuthenticationService>();
            Assert.NotNull(authenticationService);
        }

        [Fact]
        public void AddS3Authorization_RegistersServices()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddScoped<Lamina.Services.IAuthenticationService, MockAuthenticationService>();

            // Act
            services.AddS3Authorization();

            // Assert
            var serviceProvider = services.BuildServiceProvider();
            
            // Check that authorization services are registered
            var authorizationService = serviceProvider.GetService<IAuthorizationService>();
            Assert.NotNull(authorizationService);

            // Check that our custom authorization handler is registered
            var authorizationHandlers = serviceProvider.GetServices<IAuthorizationHandler>();
            Assert.Contains(authorizationHandlers, h => h is S3AuthorizationHandler);
        }

        [Fact]
        public void AddS3Security_RegistersBothAuthenticationAndAuthorization()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddScoped<Lamina.Services.IAuthenticationService, MockAuthenticationService>();
            services.AddScoped<Lamina.Streaming.IStreamingAuthenticationService, MockStreamingAuthenticationService>();

            // Act
            services.AddS3Security();

            // Assert
            var serviceProvider = services.BuildServiceProvider();
            
            // Check authentication services
            var authenticationService = serviceProvider.GetService<Microsoft.AspNetCore.Authentication.IAuthenticationService>();
            Assert.NotNull(authenticationService);

            // Check authorization services
            var authorizationService = serviceProvider.GetService<IAuthorizationService>();
            Assert.NotNull(authorizationService);

            // Check that our custom authorization handler is registered
            var authorizationHandlers = serviceProvider.GetServices<IAuthorizationHandler>();
            Assert.Contains(authorizationHandlers, h => h is S3AuthorizationHandler);
        }

        [Fact]
        public void AddS3Authentication_WithCustomOptions_ConfiguresOptions()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddScoped<Lamina.Services.IAuthenticationService, MockAuthenticationService>();
            services.AddScoped<Lamina.Streaming.IStreamingAuthenticationService, MockStreamingAuthenticationService>();
            var customHealthPath = "/custom-health";

            // Act
            services.AddS3Authentication(options =>
            {
                options.HealthCheckPath = customHealthPath;
                options.Enabled = false;
            });

            // Assert
            var serviceProvider = services.BuildServiceProvider();
            var optionsMonitor = serviceProvider.GetService<Microsoft.Extensions.Options.IOptionsMonitor<S3AuthenticationOptions>>();
            Assert.NotNull(optionsMonitor);
            
            var options = optionsMonitor.Get(S3AuthenticationDefaults.AuthenticationScheme);
            Assert.Equal(customHealthPath, options.HealthCheckPath);
            Assert.False(options.Enabled);
        }

        [Fact]
        public void AddS3Authorization_CreatesCorrectPolicies()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddLogging();

            // Act
            services.AddS3Authorization();

            // Assert
            var serviceProvider = services.BuildServiceProvider();
            var authorizationOptions = serviceProvider.GetService<Microsoft.Extensions.Options.IOptions<AuthorizationOptions>>();
            Assert.NotNull(authorizationOptions);

            var options = authorizationOptions.Value;
            
            // Check that S3 policies are created
            Assert.True(options.GetPolicy("S3BucketRead") != null);
            Assert.True(options.GetPolicy("S3BucketWrite") != null);
            Assert.True(options.GetPolicy("S3BucketDelete") != null);
            Assert.True(options.GetPolicy("S3BucketList") != null);
            Assert.True(options.GetPolicy("S3Bucket*") != null);
            
            Assert.True(options.GetPolicy("S3ObjectRead") != null);
            Assert.True(options.GetPolicy("S3ObjectWrite") != null);
            Assert.True(options.GetPolicy("S3ObjectDelete") != null);
            Assert.True(options.GetPolicy("S3ObjectList") != null);
            Assert.True(options.GetPolicy("S3Object*") != null);
        }
    }
}