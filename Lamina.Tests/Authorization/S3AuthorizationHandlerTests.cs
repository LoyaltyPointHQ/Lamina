using System.Security.Claims;
using Lamina.Core.Models;
using Lamina.WebApi.Authorization;
using Lamina.WebApi.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace Lamina.Tests.Authorization
{
    public class S3AuthorizationHandlerTests
    {
        private readonly Mock<IAuthenticationService> _authServiceMock;
        private readonly Mock<IOptions<AuthenticationSettings>> _authSettingsMock;
        private readonly Mock<ILogger<S3AuthorizationHandler>> _loggerMock;
        private readonly AuthenticationSettings _authSettings;
        private readonly S3AuthorizationHandler _handler;

        public S3AuthorizationHandlerTests()
        {
            _authServiceMock = new Mock<IAuthenticationService>();
            _authSettingsMock = new Mock<IOptions<AuthenticationSettings>>();
            _loggerMock = new Mock<ILogger<S3AuthorizationHandler>>();
            
            _authSettings = new AuthenticationSettings { Enabled = true };
            _authSettingsMock.Setup(x => x.Value).Returns(_authSettings);

            _handler = new S3AuthorizationHandler(
                _authServiceMock.Object,
                _authSettingsMock.Object,
                _loggerMock.Object);
        }

        private ClaimsPrincipal CreateS3User(S3User s3User)
        {
            var identity = new ClaimsIdentity("S3");
            identity.AddClaim(new Claim(ClaimTypes.NameIdentifier, s3User.AccessKeyId));
            identity.AddClaim(new Claim(ClaimTypes.Name, s3User.Name));
            identity.AddClaim(new Claim("s3_user", System.Text.Json.JsonSerializer.Serialize(s3User)));
            
            return new ClaimsPrincipal(identity);
        }

        private ClaimsPrincipal CreateAnonymousUser()
        {
            var identity = new ClaimsIdentity("S3");
            identity.AddClaim(new Claim(ClaimTypes.Anonymous, "true"));
            return new ClaimsPrincipal(identity);
        }

        private HttpContext CreateHttpContext(string path)
        {
            var context = new DefaultHttpContext();
            context.Request.Path = path;
            return context;
        }

        [Fact]
        public async Task HandleAsync_AuthenticationDisabled_BucketRequirement_Succeeds()
        {
            // Arrange
            _authSettings.Enabled = false;
            var requirement = new S3BucketAccessRequirement("read");
            var user = new ClaimsPrincipal();
            var httpContext = CreateHttpContext("/test-bucket");
            
            var authContext = new AuthorizationHandlerContext(
                new[] { requirement }, user, httpContext);

            // Act
            await _handler.HandleAsync(authContext);

            // Assert
            Assert.True(authContext.HasSucceeded);
        }

        [Fact]
        public async Task HandleAsync_AuthenticationDisabled_ObjectRequirement_Succeeds()
        {
            // Arrange
            _authSettings.Enabled = false;
            var requirement = new S3ObjectAccessRequirement("read");
            var user = new ClaimsPrincipal();
            var httpContext = CreateHttpContext("/test-bucket/object.txt");
            
            var authContext = new AuthorizationHandlerContext(
                new[] { requirement }, user, httpContext);

            // Act
            await _handler.HandleAsync(authContext);

            // Assert
            Assert.True(authContext.HasSucceeded);
        }

        [Fact]
        public async Task HandleAsync_AnonymousUser_Succeeds()
        {
            // Arrange
            var requirement = new S3BucketAccessRequirement("read");
            var user = CreateAnonymousUser();
            var httpContext = CreateHttpContext("/test-bucket");
            
            var authContext = new AuthorizationHandlerContext(
                new[] { requirement }, user, httpContext);

            // Act
            await _handler.HandleAsync(authContext);

            // Assert
            Assert.True(authContext.HasSucceeded);
        }

        [Fact]
        public async Task HandleAsync_BucketRequirement_ValidUser_Succeeds()
        {
            // Arrange
            var s3User = new S3User
            {
                AccessKeyId = "testkey",
                Name = "testuser",
                BucketPermissions = new List<BucketPermission>
                {
                    new BucketPermission
                    {
                        BucketName = "test-bucket",
                        Permissions = new List<string> { "read" }
                    }
                }
            };

            var requirement = new S3BucketAccessRequirement("read");
            var user = CreateS3User(s3User);
            var httpContext = CreateHttpContext("/test-bucket");

            _authServiceMock.Setup(x => x.UserHasAccessToBucket(It.IsAny<S3User>(), "test-bucket", "read"))
                .Returns(true);
            
            var authContext = new AuthorizationHandlerContext(
                new[] { requirement }, user, httpContext);

            // Act
            await _handler.HandleAsync(authContext);

            // Assert
            Assert.True(authContext.HasSucceeded);
            _authServiceMock.Verify(x => x.UserHasAccessToBucket(It.IsAny<S3User>(), "test-bucket", "read"), Times.Once);
        }

        [Fact]
        public async Task HandleAsync_BucketRequirement_InsufficientPermissions_Fails()
        {
            // Arrange
            var s3User = new S3User
            {
                AccessKeyId = "testkey",
                Name = "testuser",
                BucketPermissions = new List<BucketPermission>
                {
                    new BucketPermission
                    {
                        BucketName = "other-bucket",
                        Permissions = new List<string> { "read" }
                    }
                }
            };

            var requirement = new S3BucketAccessRequirement("read");
            var user = CreateS3User(s3User);
            var httpContext = CreateHttpContext("/test-bucket");

            _authServiceMock.Setup(x => x.UserHasAccessToBucket(It.IsAny<S3User>(), "test-bucket", "read"))
                .Returns(false);
            
            var authContext = new AuthorizationHandlerContext(
                new[] { requirement }, user, httpContext);

            // Act
            await _handler.HandleAsync(authContext);

            // Assert
            Assert.False(authContext.HasSucceeded);
            Assert.True(authContext.HasFailed);
        }

        [Fact]
        public async Task HandleAsync_ObjectRequirement_ValidUser_Succeeds()
        {
            // Arrange
            var s3User = new S3User
            {
                AccessKeyId = "testkey",
                Name = "testuser",
                BucketPermissions = new List<BucketPermission>
                {
                    new BucketPermission
                    {
                        BucketName = "test-bucket",
                        Permissions = new List<string> { "write" }
                    }
                }
            };

            var requirement = new S3ObjectAccessRequirement("write");
            var user = CreateS3User(s3User);
            var httpContext = CreateHttpContext("/test-bucket/path/to/object.txt");

            _authServiceMock.Setup(x => x.UserHasAccessToBucket(It.IsAny<S3User>(), "test-bucket", "write"))
                .Returns(true);
            
            var authContext = new AuthorizationHandlerContext(
                new[] { requirement }, user, httpContext);

            // Act
            await _handler.HandleAsync(authContext);

            // Assert
            Assert.True(authContext.HasSucceeded);
            _authServiceMock.Verify(x => x.UserHasAccessToBucket(It.IsAny<S3User>(), "test-bucket", "write"), Times.Once);
        }

        [Fact]
        public async Task HandleAsync_ListBucketsOperation_WithAnyPermission_Succeeds()
        {
            // Arrange
            var s3User = new S3User
            {
                AccessKeyId = "testkey",
                Name = "testuser",
                BucketPermissions = new List<BucketPermission>
                {
                    new BucketPermission
                    {
                        BucketName = "some-bucket",
                        Permissions = new List<string> { "list" }
                    }
                }
            };

            var requirement = new S3BucketAccessRequirement("list");
            var user = CreateS3User(s3User);
            var httpContext = CreateHttpContext("/"); // Root path for list buckets
            
            var authContext = new AuthorizationHandlerContext(
                new[] { requirement }, user, httpContext);

            // Act
            await _handler.HandleAsync(authContext);

            // Assert
            Assert.True(authContext.HasSucceeded);
        }

        [Fact]
        public async Task HandleAsync_ListBucketsOperation_NoPermissions_Fails()
        {
            // Arrange
            var s3User = new S3User
            {
                AccessKeyId = "testkey",
                Name = "testuser",
                BucketPermissions = new List<BucketPermission>()
            };

            var requirement = new S3BucketAccessRequirement("list");
            var user = CreateS3User(s3User);
            var httpContext = CreateHttpContext("/"); // Root path for list buckets
            
            var authContext = new AuthorizationHandlerContext(
                new[] { requirement }, user, httpContext);

            // Act
            await _handler.HandleAsync(authContext);

            // Assert
            Assert.False(authContext.HasSucceeded);
            Assert.True(authContext.HasFailed);
        }

        [Fact]
        public async Task HandleAsync_NoHttpContext_Fails()
        {
            // Arrange
            var s3User = new S3User
            {
                AccessKeyId = "testkey",
                Name = "testuser",
                BucketPermissions = new List<BucketPermission>()
            };

            var requirement = new S3BucketAccessRequirement("read");
            var user = CreateS3User(s3User);
            
            var authContext = new AuthorizationHandlerContext(
                new[] { requirement }, user, null); // No HTTP context

            // Act
            await _handler.HandleAsync(authContext);

            // Assert
            Assert.False(authContext.HasSucceeded);
            Assert.True(authContext.HasFailed);
        }

        [Fact]
        public async Task HandleAsync_NoS3UserInClaims_Fails()
        {
            // Arrange
            var requirement = new S3BucketAccessRequirement("read");
            var user = new ClaimsPrincipal(new ClaimsIdentity("S3")); // No S3 user claim
            var httpContext = CreateHttpContext("/test-bucket");
            
            var authContext = new AuthorizationHandlerContext(
                new[] { requirement }, user, httpContext);

            // Act
            await _handler.HandleAsync(authContext);

            // Assert
            Assert.False(authContext.HasSucceeded);
            Assert.True(authContext.HasFailed);
        }

        [Fact]
        public async Task HandleAsync_ObjectRequirement_NoBucketName_Fails()
        {
            // Arrange
            var s3User = new S3User
            {
                AccessKeyId = "testkey",
                Name = "testuser",
                BucketPermissions = new List<BucketPermission>()
            };

            var requirement = new S3ObjectAccessRequirement("read");
            var user = CreateS3User(s3User);
            var httpContext = CreateHttpContext("/"); // Root path, no bucket for object operation
            
            var authContext = new AuthorizationHandlerContext(
                new[] { requirement }, user, httpContext);

            // Act
            await _handler.HandleAsync(authContext);

            // Assert
            Assert.False(authContext.HasSucceeded);
            Assert.True(authContext.HasFailed);
        }
    }
}