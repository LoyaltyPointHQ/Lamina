using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using S3Test.Models;
using S3Test.Services;
using Xunit;

namespace S3Test.Tests.Services
{
    public class AuthenticationServiceTests
    {
        private readonly Mock<ILogger<AuthenticationService>> _loggerMock;
        private readonly AuthenticationService _authService;
        private readonly AuthenticationSettings _authSettings;

        public AuthenticationServiceTests()
        {
            _loggerMock = new Mock<ILogger<AuthenticationService>>();
            _authSettings = new AuthenticationSettings
            {
                Enabled = true,
                Users = new List<S3User>
                {
                    new S3User
                    {
                        AccessKeyId = "TESTACCESSKEY",
                        SecretAccessKey = "TestSecretKey123",
                        Name = "testuser",
                        BucketPermissions = new List<BucketPermission>
                        {
                            new BucketPermission
                            {
                                BucketName = "*",
                                Permissions = new List<string> { "*" }
                            }
                        }
                    },
                    new S3User
                    {
                        AccessKeyId = "LIMITEDUSER",
                        SecretAccessKey = "LimitedSecret456",
                        Name = "limiteduser",
                        BucketPermissions = new List<BucketPermission>
                        {
                            new BucketPermission
                            {
                                BucketName = "test-bucket",
                                Permissions = new List<string> { "read", "list" }
                            }
                        }
                    }
                }
            };

            var options = Options.Create(_authSettings);
            _authService = new AuthenticationService(_loggerMock.Object, options);
        }

        [Fact]
        public void IsAuthenticationEnabled_ReturnsCorrectValue()
        {
            Assert.True(_authService.IsAuthenticationEnabled());

            var disabledSettings = new AuthenticationSettings { Enabled = false };
            var disabledOptions = Options.Create(disabledSettings);
            var disabledService = new AuthenticationService(_loggerMock.Object, disabledOptions);

            Assert.False(disabledService.IsAuthenticationEnabled());
        }

        [Fact]
        public void GetUserByAccessKey_ReturnsCorrectUser()
        {
            var user = _authService.GetUserByAccessKey("TESTACCESSKEY");
            Assert.NotNull(user);
            Assert.Equal("testuser", user.Name);
            Assert.Equal("TESTACCESSKEY", user.AccessKeyId);
        }

        [Fact]
        public void GetUserByAccessKey_ReturnsNullForInvalidKey()
        {
            var user = _authService.GetUserByAccessKey("INVALIDKEY");
            Assert.Null(user);
        }

        [Fact]
        public async Task ValidateRequest_AllowsWhenAuthDisabled()
        {
            var disabledSettings = new AuthenticationSettings { Enabled = false };
            var disabledOptions = Options.Create(disabledSettings);
            var disabledService = new AuthenticationService(_loggerMock.Object, disabledOptions);

            var context = new DefaultHttpContext();
            var (isValid, user, error) = await disabledService.ValidateRequestAsync(context.Request, "test-bucket");

            Assert.True(isValid);
            Assert.Null(user);
            Assert.Null(error);
        }

        [Fact]
        public async Task ValidateRequest_RejectsWithoutAuthHeader()
        {
            var context = new DefaultHttpContext();
            var (isValid, user, error) = await _authService.ValidateRequestAsync(context.Request, "test-bucket");

            Assert.False(isValid);
            Assert.Null(user);
            Assert.Equal("Missing Authorization header", error);
        }

        [Fact]
        public async Task ValidateRequest_RejectsUnsupportedAuthMethod()
        {
            var context = new DefaultHttpContext();
            context.Request.Headers["Authorization"] = "Basic dGVzdDp0ZXN0";

            var (isValid, user, error) = await _authService.ValidateRequestAsync(context.Request, "test-bucket");

            Assert.False(isValid);
            Assert.Null(user);
            Assert.Equal("Unsupported authentication method", error);
        }

        [Fact]
        public async Task ValidateRequest_RejectsInvalidAccessKey()
        {
            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=INVALIDKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "localhost";
            context.Request.Method = "GET";
            context.Request.Path = "/test-bucket";

            var (isValid, user, error) = await _authService.ValidateRequestAsync(context.Request, "test-bucket");

            Assert.False(isValid);
            Assert.Null(user);
            Assert.Equal("Invalid access key", error);
        }

        [Fact]
        public async Task ValidateRequest_ChecksPermissions()
        {
            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=LIMITEDUSER/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "localhost";
            context.Request.Method = "PUT";
            context.Request.Path = "/test-bucket/file.txt";

            var (isValid, user, error) = await _authService.ValidateRequestAsync(context.Request, "test-bucket", "file.txt", "PUT");

            Assert.False(isValid);
            Assert.NotNull(user);
            Assert.Equal("Access denied", error);
        }

        [Theory]
        [InlineData("GET", "read")]
        [InlineData("HEAD", "read")]
        [InlineData("PUT", "write")]
        [InlineData("POST", "write")]
        [InlineData("DELETE", "delete")]
        public async Task ValidateRequest_MapsOperationsToPermissions(string httpMethod, string expectedPermission)
        {
            var testUser = new S3User
            {
                AccessKeyId = "TESTKEY",
                SecretAccessKey = "secret",
                Name = "test",
                BucketPermissions = new List<BucketPermission>
                {
                    new BucketPermission
                    {
                        BucketName = "test-bucket",
                        Permissions = new List<string> { expectedPermission }
                    }
                }
            };

            var settings = new AuthenticationSettings
            {
                Enabled = true,
                Users = new List<S3User> { testUser }
            };

            var service = new AuthenticationService(_loggerMock.Object, Options.Create(settings));

            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=test";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "localhost";
            context.Request.Method = httpMethod;
            context.Request.Path = "/test-bucket/file.txt";

            var (isValid, user, error) = await service.ValidateRequestAsync(context.Request, "test-bucket", "file.txt", httpMethod);

            Assert.NotNull(user);
        }
    }
}