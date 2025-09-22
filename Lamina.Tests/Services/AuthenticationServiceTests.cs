using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Lamina.Models;
using Lamina.Services;
using Xunit;

namespace Lamina.Tests.Services
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
        [InlineData("GET", "write")]  // GET needs read but user only has write
        [InlineData("HEAD", "write")] // HEAD needs read but user only has write
        [InlineData("PUT", "read")]   // PUT needs write but user only has read
        [InlineData("POST", "read")]  // POST needs write but user only has read
        [InlineData("DELETE", "read")] // DELETE needs delete but user only has read
        public async Task ValidateRequest_RejectsWrongPermissions(string httpMethod, string userPermission)
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
                        Permissions = new List<string> { userPermission }
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

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "localhost";
            context.Request.Method = httpMethod;
            context.Request.Path = "/test-bucket/file.txt";

            var (isValid, user, error) = await service.ValidateRequestAsync(context.Request, "test-bucket", "file.txt", httpMethod);

            Assert.False(isValid);
            Assert.NotNull(user);
            Assert.Equal("Access denied", error);
        }

        [Fact]
        public async Task ValidateRequestAsync_RejectsInvalidAuthHeaderFormat()
        {
            var context = new DefaultHttpContext();
            context.Request.Headers["Authorization"] = "AWS4-HMAC-SHA256 Credential=invalid-format";

            var (isValid, user, error) = await _authService.ValidateRequestAsync(context.Request, "test-bucket");

            Assert.False(isValid);
            Assert.Null(user);
            Assert.Equal("Invalid authorization header format", error);
        }

        [Fact]
        public async Task ValidateRequestAsync_RejectsMissingXAmzDate()
        {
            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTACCESSKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abcd1234";
            context.Request.Headers["Host"] = "localhost";

            var (isValid, user, error) = await _authService.ValidateRequestAsync(context.Request, "test-bucket");

            Assert.False(isValid);
            Assert.Null(user);
            Assert.Equal("Invalid authorization header format", error);
        }

        [Fact]
        public async Task ValidateRequestAsync_HandlesEmptyPayload()
        {
            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTACCESSKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "localhost";
            context.Request.Method = "GET";
            context.Request.Path = "/test-bucket";

            var (isValid, user, error) = await _authService.ValidateRequestAsync(context.Request, "test-bucket");

            Assert.False(isValid);
            Assert.NotNull(user);
            Assert.Equal("Invalid signature", error);
        }

        [Fact]
        public async Task ValidateRequestAsync_AcceptsWildcardBucketPermissions()
        {
            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTACCESSKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "localhost";
            context.Request.Method = "GET";
            context.Request.Path = "/any-bucket";

            var (isValid, user, error) = await _authService.ValidateRequestAsync(context.Request, "any-bucket", null, "GET");

            Assert.False(isValid);
            Assert.NotNull(user);
            Assert.Equal("Invalid signature", error);
        }

        [Fact]
        public async Task ValidateRequestAsync_RejectsUserWithNoBucketPermissions()
        {
            var userWithNoPerms = new S3User
            {
                AccessKeyId = "NOPERMSKEY",
                SecretAccessKey = "secret",
                Name = "noperms",
                BucketPermissions = new List<BucketPermission>()
            };

            var settings = new AuthenticationSettings
            {
                Enabled = true,
                Users = new List<S3User> { userWithNoPerms }
            };

            var service = new AuthenticationService(_loggerMock.Object, Options.Create(settings));

            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=NOPERMSKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "localhost";
            context.Request.Method = "GET";
            context.Request.Path = "/test-bucket";

            var (isValid, user, error) = await service.ValidateRequestAsync(context.Request, "test-bucket", null, "GET");

            Assert.False(isValid);
            Assert.NotNull(user);
            Assert.Equal("Access denied", error);
        }

        [Fact]
        public async Task ValidateRequestAsync_RejectsUserWithNullBucketPermissions()
        {
            var userWithNullPerms = new S3User
            {
                AccessKeyId = "NULLPERMSKEY",
                SecretAccessKey = "secret",
                Name = "nullperms",
                BucketPermissions = null!
            };

            var settings = new AuthenticationSettings
            {
                Enabled = true,
                Users = new List<S3User> { userWithNullPerms }
            };

            var service = new AuthenticationService(_loggerMock.Object, Options.Create(settings));

            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=NULLPERMSKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "localhost";
            context.Request.Method = "GET";
            context.Request.Path = "/test-bucket";

            var (isValid, user, error) = await service.ValidateRequestAsync(context.Request, "test-bucket", null, "GET");

            Assert.False(isValid);
            Assert.NotNull(user);
            Assert.Equal("Access denied", error);
        }

        [Fact]
        public async Task ValidateRequestAsync_HandlesUnknownOperation()
        {
            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTACCESSKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "localhost";
            context.Request.Method = "PATCH";
            context.Request.Path = "/test-bucket";

            var (isValid, user, error) = await _authService.ValidateRequestAsync(context.Request, "test-bucket", null, "UNKNOWN");

            Assert.False(isValid);
            Assert.NotNull(user);
            Assert.Equal("Invalid signature", error);
        }

        [Fact]
        public async Task ValidateRequestAsync_HandlesCaseInsensitivePermissions()
        {
            var userWithMixedCasePerms = new S3User
            {
                AccessKeyId = "MIXEDCASEKEY",
                SecretAccessKey = "secret",
                Name = "mixedcase",
                BucketPermissions = new List<BucketPermission>
                {
                    new BucketPermission
                    {
                        BucketName = "test-bucket",
                        Permissions = new List<string> { "READ", "Write" }
                    }
                }
            };

            var settings = new AuthenticationSettings
            {
                Enabled = true,
                Users = new List<S3User> { userWithMixedCasePerms }
            };

            var service = new AuthenticationService(_loggerMock.Object, Options.Create(settings));

            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=MIXEDCASEKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "localhost";
            context.Request.Method = "GET";
            context.Request.Path = "/test-bucket";

            var (isValid, user, error) = await service.ValidateRequestAsync(context.Request, "test-bucket", null, "GET");

            Assert.False(isValid);
            Assert.NotNull(user);
            Assert.Equal("Invalid signature", error);
        }

        [Fact]
        public async Task ValidateRequestAsync_HandlesCaseInsensitiveBucketName()
        {
            var userWithSpecificBucket = new S3User
            {
                AccessKeyId = "BUCKETKEY",
                SecretAccessKey = "secret",
                Name = "bucketuser",
                BucketPermissions = new List<BucketPermission>
                {
                    new BucketPermission
                    {
                        BucketName = "Test-Bucket",
                        Permissions = new List<string> { "read" }
                    }
                }
            };

            var settings = new AuthenticationSettings
            {
                Enabled = true,
                Users = new List<S3User> { userWithSpecificBucket }
            };

            var service = new AuthenticationService(_loggerMock.Object, Options.Create(settings));

            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=BUCKETKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "localhost";
            context.Request.Method = "GET";
            context.Request.Path = "/test-bucket";

            var (isValid, user, error) = await service.ValidateRequestAsync(context.Request, "test-bucket", null, "GET");

            Assert.False(isValid);
            Assert.NotNull(user);
            Assert.Equal("Invalid signature", error);
        }

        [Fact]
        public async Task ValidateRequestAsync_HandlesNullOperation()
        {
            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTACCESSKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "localhost";
            context.Request.Method = "GET";
            context.Request.Path = "/test-bucket";

            var (isValid, user, error) = await _authService.ValidateRequestAsync(context.Request, "test-bucket", null, null);

            Assert.False(isValid);
            Assert.NotNull(user);
            Assert.Equal("Invalid signature", error);
        }

        [Theory]
        [InlineData("list")]
        public async Task ValidateRequestAsync_SupportsListOperation(string operation)
        {
            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=LIMITEDUSER/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "localhost";
            context.Request.Method = "GET";
            context.Request.Path = "/test-bucket";

            var (isValid, user, error) = await _authService.ValidateRequestAsync(context.Request, "test-bucket", null, operation);

            Assert.False(isValid);
            Assert.NotNull(user);
            Assert.Equal("Invalid signature", error);
        }

        [Fact]
        public async Task ValidateRequestAsync_HandlesSpecialCharactersInUri()
        {
            // Test that the URI encoding fix handles special characters properly
            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            // Test with a path that has special characters like in the error (dots in filename)
            context.Request.Method = "PUT";
            context.Request.Path = "/oadp/velro/kopia/levelup/kopia.blobcfg";
            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTACCESSKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "lamina.openshift-adp.svc";
            context.Request.ContentType = "application/x-kopia";

            var (isValid, user, error) = await _authService.ValidateRequestAsync(context.Request, "oadp", "velro/kopia/levelup/kopia.blobcfg", "PUT");

            // Should fail with signature mismatch (expected since we used dummy signature)
            // But the important thing is that URI encoding should not cause parsing errors
            Assert.False(isValid);
            Assert.NotNull(user);
            Assert.Equal("Invalid signature", error);
        }

        [Fact]
        public async Task ValidateRequestAsync_HandlesStreamingPayloadHash()
        {
            // Test that the streaming payload hash is handled correctly
            var context = new DefaultHttpContext();
            var dateTime = DateTime.UtcNow;
            var dateStamp = dateTime.ToString("yyyyMMdd");
            var amzDate = dateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            // Test with streaming payload hash as seen in the error logs
            context.Request.Method = "PUT";
            context.Request.Path = "/oadp/velro/kopia/levelup/kopia.blobcfg";
            context.Request.Headers["Authorization"] = $"AWS4-HMAC-SHA256 Credential=TESTACCESSKEY/{dateStamp}/us-east-1/s3/aws4_request, SignedHeaders=content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, Signature=abcd1234";
            context.Request.Headers["x-amz-date"] = amzDate;
            context.Request.Headers["Host"] = "lamina.openshift-adp.svc";
            context.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
            context.Request.Headers["x-amz-decoded-content-length"] = "30";
            context.Request.Headers["content-md5"] = "oxRdWklwGX760oPlOWA2ow==";
            context.Request.ContentType = "application/x-kopia";
            context.Request.ContentLength = 203;

            // Add test payload
            var testPayload = new byte[203];
            context.Request.Body = new MemoryStream(testPayload);

            var (isValid, user, error) = await _authService.ValidateRequestAsync(context.Request, "oadp", "velro/kopia/levelup/kopia.blobcfg", "PUT");

            // Should fail with signature mismatch (expected since we used dummy signature)
            // But the important thing is that streaming payload hash should not cause parsing errors
            Assert.False(isValid);
            Assert.NotNull(user);
            Assert.Equal("Invalid signature", error);

            // Verify that the service can calculate signature with streaming payload
            var sigRequest = new SignatureV4Request
            {
                Method = "PUT",
                CanonicalUri = "/oadp/velro/kopia/levelup/kopia.blobcfg",
                CanonicalQueryString = "",
                Headers = new Dictionary<string, string>
                {
                    {"content-md5", "oxRdWklwGX760oPlOWA2ow=="},
                    {"host", "lamina.openshift-adp.svc"},
                    {"x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"},
                    {"x-amz-date", amzDate},
                    {"x-amz-decoded-content-length", "30"}
                },
                Payload = testPayload,
                Region = "us-east-1",
                Service = "s3",
                RequestDateTime = dateTime,
                AccessKeyId = "TESTACCESSKEY",
                SignedHeaders = "content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length"
            };

            var calculatedSig = await _authService.CalculateSignatureV4(sigRequest, "testsecretkey");
            Assert.NotNull(calculatedSig);
            Assert.NotEmpty(calculatedSig);
        }
    }
}