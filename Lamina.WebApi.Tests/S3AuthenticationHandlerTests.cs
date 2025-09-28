using System.Security.Claims;
using System.Text.Encodings.Web;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.WebApi.Authentication;
using Lamina.WebApi.Streaming;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using IAuthenticationService = Lamina.WebApi.Services.IAuthenticationService;

namespace Lamina.WebApi.Tests.Authentication
{
    public class S3AuthenticationHandlerTests
    {
        private readonly Mock<IOptionsMonitor<S3AuthenticationOptions>> _optionsMock;
        private readonly Mock<ILoggerFactory> _loggerFactoryMock;
        private readonly UrlEncoder _urlEncoder;
        private readonly Mock<IAuthenticationService> _authServiceMock;
        private readonly Mock<IStreamingAuthenticationService> _streamingAuthServiceMock;
        private readonly Mock<IOptions<AuthenticationSettings>> _authSettingsMock;
        private readonly Mock<ILogger<S3AuthenticationHandler>> _loggerMock;

        private readonly S3AuthenticationOptions _options;
        private readonly AuthenticationSettings _authSettings;
        private readonly DefaultHttpContext _httpContext;

        public S3AuthenticationHandlerTests()
        {
            _optionsMock = new Mock<IOptionsMonitor<S3AuthenticationOptions>>();
            _loggerFactoryMock = new Mock<ILoggerFactory>();
            _urlEncoder = UrlEncoder.Default;
            _authServiceMock = new Mock<IAuthenticationService>();
            _streamingAuthServiceMock = new Mock<IStreamingAuthenticationService>();
            _authSettingsMock = new Mock<IOptions<AuthenticationSettings>>();
            _loggerMock = new Mock<ILogger<S3AuthenticationHandler>>();

            _options = new S3AuthenticationOptions();
            _authSettings = new AuthenticationSettings { Enabled = true };

            _optionsMock.Setup(x => x.CurrentValue).Returns(_options);
            _optionsMock.Setup(x => x.Get(It.IsAny<string>())).Returns(_options);
            _authSettingsMock.Setup(x => x.Value).Returns(_authSettings);
            _loggerFactoryMock.Setup(x => x.CreateLogger(It.IsAny<string>())).Returns(_loggerMock.Object);

            _httpContext = new DefaultHttpContext();
            _httpContext.Request.Scheme = "https";
            _httpContext.Request.Host = new HostString("localhost");
            _httpContext.Request.Path = "/test-bucket";
            _httpContext.Request.Method = "GET";
        }

        private S3AuthenticationHandler CreateHandler()
        {
            var handler = new S3AuthenticationHandler(
                _optionsMock.Object,
                _loggerFactoryMock.Object,
                _urlEncoder,
                _authServiceMock.Object,
                _streamingAuthServiceMock.Object,
                _authSettingsMock.Object);

            var scheme = new AuthenticationScheme(S3AuthenticationDefaults.AuthenticationScheme, null, typeof(S3AuthenticationHandler));
            
            // Initialize the handler properly
            var initTask = handler.InitializeAsync(scheme, _httpContext);
            initTask.Wait();
            
            return handler;
        }

        [Fact]
        public async Task HandleAuthenticateAsync_HealthCheckPath_ReturnsNoResult()
        {
            // Arrange
            _httpContext.Request.Path = "/health";
            var handler = CreateHandler();

            // Act
            var result = await handler.AuthenticateAsync();

            // Assert
            Assert.Equal(AuthenticateResult.NoResult().None, result.None);
        }

        [Fact]
        public async Task HandleAuthenticateAsync_AuthenticationDisabled_ReturnsSuccessWithAnonymousUser()
        {
            // Arrange
            _authSettings.Enabled = false;
            var handler = CreateHandler();

            // Act
            var result = await handler.AuthenticateAsync();

            // Assert
            Assert.True(result.Succeeded);
            Assert.True(result.Principal.HasClaim(ClaimTypes.Anonymous, "true"));
        }

        [Fact]
        public async Task HandleAuthenticateAsync_ValidRequest_CallsAuthenticationService()
        {
            // Arrange
            var testUser = new S3User
            {
                AccessKeyId = "testkey",
                Name = "testuser",
                BucketPermissions = new List<BucketPermission>()
            };

            _authServiceMock
                .Setup(x => x.ValidateRequestAsync(It.IsAny<HttpRequest>(), "test-bucket", null, "GET"))
                .ReturnsAsync((true, testUser, null));

            var handler = CreateHandler();

            // Act
            var result = await handler.AuthenticateAsync();

            // Assert
            Assert.True(result.Succeeded);
            Assert.Equal("testkey", result.Principal.FindFirst(ClaimTypes.NameIdentifier)?.Value);
            Assert.Equal("testuser", result.Principal.FindFirst(ClaimTypes.Name)?.Value);
            _authServiceMock.Verify(x => x.ValidateRequestAsync(It.IsAny<HttpRequest>(), "test-bucket", null, "GET"), Times.Once);
        }

        [Fact]
        public async Task HandleAuthenticateAsync_InvalidRequest_ReturnsFailure()
        {
            // Arrange
            _authServiceMock
                .Setup(x => x.ValidateRequestAsync(It.IsAny<HttpRequest>(), "test-bucket", null, "GET"))
                .ReturnsAsync((false, null, "Invalid signature"));

            var handler = CreateHandler();

            // Act
            var result = await handler.AuthenticateAsync();

            // Assert
            Assert.False(result.Succeeded);
            Assert.Contains("Invalid signature", result.Failure?.Message ?? "");
        }

        [Fact]
        public async Task HandleAuthenticateAsync_StreamingRequest_SetsUpChunkValidator()
        {
            // Arrange
            var testUser = new S3User
            {
                AccessKeyId = "testkey",
                Name = "testuser",
                BucketPermissions = new List<BucketPermission>()
            };

            _httpContext.Request.Headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

            _authServiceMock
                .Setup(x => x.ValidateRequestAsync(It.IsAny<HttpRequest>(), "test-bucket", null, "GET"))
                .ReturnsAsync((true, testUser, null));

            var mockValidator = new Mock<IChunkSignatureValidator>();
            _streamingAuthServiceMock
                .Setup(x => x.CreateChunkValidator(It.IsAny<HttpRequest>(), testUser))
                .Returns(mockValidator.Object);

            var handler = CreateHandler();

            // Act
            var result = await handler.AuthenticateAsync();

            // Assert
            Assert.True(result.Succeeded);
            Assert.Equal(mockValidator.Object, _httpContext.Items["ChunkValidator"]);
            _streamingAuthServiceMock.Verify(x => x.CreateChunkValidator(It.IsAny<HttpRequest>(), testUser), Times.Once);
        }

        [Fact]
        public async Task HandleAuthenticateAsync_ListBucketsOperation_ExtractsEmptyBucketName()
        {
            // Arrange
            _httpContext.Request.Path = "/";
            var testUser = new S3User
            {
                AccessKeyId = "testkey",
                Name = "testuser",
                BucketPermissions = new List<BucketPermission>()
            };

            _authServiceMock
                .Setup(x => x.ValidateRequestAsync(It.IsAny<HttpRequest>(), "", null, "GET"))
                .ReturnsAsync((true, testUser, null));

            var handler = CreateHandler();

            // Act
            var result = await handler.AuthenticateAsync();

            // Assert
            Assert.True(result.Succeeded);
            _authServiceMock.Verify(x => x.ValidateRequestAsync(It.IsAny<HttpRequest>(), "", null, "GET"), Times.Once);
        }

        [Fact]
        public async Task HandleAuthenticateAsync_ObjectOperation_ExtractsBucketAndObjectKey()
        {
            // Arrange
            _httpContext.Request.Path = "/test-bucket/path/to/object.txt";
            var testUser = new S3User
            {
                AccessKeyId = "testkey",
                Name = "testuser",
                BucketPermissions = new List<BucketPermission>()
            };

            _authServiceMock
                .Setup(x => x.ValidateRequestAsync(It.IsAny<HttpRequest>(), "test-bucket", "path/to/object.txt", "GET"))
                .ReturnsAsync((true, testUser, null));

            var handler = CreateHandler();

            // Act
            var result = await handler.AuthenticateAsync();

            // Assert
            Assert.True(result.Succeeded);
            _authServiceMock.Verify(x => x.ValidateRequestAsync(It.IsAny<HttpRequest>(), "test-bucket", "path/to/object.txt", "GET"), Times.Once);
        }

        [Fact]
        public async Task HandleAuthenticateAsync_Exception_ReturnsFailure()
        {
            // Arrange
            _authServiceMock
                .Setup(x => x.ValidateRequestAsync(It.IsAny<HttpRequest>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
                .ThrowsAsync(new Exception("Test exception"));

            var handler = CreateHandler();

            // Act
            var result = await handler.AuthenticateAsync();

            // Assert
            Assert.False(result.Succeeded);
            Assert.Contains("Authentication error", result.Failure?.Message ?? "");
        }
    }
}