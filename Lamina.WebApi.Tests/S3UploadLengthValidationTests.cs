using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.WebApi.Authentication;
using Lamina.WebApi.Controllers.Base;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Primitives;
using Moq;

namespace Lamina.WebApi.Tests;

public class S3UploadLengthValidationTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(12)]
    public void KnownContentLength_IsAccepted(long length)
    {
        var controller = CreateController();
        controller.Request.ContentLength = length;
        Assert.Null(controller.Validate());
    }

    [Theory]
    [InlineData(0, "chunked")]
    [InlineData(12, "CHUNKED")]
    public void SignedStreaming_WithDecodedLengthAndChunkedTransport_IsAccepted(long length, string transferEncoding)
    {
        var controller = CreateController();
        ConfigureStreaming(controller, length);
        controller.Request.Headers.TransferEncoding = transferEncoding;
        Assert.Null(controller.Request.ContentLength);
        Assert.Null(controller.Validate());
    }

    [Theory]
    [InlineData("no-validator")]
    [InlineData("no-transfer-encoding")]
    [InlineData("identity")]
    [InlineData("no-payload-marker")]
    [InlineData("unsigned-payload")]
    [InlineData("signed-trailer")]
    [InlineData("unsigned-trailer")]
    [InlineData("validator-expects-trailers")]
    [InlineData("missing-length")]
    [InlineData("negative-length")]
    [InlineData("invalid-length")]
    [InlineData("overflow-length")]
    [InlineData("duplicate-length")]
    [InlineData("mismatched-length")]
    public void MissingContentLength_WithoutQualifiedStreaming_IsRejected(string scenario)
    {
        var controller = CreateController();
        var validator = ConfigureStreaming(controller, 0);
        switch (scenario)
        {
            case "no-validator": controller.HttpContext.Items.Remove("ChunkValidator"); break;
            case "no-transfer-encoding": controller.Request.Headers.Remove("Transfer-Encoding"); break;
            case "identity": controller.Request.Headers.TransferEncoding = "identity"; break;
            case "no-payload-marker": controller.Request.Headers.Remove("x-amz-content-sha256"); break;
            case "unsigned-payload": controller.Request.Headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"; break;
            case "signed-trailer": controller.Request.Headers["x-amz-content-sha256"] = S3AuthenticationDefaults.StreamingPayloadTrailer; break;
            case "unsigned-trailer": controller.Request.Headers["x-amz-content-sha256"] = S3AuthenticationDefaults.StreamingUnsignedPayloadTrailer; break;
            case "validator-expects-trailers": validator.SetupGet(v => v.ExpectsTrailers).Returns(true); break;
            case "missing-length": controller.Request.Headers.Remove("x-amz-decoded-content-length"); break;
            case "negative-length": controller.Request.Headers["x-amz-decoded-content-length"] = "-1"; break;
            case "invalid-length": controller.Request.Headers["x-amz-decoded-content-length"] = "invalid"; break;
            case "overflow-length": controller.Request.Headers["x-amz-decoded-content-length"] = "9223372036854775808"; break;
            case "duplicate-length": controller.Request.Headers["x-amz-decoded-content-length"] = new StringValues(["0", "0"]); break;
            case "mismatched-length": controller.Request.Headers["x-amz-decoded-content-length"] = "1"; break;
        }

        var result = Assert.IsType<ObjectResult>(controller.Validate());
        Assert.Equal(411, controller.Response.StatusCode);
        Assert.Equal("MissingContentLength", Assert.IsType<S3Error>(result.Value).Code);
    }

    private static Mock<IChunkSignatureValidator> ConfigureStreaming(TestController controller, long length)
    {
        controller.Request.Headers.TransferEncoding = "chunked";
        controller.Request.Headers["x-amz-content-sha256"] = S3AuthenticationDefaults.StreamingPayload;
        controller.Request.Headers["x-amz-decoded-content-length"] = length.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var validator = new Mock<IChunkSignatureValidator>();
        validator.SetupGet(v => v.ExpectedDecodedLength).Returns(length);
        controller.HttpContext.Items["ChunkValidator"] = validator.Object;
        return validator;
    }

    private static TestController CreateController() => new()
    {
        ControllerContext = new ControllerContext { HttpContext = new DefaultHttpContext() }
    };

    private sealed class TestController : S3ControllerBase
    {
        public IActionResult? Validate() => ValidateContentLengthHeader("/bucket/object");
    }
}
