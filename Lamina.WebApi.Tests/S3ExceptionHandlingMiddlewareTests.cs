using Lamina.WebApi.Middleware;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Controllers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Lamina.WebApi.Tests;

public class S3ExceptionHandlingMiddlewareTests
{
    [Fact]
    public async Task RequestScope_IsStructuredAndPrintableForConsoleDiagnostics()
    {
        var logger = new Mock<ILogger<S3ResponseHeadersMiddleware>>();
        var context = new DefaultHttpContext();
        context.Request.Method = "PUT";
        context.Request.QueryString = new QueryString("?uploadId=upload-123&partNumber=2&unrelated=excluded-value");
        context.Request.Headers["x-amz-copy-source"] = "/bucket/source";
        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask,
            new EndpointMetadataCollection(new ControllerActionDescriptor { ActionName = "UploadPart" }), "UploadPart"));
        var middleware = new S3ResponseHeadersMiddleware(_ => Task.CompletedTask, logger.Object);

        await middleware.InvokeAsync(context);

        var scope = logger.Invocations.Single(i => i.Method.Name == "BeginScope").Arguments[0];
        Assert.NotNull(scope);
        var fields = Assert.IsAssignableFrom<IEnumerable<KeyValuePair<string, object>>>(scope).ToDictionary();
        Assert.Equal(context.Items["S3RequestId"], fields["S3RequestId"]);
        Assert.Equal("UploadPartCopy", fields["S3Operation"]);
        Assert.Equal("upload-123", fields["UploadId"]);
        Assert.Contains("UploadId=upload-123", scope.ToString());
        Assert.Contains("PartNumber=2", scope.ToString());
        Assert.DoesNotContain("excluded-value", scope.ToString());
    }

    [Fact]
    public async Task ClientCancellation_IsNotReportedAsServerFailure()
    {
        var error = new OperationCanceledException();
        var middleware = new S3ExceptionHandlingMiddleware(_ => throw error,
            NullLogger<S3ExceptionHandlingMiddleware>.Instance);
        var actual = await Assert.ThrowsAsync<OperationCanceledException>(() => middleware.InvokeAsync(new DefaultHttpContext()));
        Assert.Same(error, actual);
    }

    [Theory]
    [InlineData(400)]
    [InlineData(413)]
    public async Task BadHttpRequest_PreservesTransportStatus(int status)
    {
        var error = new BadHttpRequestException("Invalid request body", status);
        var middleware = new S3ExceptionHandlingMiddleware(_ => throw error,
            NullLogger<S3ExceptionHandlingMiddleware>.Instance);
        var actual = await Assert.ThrowsAsync<BadHttpRequestException>(() => middleware.InvokeAsync(new DefaultHttpContext()));
        Assert.Same(error, actual);
        Assert.Equal(status, actual.StatusCode);
    }
}
