using Lamina.Core.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Abstractions;
using Microsoft.AspNetCore.Routing;

namespace Lamina.WebApi.Middleware;

/// <summary>Infrastructure failures are not evidence that an S3 resource is missing.</summary>
public sealed class S3ExceptionHandlingMiddleware(RequestDelegate next, ILogger<S3ExceptionHandlingMiddleware> logger)
{
    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await next(context);
        }
        catch (Exception exception) when (exception is not OperationCanceledException and not BadHttpRequestException && !context.Response.HasStarted)
        {
            logger.LogError(exception,
                "S3 request failed: {Method} {Path}, request {RequestId}, upload {UploadId}",
                context.Request.Method, context.Request.Path, context.Items["S3RequestId"],
                context.Request.Query["uploadId"].ToString());

            context.Response.Clear();
            if (HttpMethods.IsHead(context.Request.Method))
            {
                context.Response.StatusCode = StatusCodes.Status500InternalServerError;
                context.Response.ContentType = "application/xml";
                return;
            }
            var result = new ObjectResult(new S3Error
            {
                Code = "InternalError",
                Message = "We encountered an internal error. Please try again.",
                Resource = context.Request.Path,
                RequestId = context.Items["S3RequestId"] as string ?? context.TraceIdentifier,
                HostId = context.TraceIdentifier
            })
            {
                StatusCode = StatusCodes.Status500InternalServerError,
                ContentTypes = { "application/xml" }
            };
            await result.ExecuteResultAsync(new ActionContext(context, context.GetRouteData(), new ActionDescriptor()));
        }
    }
}
