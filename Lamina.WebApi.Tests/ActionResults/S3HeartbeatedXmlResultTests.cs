using System.Text;
using System.Xml.Serialization;
using Lamina.Core.Models;
using Lamina.WebApi.ActionResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Abstractions;
using Microsoft.AspNetCore.Routing;

namespace Lamina.WebApi.Tests.ActionResults;

public class S3HeartbeatedXmlResultTests
{
    [XmlRoot("TestPayload")]
    public class TestPayload
    {
        [XmlElement("Value")]
        public string Value { get; set; } = string.Empty;
    }

    private static (ActionContext Context, MemoryStream Body) CreateContext()
    {
        var httpContext = new DefaultHttpContext();
        var body = new MemoryStream();
        httpContext.Response.Body = body;
        var actionContext = new ActionContext(
            httpContext,
            new RouteData(),
            new ActionDescriptor());
        return (actionContext, body);
    }

    private static string ReadResponse(MemoryStream body)
    {
        body.Position = 0;
        return Encoding.UTF8.GetString(body.ToArray());
    }

    [Fact]
    public async Task ExecuteAsync_FactoryReturnsImmediately_WritesXmlBodyAndStatus200()
    {
        var (context, body) = CreateContext();

        var payload = new TestPayload { Value = "hello" };
        var sut = new S3HeartbeatedXmlResult(
            _ => Task.FromResult(new HeartbeatedXmlPayload(payload)),
            interval: TimeSpan.FromSeconds(5),
            enabled: false);

        await sut.ExecuteResultAsync(context);

        Assert.Equal(200, context.HttpContext.Response.StatusCode);
        Assert.Equal("application/xml", context.HttpContext.Response.ContentType);

        var responseText = ReadResponse(body);
        Assert.Contains("<TestPayload", responseText);
        Assert.Contains("<Value>hello</Value>", responseText);
    }

    [Fact]
    public async Task ExecuteAsync_HeartbeatDisabled_NoLeadingWhitespace()
    {
        var (context, body) = CreateContext();

        var payload = new TestPayload { Value = "quiet" };
        var sut = new S3HeartbeatedXmlResult(
            async ct =>
            {
                await Task.Delay(TimeSpan.FromMilliseconds(200), ct);
                return new HeartbeatedXmlPayload(payload);
            },
            interval: TimeSpan.FromMilliseconds(50),
            enabled: false);

        await sut.ExecuteResultAsync(context);

        var responseText = ReadResponse(body);
        var trimmed = responseText.TrimStart('\uFEFF');
        Assert.StartsWith("<?xml", trimmed);
        Assert.DoesNotContain("  ", trimmed[..30]);
    }

    [Fact]
    public async Task ExecuteAsync_FactoryThrows_HeartbeatDisabled_RethrowsForGlobalHandler()
    {
        var (context, _) = CreateContext();

        var sut = new S3HeartbeatedXmlResult(
            _ => throw new InvalidOperationException("nope"),
            interval: TimeSpan.FromSeconds(5),
            enabled: false);

        await Assert.ThrowsAsync<InvalidOperationException>(() => sut.ExecuteResultAsync(context));
    }

    [Fact]
    public async Task ExecuteAsync_FactoryThrows_AfterHeartbeatStart_WritesInternalErrorXml()
    {
        var (context, body) = CreateContext();

        var sut = new S3HeartbeatedXmlResult(
            async ct =>
            {
                await Task.Delay(TimeSpan.FromMilliseconds(150), ct);
                throw new InvalidOperationException("boom");
            },
            interval: TimeSpan.FromMilliseconds(50),
            enabled: true);

        await sut.ExecuteResultAsync(context);

        Assert.Equal(200, context.HttpContext.Response.StatusCode);

        var responseText = ReadResponse(body);
        Assert.StartsWith("<?xml", responseText);

        var endOfDecl = responseText.IndexOf("?>", StringComparison.Ordinal);
        var afterDecl = responseText[(endOfDecl + 2)..];
        var bodyStart = afterDecl.TrimStart(' ', '\n', '\r');
        Assert.StartsWith("<Error", bodyStart);
        Assert.Contains("<Code>InternalError</Code>", bodyStart);
    }

    [Fact]
    public async Task ExecuteAsync_FactoryReturnsErrorPayload_WritesErrorXmlWithStatus200()
    {
        var (context, body) = CreateContext();

        var error = new S3Error
        {
            Code = "InvalidPart",
            Message = "Part 3 ETag mismatch",
            Resource = "bucket/key"
        };
        var sut = new S3HeartbeatedXmlResult(
            async ct =>
            {
                await Task.Delay(TimeSpan.FromMilliseconds(150), ct);
                return new HeartbeatedXmlPayload(error, FallbackStatusCode: 400);
            },
            interval: TimeSpan.FromMilliseconds(50),
            enabled: true);

        await sut.ExecuteResultAsync(context);

        Assert.Equal(200, context.HttpContext.Response.StatusCode);

        var responseText = ReadResponse(body);
        Assert.StartsWith("<?xml", responseText);

        var endOfDecl = responseText.IndexOf("?>", StringComparison.Ordinal);
        var afterDecl = responseText[(endOfDecl + 2)..];
        var bodyStart = afterDecl.TrimStart(' ', '\n', '\r');
        Assert.StartsWith("<Error", bodyStart);
        Assert.Contains("<Code>InvalidPart</Code>", bodyStart);
    }

    [Fact]
    public async Task ExecuteAsync_FastFactory_HeartbeatNotTickedYet_FullSerialization()
    {
        var (context, body) = CreateContext();

        var payload = new TestPayload { Value = "fast" };
        var sut = new S3HeartbeatedXmlResult(
            _ => Task.FromResult(new HeartbeatedXmlPayload(payload)),
            interval: TimeSpan.FromSeconds(10),
            enabled: true);

        await sut.ExecuteResultAsync(context);

        var responseText = ReadResponse(body);

        // Heartbeat enabled but factory finished before first tick (10s away):
        // expect a full standalone XML document with declaration, no leading whitespace,
        // no whitespace between declaration and root element.
        Assert.StartsWith("<?xml", responseText);
        var endOfDecl = responseText.IndexOf("?>", StringComparison.Ordinal);
        var afterDecl = responseText[(endOfDecl + 2)..];
        Assert.StartsWith("<TestPayload", afterDecl);
        Assert.Contains("<Value>fast</Value>", responseText);
    }

    [Fact]
    public async Task ExecuteAsync_SlowFactory_WritesXmlHeaderThenWhitespaceThenBody()
    {
        var (context, body) = CreateContext();

        var payload = new TestPayload { Value = "world" };
        var sut = new S3HeartbeatedXmlResult(
            async ct =>
            {
                await Task.Delay(TimeSpan.FromMilliseconds(300), ct);
                return new HeartbeatedXmlPayload(payload);
            },
            interval: TimeSpan.FromMilliseconds(50),
            enabled: true);

        await sut.ExecuteResultAsync(context);

        var responseText = ReadResponse(body);

        // Format wzorowany na minio sendWhiteSpace: XML declaration najpierw, potem spacje (między
        // prologiem a root elementem - dozwolone XML 1.0), potem body bez XML decl. To akceptują
        // expat (boto3), Java SAX, .NET XmlReader. Whitespace PRZED <?xml jest niedozwolone.
        Assert.StartsWith("<?xml", responseText);

        var endOfDecl = responseText.IndexOf("?>", StringComparison.Ordinal);
        Assert.True(endOfDecl > 0, "Expected closing '?>' of XML declaration");

        var afterDecl = responseText[(endOfDecl + 2)..];
        var leadingSpaces = afterDecl.TakeWhile(c => c == ' ' || c == '\n' || c == '\r').Count();
        Assert.True(leadingSpaces >= 3,
            $"Expected ≥3 whitespace bytes between XML declaration and root element, got {leadingSpaces}. After decl: '{afterDecl[..Math.Min(30, afterDecl.Length)]}'");

        var bodyStart = afterDecl.TrimStart(' ', '\n', '\r');
        Assert.StartsWith("<TestPayload", bodyStart);
        Assert.Contains("<Value>world</Value>", bodyStart);
    }
}
