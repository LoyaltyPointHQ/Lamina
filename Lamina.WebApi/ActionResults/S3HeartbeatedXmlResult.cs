using System.Text;
using System.Xml;
using System.Xml.Serialization;
using Lamina.Core.Models;
using Microsoft.AspNetCore.Mvc;

namespace Lamina.WebApi.ActionResults;

public record HeartbeatedXmlPayload(object Body, int FallbackStatusCode = 200);

public class S3HeartbeatedXmlResult : IActionResult
{
    private static readonly byte[] HeartbeatBytes = { 0x20 };
    private static readonly byte[] XmlHeaderBytes =
        Encoding.UTF8.GetBytes("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");

    private readonly Func<CancellationToken, Task<HeartbeatedXmlPayload>> _factory;
    private readonly TimeSpan _interval;
    private readonly bool _enabled;
    private readonly ILogger? _logger;

    // Set by the heartbeat loop on the first tick. Read by the main flow only after
    // heartbeatCts.Cancel() + await heartbeatTask, which gives a happens-before edge.
    private bool _xmlHeaderWritten;

    public S3HeartbeatedXmlResult(
        Func<CancellationToken, Task<HeartbeatedXmlPayload>> factory,
        TimeSpan interval,
        bool enabled,
        ILogger? logger = null)
    {
        _factory = factory;
        _interval = interval;
        _enabled = enabled;
        _logger = logger;
    }

    public async Task ExecuteResultAsync(ActionContext context)
    {
        var response = context.HttpContext.Response;
        var ct = context.HttpContext.RequestAborted;

        response.StatusCode = 200;
        response.ContentType = "application/xml";

        HeartbeatedXmlPayload payload;

        if (_enabled)
        {
            using var heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var heartbeatTask = RunHeartbeatLoopAsync(response.Body, _interval, heartbeatCts.Token);

            try
            {
                payload = await _factory(ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger?.LogError(ex, "Factory threw after heartbeat started; returning InternalError as 200+Error XML");
                payload = new HeartbeatedXmlPayload(BuildInternalError());
            }
            finally
            {
                heartbeatCts.Cancel();
                try
                {
                    await heartbeatTask;
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
        else
        {
            payload = await _factory(ct);
            if (payload.FallbackStatusCode != 200)
            {
                response.StatusCode = payload.FallbackStatusCode;
            }
        }

        // Skip XML declaration during serialization if the heartbeat loop already wrote it
        // before the first space tick — see RunHeartbeatLoopAsync. This mirrors minio's
        // sendWhiteSpace pattern and keeps the response a single well-formed XML document
        // (whitespace between prolog and root element is legal XML 1.0 Misc*).
        await SerializeXmlAsync(response.Body, payload.Body, omitXmlDeclaration: _xmlHeaderWritten, ct);
    }

    private static S3Error BuildInternalError() => new()
    {
        Code = "InternalError",
        Message = "We encountered an internal error. Please try again."
    };

    private async Task RunHeartbeatLoopAsync(Stream stream, TimeSpan interval, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, ct);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            // Pierwszy tick: emit XML declaration once, then a space. Subsequent ticks: just a space.
            // Boto3/expat (and Java SAX, .NET XmlReader) reject any bytes before the XML declaration.
            if (!_xmlHeaderWritten)
            {
                await stream.WriteAsync(XmlHeaderBytes, ct);
                _xmlHeaderWritten = true;
            }
            await stream.WriteAsync(HeartbeatBytes, ct);
            await stream.FlushAsync(ct);
        }
    }

    private static async Task SerializeXmlAsync(Stream stream, object body, bool omitXmlDeclaration, CancellationToken ct)
    {
        var settings = new XmlWriterSettings
        {
            Encoding = new UTF8Encoding(false),
            OmitXmlDeclaration = omitXmlDeclaration
        };
        var namespaces = new XmlSerializerNamespaces();
        namespaces.Add("", "");

        using var buffer = new MemoryStream();
        using (var writer = XmlWriter.Create(buffer, settings))
        {
            var serializer = new XmlSerializer(body.GetType());
            serializer.Serialize(writer, body, namespaces);
        }

        buffer.Position = 0;
        await buffer.CopyToAsync(stream, ct);
        await stream.FlushAsync(ct);
    }
}
