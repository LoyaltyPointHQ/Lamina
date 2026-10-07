using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.InMemory;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Lamina.WebApi.Tests.Controllers;

public sealed class ChunkedTransferUploadIntegrationTests : IDisposable
{
    private const string StreamingHash = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
    private static readonly XNamespace S3 = "http://s3.amazonaws.com/doc/2006-03-01/";
    private readonly string _root = Path.Combine(Path.GetTempPath(), $"lamina-chunked-http-{Guid.NewGuid():N}");
    private readonly string _accessKey = Guid.NewGuid().ToString("N");
    private readonly string _secret = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
    private readonly WebApplicationFactory<global::Program> _factory;
    private readonly HttpClient _client;

    public ChunkedTransferUploadIntegrationTests()
    {
        _factory = new WebApplicationFactory<global::Program>().WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Test");
            builder.ConfigureAppConfiguration((_, config) =>
            {
                config.Sources.Clear();
                config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["StorageType"] = "InMemory",
                    ["MetadataStorageType"] = "InMemory",
                    ["FilesystemStorage:DataDirectory"] = Path.Combine(_root, "data"),
                    ["FilesystemStorage:MetadataDirectory"] = Path.Combine(_root, "metadata"),
                    ["MultipartUploadCleanup:Enabled"] = "false",
                    ["MultipartUpload:Heartbeat:Enabled"] = "false",
                    ["LifecycleExpiration:Enabled"] = "false",
                    ["Logging:LogLevel:Default"] = "None",
                    ["Authentication:Enabled"] = "true",
                    ["Authentication:Users:0:AccessKeyId"] = _accessKey,
                    ["Authentication:Users:0:SecretAccessKey"] = _secret,
                    ["Authentication:Users:0:Name"] = "chunked-test",
                    ["Authentication:Users:0:BucketPermissions:0:BucketName"] = "*",
                    ["Authentication:Users:0:BucketPermissions:0:Permissions:0"] = "*"
                });
            });
            builder.ConfigureServices(services =>
            {
                services.AddSingleton<IObjectDataStorage, InMemoryObjectDataStorage>();
                services.AddSingleton<IBucketDataStorage, InMemoryBucketDataStorage>();
                services.AddSingleton<IMultipartUploadDataStorage, InMemoryMultipartUploadDataStorage>();
                services.AddSingleton<IObjectMetadataStorage, InMemoryObjectMetadataStorage>();
                services.AddSingleton<IBucketMetadataStorage, InMemoryBucketMetadataStorage>();
                services.AddSingleton<IMultipartUploadMetadataStorage, InMemoryMultipartUploadMetadataStorage>();
                services.Configure<MvcOptions>(options => options.Filters.Add(new ObserveTransportFilter()));
            });
        });
        _client = _factory.CreateClient();
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task SignedChunkedWithoutContentLength_StoresExactBytes(bool multipart, bool empty)
    {
        var (path, uploadPath) = await PrepareAsync(multipart);
        byte[] payload = empty ? [] : [0, 1, 0xff, 0x80, 13, 10, 42];
        using var request = SignedRequest(HttpMethod.Put, uploadPath, payload, streaming: true);
        using var response = await _client.SendAsync(request);
        AssertTransport(response);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal($"\"{Md5(payload)}\"", response.Headers.ETag?.Tag);
        await CompleteAndAssertAsync(path, uploadPath, multipart, payload);
    }

    public static TheoryData<bool, string, bool> InvalidStreams
    {
        get
        {
            var cases = new TheoryData<bool, string, bool>();
            foreach (var multipart in new[] { false, true })
            foreach (var fault in new[] { "signature", "empty-signature", "missing-terminal", "truncated-crlf", "length-short", "length-long" })
            foreach (var overwrite in new[] { false, true })
                cases.Add(multipart, fault, overwrite);
            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(InvalidStreams))]
    public async Task InvalidStream_DoesNotPublishOrReplaceData(bool multipart, string fault, bool overwrite)
    {
        var (path, uploadPath) = await PrepareAsync(multipart);
        byte[] original = "original data"u8.ToArray();
        if (overwrite)
        {
            using var initial = await SendAsync(HttpMethod.Put, uploadPath, original);
            Assert.Equal(HttpStatusCode.OK, initial.StatusCode);
        }
        var replacement = fault == "empty-signature" ? [] : "replacement"u8.ToArray();
        using var request = SignedRequest(HttpMethod.Put, uploadPath, replacement, streaming: true, fault: fault);
        using var response = await _client.SendAsync(request);
        AssertTransport(response);
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        var error = XDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("SignatureDoesNotMatch", error.Root!.Element(S3 + "Code")?.Value ?? error.Root.Element("Code")?.Value);
        if (overwrite)
        {
            await CompleteAndAssertAsync(path, uploadPath, multipart, original);
        }
        else if (multipart)
        {
            using var list = await SendAsync(HttpMethod.Get, uploadPath.Replace("partNumber=1&", ""));
            Assert.Equal(HttpStatusCode.OK, list.StatusCode);
            Assert.Empty(XDocument.Parse(await list.Content.ReadAsStringAsync()).Root!.Elements(S3 + "Part"));
        }
        else
        {
            using var get = await SendAsync(HttpMethod.Get, path);
            Assert.Equal(HttpStatusCode.NotFound, get.StatusCode);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OrdinaryChunkedWithoutContentLength_RemainsLengthRequired(bool multipart)
    {
        var (_, uploadPath) = await PrepareAsync(multipart);
        using var request = SignedRequest(HttpMethod.Put, uploadPath, "plain"u8.ToArray(), unknownLength: true);
        using var response = await _client.SendAsync(request);
        AssertTransport(response);
        Assert.Equal(HttpStatusCode.LengthRequired, response.StatusCode);
    }

    private async Task<(string Path, string UploadPath)> PrepareAsync(bool multipart)
    {
        var path = $"/chunked-{Guid.NewGuid():N}";
        using var bucket = await SendAsync(HttpMethod.Put, path);
        Assert.Equal(HttpStatusCode.OK, bucket.StatusCode);
        path += "/object";
        if (!multipart) return (path, path);
        using var initiate = await SendAsync(HttpMethod.Post, path + "?uploads=");
        Assert.Equal(HttpStatusCode.OK, initiate.StatusCode);
        var id = XDocument.Parse(await initiate.Content.ReadAsStringAsync()).Root!.Element(S3 + "UploadId")!.Value;
        return (path, $"{path}?partNumber=1&uploadId={Uri.EscapeDataString(id)}");
    }

    private async Task CompleteAndAssertAsync(string path, string uploadPath, bool multipart, byte[] expected)
    {
        if (multipart)
        {
            using var list = await SendAsync(HttpMethod.Get, uploadPath.Replace("partNumber=1&", ""));
            Assert.Equal(HttpStatusCode.OK, list.StatusCode);
            var part = Assert.Single(XDocument.Parse(await list.Content.ReadAsStringAsync()).Root!.Elements(S3 + "Part"));
            Assert.Equal(expected.Length, (int)part.Element(S3 + "Size")!);
            Assert.Equal(Md5(expected), part.Element(S3 + "ETag")!.Value.Trim('"'));
            var xml = $"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{Md5(expected)}\"</ETag></Part></CompleteMultipartUpload>";
            using var complete = await SendAsync(HttpMethod.Post, uploadPath.Replace("partNumber=1&", ""), Encoding.UTF8.GetBytes(xml));
            Assert.Equal(HttpStatusCode.OK, complete.StatusCode);
        }
        using var get = await SendAsync(HttpMethod.Get, path);
        Assert.Equal(HttpStatusCode.OK, get.StatusCode);
        Assert.Equal(expected, await get.Content.ReadAsByteArrayAsync());
    }

    private async Task<HttpResponseMessage> SendAsync(HttpMethod method, string path, byte[]? data = null)
    {
        using var request = SignedRequest(method, path, data ?? []);
        return await _client.SendAsync(request);
    }

    private HttpRequestMessage SignedRequest(HttpMethod method, string target, byte[] payload,
        bool streaming = false, string? fault = null, bool unknownLength = false)
    {
        var now = DateTime.UtcNow;
        var date = now.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        var timestamp = now.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var scope = $"{date}/us-east-1/s3/aws4_request";
        var signingKey = Hmac(Hmac(Hmac(Hmac(Encoding.UTF8.GetBytes("AWS4" + _secret), date), "us-east-1"), "s3"), "aws4_request");
        var headers = new SortedDictionary<string, string>(StringComparer.Ordinal)
        {
            ["host"] = "localhost",
            ["x-amz-date"] = timestamp,
            ["x-amz-content-sha256"] = streaming ? StreamingHash : Hash(payload)
        };
        if (streaming)
        {
            var length = payload.Length + (fault == "length-short" ? -1 : fault == "length-long" ? 1 : 0);
            headers["x-amz-decoded-content-length"] = length.ToString(CultureInfo.InvariantCulture);
        }
        var split = target.Split('?', 2);
        var signedHeaders = string.Join(';', headers.Keys);
        var canonical = $"{method}\n{split[0]}\n{(split.Length == 2 ? split[1] : "")}\n" +
                        string.Concat(headers.Select(pair => $"{pair.Key}:{pair.Value}\n")) +
                        $"\n{signedHeaders}\n{headers["x-amz-content-sha256"]}";
        var seed = Hex(Hmac(signingKey, $"AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{Hash(Encoding.UTF8.GetBytes(canonical))}"));
        var request = new HttpRequestMessage(method, target) { Version = HttpVersion.Version11 };
        foreach (var (name, value) in headers) request.Headers.TryAddWithoutValidation(name, value);
        request.Headers.TryAddWithoutValidation("Authorization", $"AWS4-HMAC-SHA256 Credential={_accessKey}/{scope}, SignedHeaders={signedHeaders}, Signature={seed}");
        if (streaming)
        {
            using var body = new MemoryStream();
            var previous = seed;
            void Chunk(byte[] bytes, bool terminal)
            {
                var signature = Hex(Hmac(signingKey, $"AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{scope}\n{previous}\n{Hash([])}\n{Hash(bytes)}"));
                previous = signature;
                if (terminal && fault is "signature" or "empty-signature") signature = new string('0', 64);
                body.Write(Encoding.ASCII.GetBytes($"{bytes.Length:x};chunk-signature={signature}\r\n"));
                body.Write(bytes);
                body.Write("\r\n"u8);
            }
            if (payload.Length > 0) Chunk(payload, false);
            if (fault != "missing-terminal") Chunk([], true);
            payload = body.ToArray();
            if (fault == "truncated-crlf") payload = payload[..^1];
        }
        if (streaming || unknownLength)
        {
            request.Content = new UnknownLengthContent(payload);
            request.Headers.TransferEncodingChunked = true;
            request.Headers.Add("x-lamina-test-observe-transport", "true");
        }
        else request.Content = new ByteArrayContent(payload);
        return request;
    }

    private static void AssertTransport(HttpResponseMessage response)
    {
        Assert.Equal("true", Assert.Single(response.Headers.GetValues("x-test-content-length-null")));
        Assert.Equal("false", Assert.Single(response.Headers.GetValues("x-test-has-content-length")));
        Assert.Equal("chunked", Assert.Single(response.Headers.GetValues("x-test-transfer-encoding")));
    }

    private sealed class ObserveTransportFilter : IResourceFilter
    {
        public void OnResourceExecuting(ResourceExecutingContext context)
        {
            var request = context.HttpContext.Request;
            if (!request.Headers.ContainsKey("x-lamina-test-observe-transport")) return;
            var headers = context.HttpContext.Response.Headers;
            headers["x-test-content-length-null"] = request.ContentLength is null ? "true" : "false";
            headers["x-test-has-content-length"] = request.Headers.ContainsKey("Content-Length") ? "true" : "false";
            headers["x-test-transfer-encoding"] = request.Headers.TransferEncoding.ToString();
        }
        public void OnResourceExecuted(ResourceExecutedContext context) { }
    }

    private sealed class UnknownLengthContent(byte[] bytes) : HttpContent
    {
        protected override bool TryComputeLength(out long length) { length = 0; return false; }
        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) => stream.WriteAsync(bytes).AsTask();
    }

    private static byte[] Hmac(byte[] key, string text) => HMACSHA256.HashData(key, Encoding.UTF8.GetBytes(text));
    private static string Hash(byte[] bytes) => Hex(SHA256.HashData(bytes));
    private static string Md5(byte[] bytes) => Hex(MD5.HashData(bytes));
    private static string Hex(byte[] bytes) => Convert.ToHexString(bytes).ToLowerInvariant();

    public void Dispose()
    {
        _client.Dispose();
        _factory.Dispose();
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }
}
