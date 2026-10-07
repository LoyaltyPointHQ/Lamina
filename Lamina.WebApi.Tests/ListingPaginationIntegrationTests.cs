using System.Net;
using System.Xml.Linq;
using Lamina.WebApi.Tests.Controllers;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Lamina.WebApi.Tests;

public sealed class ListingPaginationIntegrationTests(WebApplicationFactory<global::Program> factory) : IntegrationTestBase(factory)
{
    private static readonly XNamespace S3 = "http://s3.amazonaws.com/doc/2006-03-01/";

    private async Task<string> Bucket(bool directory)
    {
        var bucket = "listing-" + Guid.NewGuid().ToString("N");
        using var request = new HttpRequestMessage(HttpMethod.Put, "/" + bucket);
        if (directory)
            request.Headers.Add("x-amz-bucket-type", "Directory");
        (await Client.SendAsync(request)).EnsureSuccessStatusCode();
        return bucket;
    }

    private async Task Put(string bucket, string key) =>
        (await Client.PutAsync($"/{bucket}/{key}", new StringContent("data"))).EnsureSuccessStatusCode();

    private async Task Upload(string bucket, string key) =>
        (await Client.PostAsync($"/{bucket}/{key}?uploads", null)).EnsureSuccessStatusCode();

    private async Task<XElement> List(string bucket, string query)
    {
        var response = await Client.GetAsync($"/{bucket}?{query}");
        response.EnsureSuccessStatusCode();
        return XElement.Parse(await response.Content.ReadAsStringAsync());
    }

    private static string[] Items(XElement page) => page.Elements(S3 + "Contents")
        .Select(e => e.Element(S3 + "Key")!.Value)
        .Concat(page.Elements(S3 + "CommonPrefixes").Select(e => e.Element(S3 + "Prefix")!.Value)).ToArray();

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Directory_MergesMultipartBeforePaginationAndNeverRepeatsResults(int version)
    {
        var bucket = await Bucket(true);
        var expected = Enumerable.Range(0, 9).Select(i => $"wal_005/{i:D8}.lz4").ToHashSet();
        foreach (var key in expected)
            await Put(bucket, key);
        await Put(bucket, "wal_005/completed/file");
        await Upload(bucket, "wal_005/completed/unfinished");
        await Upload(bucket, "wal_005/uploads-a/one");
        await Upload(bucket, "wal_005/uploads-a/two");
        await Upload(bucket, "wal_005/uploads-b/one");
        await Upload(bucket, "wal_005/direct-upload");
        await Upload(bucket, "other/ignored/file");
        expected.UnionWith(["wal_005/completed/", "wal_005/uploads-a/", "wal_005/uploads-b/"]);

        var seen = new HashSet<string>();
        string? token = null;
        for (var n = 0; n <= expected.Count; n++)
        {
            var tokenParameter = version == 2 ? "continuation-token" : "marker";
            var query = $"list-type={version}&prefix=wal_005%2F&delimiter=%2F&max-keys=2";
            if (token != null)
                query += $"&{tokenParameter}={Uri.EscapeDataString(token)}";
            var page = await List(bucket, query);
            var items = Items(page);
            Assert.InRange(items.Length, 1, 2);
            if (version == 2)
                Assert.Equal(items.Length, (int)page.Element(S3 + "KeyCount")!);
            foreach (var item in items)
                Assert.True(seen.Add(item), "Repeated " + item);

            // A retry of the same request must give the same page for an unchanged bucket.
            Assert.Equal(items, Items(await List(bucket, query)));
            if (!(bool)page.Element(S3 + "IsTruncated")!)
            {
                Assert.Equal(expected.Order(), seen.Order());
                return;
            }
            var next = page.Element(S3 + (version == 2 ? "NextContinuationToken" : "NextMarker"))!.Value;
            Assert.StartsWith("d1:", next);
            Assert.NotEqual(token, next);
            token = next;
        }
        Assert.Fail("Directory pagination did not terminate.");
    }

    [Fact]
    public async Task Directory_TokensAreBoundToQueryAndOldTokensAreRejected()
    {
        var bucket = await Bucket(true);
        await Put(bucket, "wal_005/a");
        await Put(bucket, "wal_005/b");
        var page = await List(bucket, "list-type=2&prefix=wal_005%2F&delimiter=%2F&max-keys=1");
        var token = Uri.EscapeDataString(page.Element(S3 + "NextContinuationToken")!.Value);
        var otherBucket = await Bucket(true);
        var requests = new[]
        {
            $"/{bucket}?list-type=2&prefix=other%2F&delimiter=%2F&continuation-token={token}",
            $"/{bucket}?list-type=2&prefix=wal_005%2F&continuation-token={token}",
            $"/{otherBucket}?list-type=2&prefix=wal_005%2F&delimiter=%2F&continuation-token={token}",
            $"/{bucket}?list-type=2&continuation-token=v1:YQ%3D%3D",
            $"/{bucket}?list-type=2&continuation-token=d1:invalid",
            $"/{bucket}?list-type=2&start-after=a"
        };
        foreach (var request in requests)
        {
            var response = await Client.GetAsync(request);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidArgument", XElement.Parse(await response.Content.ReadAsStringAsync()).Element(S3 + "Code")?.Value
                ?? XElement.Parse(await response.Content.ReadAsStringAsync()).Element("Code")?.Value);
        }
        var continued = await List(bucket, $"list-type=2&prefix=wal_005%2F&delimiter=%2F&max-keys=7&continuation-token={token}");
        Assert.Single(Items(continued));
    }

    [Fact]
    public async Task GeneralPurpose_ResponseCountsBothKindsAndReportsEffectiveLimit()
    {
        var bucket = await Bucket(false);
        await Put(bucket, "a");
        await Put(bucket, "folder/file");
        var page = await List(bucket, "list-type=2&delimiter=%2F&max-keys=2000");
        Assert.Equal(1000, (int)page.Element(S3 + "MaxKeys")!);
        Assert.Equal(2, (int)page.Element(S3 + "KeyCount")!);
        Assert.False((bool)page.Element(S3 + "IsTruncated")!);

        var zero = await List(bucket, "list-type=2&max-keys=0");
        Assert.Empty(Items(zero));
        Assert.False((bool)zero.Element(S3 + "IsTruncated")!);
        Assert.Null(zero.Element(S3 + "NextContinuationToken"));
        var negative = await Client.GetAsync($"/{bucket}?list-type=2&max-keys=-1");
        Assert.Equal(HttpStatusCode.BadRequest, negative.StatusCode);
    }

    [Fact]
    public async Task Directory_MultipartCannotReintroduceInternalFilesystemPrefixes()
    {
        var bucket = await Bucket(true);
        await Put(bucket, "visible.tmp");
        await Upload(bucket, ".lamina-meta/hidden");
        await Upload(bucket, ".lamina-tmp-upload/hidden");
        await Upload(bucket, ".lamina-meta-backup/visible");
        var page = await List(bucket, "list-type=2&delimiter=%2F");
        Assert.Equal(new[] { ".lamina-meta-backup/", "visible.tmp" }, Items(page).Order(StringComparer.Ordinal));
    }
}
