using System.Net;
using System.Text;
using System.Xml.Serialization;
using Lamina.Core.Models;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Lamina.WebApi.Tests.Controllers;

public class DirectoryBucketIntegrationTests : IntegrationTestBase
{
    public DirectoryBucketIntegrationTests(WebApplicationFactory<global::Program> factory) : base(factory)
    {
    }

    [Fact]
    public async Task CreateBucket_WithDirectoryType_Returns200()
    {
        var bucketName = $"directory-bucket-{Guid.NewGuid()}";

        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
        request.Headers.Add("x-amz-bucket-type", "Directory");
        request.Headers.Add("x-amz-storage-class", "EXPRESS_ONEZONE");

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("Location", response.Headers.Select(h => h.Key));
    }

    [Fact]
    public async Task CreateBucket_WithDefaultType_CreatesGeneralPurpose()
    {
        var bucketName = $"default-bucket-{Guid.NewGuid()}";

        var response = await Client.PutAsync($"/{bucketName}", null);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        // Verify bucket type via HEAD request
        var headResponse = await Client.SendAsync(new HttpRequestMessage(HttpMethod.Head, $"/{bucketName}"));
        Assert.Equal(HttpStatusCode.OK, headResponse.StatusCode);

        var bucketTypeHeader = headResponse.Headers.GetValues("x-amz-bucket-type").FirstOrDefault();
        Assert.Equal("GeneralPurpose", bucketTypeHeader);
    }

    [Fact]
    public async Task HeadBucket_DirectoryBucket_IncludesCorrectHeaders()
    {
        var bucketName = $"head-directory-{Guid.NewGuid()}";

        // Create Directory bucket
        var createRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
        createRequest.Headers.Add("x-amz-bucket-type", "Directory");
        createRequest.Headers.Add("x-amz-storage-class", "EXPRESS_ONEZONE");
        await Client.SendAsync(createRequest);

        // HEAD request
        var response = await Client.SendAsync(new HttpRequestMessage(HttpMethod.Head, $"/{bucketName}"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("Directory", response.Headers.GetValues("x-amz-bucket-type").FirstOrDefault());
        Assert.Equal("EXPRESS_ONEZONE", response.Headers.GetValues("x-amz-storage-class").FirstOrDefault());
    }

    [Fact]
    public async Task ListBuckets_IncludesBucketTypes()
    {
        var generalPurposeBucket = $"gp-bucket-{Guid.NewGuid()}";
        var directoryBucket = $"dir-bucket-{Guid.NewGuid()}";

        // Create General Purpose bucket
        await Client.PutAsync($"/{generalPurposeBucket}", null);

        // Create Directory bucket
        var directoryRequest = new HttpRequestMessage(HttpMethod.Put, $"/{directoryBucket}");
        directoryRequest.Headers.Add("x-amz-bucket-type", "Directory");
        await Client.SendAsync(directoryRequest);

        // List buckets
        var response = await Client.GetAsync("/");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var xml = await response.Content.ReadAsStringAsync();
        var serializer = new XmlSerializer(typeof(ListAllMyBucketsResult));
        using var reader = new StringReader(xml);
        var result = (ListAllMyBucketsResult)serializer.Deserialize(reader)!;

        var gpBucket = result.Buckets.FirstOrDefault(b => b.Name == generalPurposeBucket);
        var dirBucket = result.Buckets.FirstOrDefault(b => b.Name == directoryBucket);

        Assert.NotNull(gpBucket);
        Assert.NotNull(dirBucket);
        Assert.Equal("GeneralPurpose", gpBucket.BucketType);
        Assert.Equal("Directory", dirBucket.BucketType);
    }

    [Fact]
    public async Task ListObjects_DirectoryBucket_ValidatesDelimiter()
    {
        var bucketName = $"dir-delimiter-{Guid.NewGuid()}";

        // Create Directory bucket
        var createRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
        createRequest.Headers.Add("x-amz-bucket-type", "Directory");
        await Client.SendAsync(createRequest);

        // Try to list with invalid delimiter
        var response = await Client.GetAsync($"/{bucketName}?delimiter=%2C"); // comma delimiter

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var errorXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("InvalidArgument", errorXml);
        Assert.Contains("only support '/' as a delimiter", errorXml);
    }

    [Fact]
    public async Task ListObjects_DirectoryBucket_ValidatesPrefixWithDelimiter()
    {
        var bucketName = $"dir-prefix-{Guid.NewGuid()}";

        // Create Directory bucket
        var createRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
        createRequest.Headers.Add("x-amz-bucket-type", "Directory");
        await Client.SendAsync(createRequest);

        // Try to list with prefix that doesn't end with delimiter
        var response = await Client.GetAsync($"/{bucketName}?prefix=folder&delimiter=%2F"); // prefix without trailing /

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var errorXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("InvalidArgument", errorXml);
        Assert.Contains("prefixes must end with the delimiter", errorXml);
    }

    [Fact]
    public async Task ListObjects_DirectoryBucket_AllowsValidPrefixWithDelimiter()
    {
        var bucketName = $"dir-valid-prefix-{Guid.NewGuid()}";

        // Create Directory bucket
        var createRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
        createRequest.Headers.Add("x-amz-bucket-type", "Directory");
        await Client.SendAsync(createRequest);

        // List with valid prefix ending with delimiter
        var response = await Client.GetAsync($"/{bucketName}?prefix=folder%2F&delimiter=%2F"); // prefix with trailing /

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task ListObjects_DirectoryBucket_NonLexicographicalOrder()
    {
        var bucketName = $"dir-order-{Guid.NewGuid()}";

        // Create Directory bucket
        var createRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
        createRequest.Headers.Add("x-amz-bucket-type", "Directory");
        await Client.SendAsync(createRequest);

        // Create multiple objects
        var objectKeys = new[] { "a-object", "b-object", "c-object", "d-object" };
        foreach (var key in objectKeys)
        {
            await Client.PutAsync($"/{bucketName}/{key}", new StringContent("test content"));
        }

        // List objects multiple times and check if order varies
        var orders = new List<string[]>();
        for (int i = 0; i < 5; i++)
        {
            var response = await Client.GetAsync($"/{bucketName}");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var xml = await response.Content.ReadAsStringAsync();
            var serializer = new XmlSerializer(typeof(ListBucketResultV2));
            using var reader = new StringReader(xml);
            var result = (ListBucketResultV2)serializer.Deserialize(reader)!;

            var keys = result.ContentsList.Select(c => c.Key).ToArray();
            orders.Add(keys);
        }

        // Check that not all orders are the same (indicating non-lexicographical ordering)
        var firstOrder = orders[0];
        var hasVariation = orders.Any(order => !order.SequenceEqual(firstOrder));

        // Note: Due to randomization, there's a small chance all orders could be the same
        // This test might occasionally fail, but it demonstrates the non-lexicographical behavior
        Assert.True(hasVariation || orders.Count == 1, "Directory bucket objects should not always be in lexicographical order");
    }

    [Fact]
    public async Task ListObjects_GeneralPurposeBucket_AllowsAnyDelimiter()
    {
        var bucketName = $"gp-delimiter-{Guid.NewGuid()}";

        // Create General Purpose bucket (default)
        await Client.PutAsync($"/{bucketName}", null);

        // Try to list with comma delimiter (should work for General Purpose)
        var response = await Client.GetAsync($"/{bucketName}?delimiter=%2C"); // comma delimiter

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task CreateBucket_InvalidBucketType_UsesDefault()
    {
        var bucketName = $"invalid-type-{Guid.NewGuid()}";

        var request = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
        request.Headers.Add("x-amz-bucket-type", "InvalidType");

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        // Verify bucket defaults to GeneralPurpose
        var headResponse = await Client.SendAsync(new HttpRequestMessage(HttpMethod.Head, $"/{bucketName}"));
        var bucketTypeHeader = headResponse.Headers.GetValues("x-amz-bucket-type").FirstOrDefault();
        Assert.Equal("GeneralPurpose", bucketTypeHeader);
    }

    [Fact]
    public async Task ListObjects_DirectoryBucket_AllowsPrefixWithoutDelimiter()
    {
        var bucketName = $"dir-prefix-no-delimiter-{Guid.NewGuid()}";

        // Create Directory bucket
        var createRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
        createRequest.Headers.Add("x-amz-bucket-type", "Directory");
        await Client.SendAsync(createRequest);

        // List with prefix but no delimiter (should be allowed)
        var response = await Client.GetAsync($"/{bucketName}?prefix=folder");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task ListObjects_DirectoryBucket_AllowsEmptyPrefixWithDelimiter()
    {
        var bucketName = $"dir-empty-prefix-{Guid.NewGuid()}";

        // Create Directory bucket
        var createRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
        createRequest.Headers.Add("x-amz-bucket-type", "Directory");
        await Client.SendAsync(createRequest);

        // List with empty prefix and delimiter (should be allowed)
        var response = await Client.GetAsync($"/{bucketName}?delimiter=%2F"); // "/" delimiter

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task ListObjects_DirectoryBucket_AllowsValidPrefixEndingWithDelimiter()
    {
        var bucketName = $"dir-valid-prefix-delimiter-{Guid.NewGuid()}";

        // Create Directory bucket
        var createRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
        createRequest.Headers.Add("x-amz-bucket-type", "Directory");
        await Client.SendAsync(createRequest);

        // List with prefix ending with delimiter (should be allowed)
        var response = await Client.GetAsync($"/{bucketName}?prefix=folder%2F&delimiter=%2F"); // "folder/" with "/" delimiter

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task ListObjects_DirectoryBucket_RejectsMultipleSlashDelimiter()
    {
        var bucketName = $"dir-multiple-slash-{Guid.NewGuid()}";

        // Create Directory bucket
        var createRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
        createRequest.Headers.Add("x-amz-bucket-type", "Directory");
        await Client.SendAsync(createRequest);

        // Try to list with multiple slash delimiter (should fail)
        var response = await Client.GetAsync($"/{bucketName}?delimiter=%2F%2F"); // "//" delimiter

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var errorXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("InvalidArgument", errorXml);
        Assert.Contains("only support '/' as a delimiter", errorXml);
    }

    [Fact]
    public async Task ListObjects_DirectoryBucket_RejectsPipeDelimiter()
    {
        var bucketName = $"dir-pipe-delimiter-{Guid.NewGuid()}";

        // Create Directory bucket
        var createRequest = new HttpRequestMessage(HttpMethod.Put, $"/{bucketName}");
        createRequest.Headers.Add("x-amz-bucket-type", "Directory");
        await Client.SendAsync(createRequest);

        // Try to list with pipe delimiter (should fail)
        var response = await Client.GetAsync($"/{bucketName}?delimiter=%7C"); // "|" delimiter

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var errorXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("InvalidArgument", errorXml);
        Assert.Contains("only support '/' as a delimiter", errorXml);
    }

    [Fact]
    public async Task ListObjects_GeneralPurposeBucket_AllowsPrefixWithoutTrailingSlash()
    {
        var bucketName = $"gp-prefix-no-trailing-{Guid.NewGuid()}";

        // Create General Purpose bucket (default)
        await Client.PutAsync($"/{bucketName}", null);

        // List with prefix not ending with delimiter (should be allowed for GP buckets)
        var response = await Client.GetAsync($"/{bucketName}?prefix=folder&delimiter=%2F"); // "folder" with "/" delimiter

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}