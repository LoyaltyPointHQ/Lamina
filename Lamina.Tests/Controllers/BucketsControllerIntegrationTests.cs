using System.Net;
using System.Text;
using System.Xml.Serialization;
using Microsoft.AspNetCore.Mvc.Testing;
using Lamina.Models;

namespace Lamina.Tests.Controllers;

public class BucketsControllerIntegrationTests : IntegrationTestBase
{
    public BucketsControllerIntegrationTests(WebApplicationFactory<Program> factory) : base(factory)
    {
    }

    [Fact]
    public async Task CreateBucket_ValidRequest_Returns200()
    {
        var bucketName = $"test-bucket-{Guid.NewGuid()}";

        var response = await Client.PutAsync($"/{bucketName}", null);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("Location", response.Headers.Select(h => h.Key));
    }

    [Fact]
    public async Task CreateBucket_DuplicateName_Returns409()
    {
        var bucketName = $"duplicate-{Guid.NewGuid()}";

        var response1 = await Client.PutAsync($"/{bucketName}", null);
        var response2 = await Client.PutAsync($"/{bucketName}", null);

        Assert.Equal(HttpStatusCode.OK, response1.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, response2.StatusCode);

        var errorXml = await response2.Content.ReadAsStringAsync();
        Assert.Contains("BucketAlreadyExists", errorXml);
        Assert.Equal("application/xml", response2.Content.Headers.ContentType?.MediaType);
    }

    [Fact]
    public async Task ListObjects_ExistingBucket_Returns200()
    {
        var bucketName = $"get-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);

        var response = await Client.GetAsync($"/{bucketName}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var xmlContent = await response.Content.ReadAsStringAsync();
        Assert.Contains("ListBucketResult", xmlContent);
        Assert.Contains(bucketName, xmlContent);
    }

    [Fact]
    public async Task ListObjects_NonExistingBucket_Returns404()
    {
        var response = await Client.GetAsync($"/non-existing-{Guid.NewGuid()}");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var errorXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("NoSuchBucket", errorXml);
    }

    [Fact]
    public async Task ListBuckets_ReturnsAllBuckets()
    {
        var bucket1 = $"list-test-1-{Guid.NewGuid()}";
        var bucket2 = $"list-test-2-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucket1}", null);
        await Client.PutAsync($"/{bucket2}", null);

        var response = await Client.GetAsync("/");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var xmlContent = await response.Content.ReadAsStringAsync();
        Assert.Contains("ListAllMyBucketsResult", xmlContent);
        Assert.Contains(bucket1, xmlContent);
        Assert.Contains(bucket2, xmlContent);

        var serializer = new XmlSerializer(typeof(ListAllMyBucketsResult));
        using var reader = new StringReader(xmlContent);
        var result = (ListAllMyBucketsResult?)serializer.Deserialize(reader);

        Assert.NotNull(result);
        Assert.NotNull(result.Owner);
        Assert.Contains(result.Buckets, b => b.Name == bucket1);
        Assert.Contains(result.Buckets, b => b.Name == bucket2);
    }

    [Fact]
    public async Task GetBucketLocation_ExistingBucket_ReturnsEmptyLocationConstraint()
    {
        // Arrange: Create a bucket to test with
        var bucketName = $"location-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);

        // Act: Request bucket location
        var response = await Client.GetAsync($"/{bucketName}?location");

        // Assert: Should return 200 with empty LocationConstraint
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var xmlContent = await response.Content.ReadAsStringAsync();
        
        // Per S3 API specification, GetBucketLocation for us-east-1 returns empty/null value
        // According to AWS docs: "If the bucket is in the US East (N. Virginia) region,
        // Amazon S3 returns null for the bucket's region"
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
        
        // Lamina operates as a single-region system (documented in README.md under "Known S3 API Limitations")
        // This implementation simulates us-east-1 behavior by returning empty LocationConstraint
        Assert.Contains("LocationConstraint", xmlContent);
        
        // Deserialize and verify the structure
        var serializer = new XmlSerializer(typeof(LocationConstraintResult));
        using var reader = new StringReader(xmlContent);
        var result = (LocationConstraintResult?)serializer.Deserialize(reader);
        
        Assert.NotNull(result);
        // Region should be null/empty for us-east-1 compatibility
        Assert.True(string.IsNullOrEmpty(result.Region));
    }

    [Fact]
    public async Task GetBucketLocation_NonExistingBucket_Returns404()
    {
        // Arrange: Use a non-existing bucket name
        var bucketName = $"non-existing-location-{Guid.NewGuid()}";

        // Act: Request location for non-existing bucket
        var response = await Client.GetAsync($"/{bucketName}?location");

        // Assert: Should return 404 with NoSuchBucket error
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var errorXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("NoSuchBucket", errorXml);
        Assert.Contains("The specified bucket does not exist", errorXml);
        
        // Verify error structure per S3 spec
        var serializer = new XmlSerializer(typeof(S3Error));
        using var reader = new StringReader(errorXml);
        var error = (S3Error?)serializer.Deserialize(reader);
        
        Assert.NotNull(error);
        Assert.Equal("NoSuchBucket", error.Code);
        Assert.Equal(bucketName, error.Resource);
    }

    [Fact]
    public async Task GetBucketLocation_WithEmptyLocationParameter_ReturnsEmptyLocationConstraint()
    {
        // Arrange: Create a bucket to test with
        var bucketName = $"location-empty-param-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);

        // Act: Request bucket location with empty location parameter (both ?location and ?location= should work)
        var response1 = await Client.GetAsync($"/{bucketName}?location");
        var response2 = await Client.GetAsync($"/{bucketName}?location=");

        // Assert: Both requests should return the same result
        Assert.Equal(HttpStatusCode.OK, response1.StatusCode);
        Assert.Equal(HttpStatusCode.OK, response2.StatusCode);

        var xmlContent1 = await response1.Content.ReadAsStringAsync();
        var xmlContent2 = await response2.Content.ReadAsStringAsync();
        
        // Both should contain LocationConstraint
        Assert.Contains("LocationConstraint", xmlContent1);
        Assert.Contains("LocationConstraint", xmlContent2);
        
        // Content should be essentially the same (empty LocationConstraint)
        Assert.Equal(xmlContent1, xmlContent2);
    }

    [Fact]
    public async Task DeleteBucket_ExistingBucket_Returns204()
    {
        var bucketName = $"delete-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);

        var response = await Client.DeleteAsync($"/{bucketName}");

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);

        var getResponse = await Client.GetAsync($"/{bucketName}");
        Assert.Equal(HttpStatusCode.NotFound, getResponse.StatusCode);
    }

    [Fact]
    public async Task DeleteBucket_NonExistingBucket_Returns404()
    {
        var response = await Client.DeleteAsync($"/non-existing-delete-{Guid.NewGuid()}");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);

        var errorXml = await response.Content.ReadAsStringAsync();
        Assert.Contains("NoSuchBucket", errorXml);
    }

    [Fact]
    public async Task DeleteBucket_BucketWithFiles_Returns204()
    {
        var bucketName = $"delete-with-files-{Guid.NewGuid()}";

        // Create bucket
        await Client.PutAsync($"/{bucketName}", null);

        // Add one simple file to the bucket
        var putResponse = await Client.PutAsync($"/{bucketName}/file1.txt", new StringContent("content1"));
        Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);

        // Delete bucket (should delete all files)
        var deleteResponse = await Client.DeleteAsync($"/{bucketName}");
        Assert.Equal(HttpStatusCode.NoContent, deleteResponse.StatusCode);

        // Verify bucket is gone
        var bucketResponse = await Client.GetAsync($"/{bucketName}");
        Assert.Equal(HttpStatusCode.NotFound, bucketResponse.StatusCode);
    }

    [Fact]
    public async Task HeadBucket_ExistingBucket_Returns200()
    {
        var bucketName = $"head-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);

        var request = new HttpRequestMessage(HttpMethod.Head, $"/{bucketName}");
        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task HeadBucket_NonExistingBucket_Returns404()
    {
        var request = new HttpRequestMessage(HttpMethod.Head, $"/non-existing-head-{Guid.NewGuid()}");
        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    // Note: Bucket tagging is not part of core S3 XML API spec
    // This test is removed as S3 controllers now follow XML response format

    // Note: Bucket tagging is not part of core S3 XML API spec
    // This test is removed as S3 controllers now follow XML response format

    [Fact]
    public async Task ListObjectsV2_WithEncodingTypeUrl_EncodesSpecialCharacters()
    {
        // Arrange
        var bucketName = $"encoding-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);

        // Create objects with special characters
        var objectsWithSpecialChars = new[]
        {
            "file with spaces.txt",
            "file[brackets].txt",
            "file(parentheses).txt",
            "file%percent.txt",
            "ąćęłńóśźż.txt",
            "文件.txt"
        };

        foreach (var objectKey in objectsWithSpecialChars)
        {
            var response = await Client.PutAsync($"/{bucketName}/{Uri.EscapeDataString(objectKey)}", 
                new StringContent("test content"));
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        // Act - List objects with encoding-type=url
        var listResponse = await Client.GetAsync($"/{bucketName}?list-type=2&encoding-type=url");

        // Assert
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var xmlContent = await listResponse.Content.ReadAsStringAsync();
        
        // Check that EncodingType is present in response
        Assert.Contains("<EncodingType>url</EncodingType>", xmlContent);
        
        // Check that special characters are URL encoded
        Assert.Contains("file%20with%20spaces.txt", xmlContent);
        Assert.Contains("file%5Bbrackets%5D.txt", xmlContent);
        Assert.Contains("file%28parentheses%29.txt", xmlContent);
        Assert.Contains("file%25percent.txt", xmlContent);
        Assert.Contains("%C4%85%C4%87%C4%99%C5%82%C5%84%C3%B3%C5%9B%C5%BA%C5%BC.txt", xmlContent);
        Assert.Contains("%E6%96%87%E4%BB%B6.txt", xmlContent);
        
        // Ensure non-encoded versions are NOT present
        Assert.DoesNotContain(">file with spaces.txt<", xmlContent);
        Assert.DoesNotContain(">file[brackets].txt<", xmlContent);
    }

    [Fact]
    public async Task ListObjectsV2_WithoutEncodingType_DoesNotEncodeCharacters()
    {
        // Arrange
        var bucketName = $"no-encoding-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);

        var objectKey = "file with spaces.txt";
        await Client.PutAsync($"/{bucketName}/{Uri.EscapeDataString(objectKey)}", 
            new StringContent("test content"));

        // Act - List objects without encoding-type
        var listResponse = await Client.GetAsync($"/{bucketName}?list-type=2");

        // Assert
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var xmlContent = await listResponse.Content.ReadAsStringAsync();
        
        // EncodingType should not be present
        Assert.DoesNotContain("<EncodingType>", xmlContent);
        
        // Special characters should NOT be encoded
        Assert.Contains(">file with spaces.txt<", xmlContent);
        Assert.DoesNotContain("file%20with%20spaces.txt", xmlContent);
    }

    [Fact]
    public async Task ListObjectsV2_WithInvalidEncodingType_ReturnsBadRequest()
    {
        // Arrange
        var bucketName = $"invalid-encoding-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);

        // Act - Try to list with invalid encoding-type
        var listResponse = await Client.GetAsync($"/{bucketName}?list-type=2&encoding-type=base64");

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, listResponse.StatusCode);
        var xmlContent = await listResponse.Content.ReadAsStringAsync();
        Assert.Contains("<Code>InvalidArgument</Code>", xmlContent);
        Assert.Contains("Invalid Encoding Method", xmlContent);
        Assert.Contains("base64", xmlContent);
    }

    [Fact]
    public async Task ListObjectsV2_WithFetchOwnerTrue_IncludesOwner()
    {
        // Arrange
        var bucketName = $"fetch-owner-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        await Client.PutAsync($"/{bucketName}/test.txt", new StringContent("content"));

        // Act - List with fetch-owner=true
        var listResponse = await Client.GetAsync($"/{bucketName}?list-type=2&fetch-owner=true");

        // Assert
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var xmlContent = await listResponse.Content.ReadAsStringAsync();
        
        // Should include Owner information
        Assert.Contains("<Owner>", xmlContent);
        Assert.Contains("<ID>", xmlContent);
        Assert.Contains("<DisplayName>", xmlContent);
    }

    [Fact]
    public async Task ListObjectsV2_WithFetchOwnerFalse_ExcludesOwner()
    {
        // Arrange
        var bucketName = $"no-owner-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        await Client.PutAsync($"/{bucketName}/test.txt", new StringContent("content"));

        // Act - List with fetch-owner=false (or omitted, as false is default)
        var listResponse = await Client.GetAsync($"/{bucketName}?list-type=2&fetch-owner=false");

        // Assert
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var xmlContent = await listResponse.Content.ReadAsStringAsync();
        
        // Should NOT include Owner information
        Assert.DoesNotContain("<Owner>", xmlContent);
    }

    [Fact]
    public async Task ListObjectsV1_AlwaysIncludesOwner()
    {
        // Arrange
        var bucketName = $"v1-owner-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        await Client.PutAsync($"/{bucketName}/test.txt", new StringContent("content"));

        // Act - List with V1 API (no list-type parameter)
        var listResponse = await Client.GetAsync($"/{bucketName}");

        // Assert
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var xmlContent = await listResponse.Content.ReadAsStringAsync();
        
        // V1 should always include Owner information
        Assert.Contains("<Owner>", xmlContent);
        Assert.Contains("<ID>", xmlContent);
        Assert.Contains("<DisplayName>", xmlContent);
    }

    [Fact]
    public async Task ListObjectsV2_EncodingTypeUrlWithCommonPrefixes()
    {
        // Arrange
        var bucketName = $"prefix-encoding-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        
        // Create objects with special characters in prefixes
        // We need to encode parts separately to preserve the folder structure
        var objects = new[] 
        {
            ("folder with spaces", "file1.txt"),
            ("folder with spaces", "file2.txt"),
            ("folder[brackets]", "file3.txt")
        };
        
        foreach (var (folder, file) in objects)
        {
            // URL encode folder and file separately, keep the slash unencoded
            var encodedPath = $"{Uri.EscapeDataString(folder)}/{Uri.EscapeDataString(file)}";
            await Client.PutAsync($"/{bucketName}/{encodedPath}", 
                new StringContent("content"));
        }

        // Act - List with delimiter and encoding-type=url
        var listResponse = await Client.GetAsync($"/{bucketName}?list-type=2&delimiter=/&encoding-type=url");

        // Assert
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var xmlContent = await listResponse.Content.ReadAsStringAsync();
        
        // Common prefixes should be encoded (including the trailing slash as %2F)
        Assert.Contains("<CommonPrefixes>", xmlContent);
        Assert.Contains("folder%20with%20spaces%2F", xmlContent);
        Assert.Contains("folder%5Bbrackets%5D%2F", xmlContent);
    }

    [Fact]
    public async Task ListObjectsV1_WithEncodingTypeUrl_EncodesAllFields()
    {
        // Arrange
        var bucketName = $"v1-encoding-test-{Guid.NewGuid()}";
        await Client.PutAsync($"/{bucketName}", null);
        await Client.PutAsync($"/{bucketName}/{Uri.EscapeDataString("test file.txt")}", 
            new StringContent("content"));

        // Act - List V1 with encoding-type=url (no prefix/marker filters to see all files)
        var listResponse = await Client.GetAsync($"/{bucketName}?encoding-type=url");

        // Assert
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var xmlContent = await listResponse.Content.ReadAsStringAsync();
        
        // Check encoding-type is present and keys are encoded
        Assert.Contains("<EncodingType>url</EncodingType>", xmlContent);
        Assert.Contains("test%20file.txt", xmlContent);
    }
}