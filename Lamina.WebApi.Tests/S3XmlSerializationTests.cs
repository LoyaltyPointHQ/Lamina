using System.Xml;
using System.Xml.Serialization;
using Lamina.Core.Models;

namespace Lamina.WebApi.Tests;

public class S3XmlSerializationTests
{
    private static string SerializeToXml<T>(T obj)
    {
        var serializer = new XmlSerializer(typeof(T));
        using var stringWriter = new StringWriter();
        using var xmlWriter = XmlWriter.Create(stringWriter, new XmlWriterSettings { Indent = false, OmitXmlDeclaration = true });
        serializer.Serialize(xmlWriter, obj);
        return stringWriter.ToString();
    }

    [Fact]
    public void ListPartsResult_WhenNotTruncated_OmitsNextPartNumberMarker()
    {
        // Arrange
        var result = new ListPartsResult
        {
            Bucket = "test-bucket",
            Key = "test-key",
            UploadId = "test-upload-id",
            IsTruncated = false,
            NextPartNumberMarker = 100
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<NextPartNumberMarker>", xml);
    }

    [Fact]
    public void ListPartsResult_WhenTruncatedWithValue_IncludesNextPartNumberMarker()
    {
        // Arrange
        var result = new ListPartsResult
        {
            Bucket = "test-bucket",
            Key = "test-key",
            UploadId = "test-upload-id",
            IsTruncated = true,
            NextPartNumberMarker = 100
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.Contains("<NextPartNumberMarker>100</NextPartNumberMarker>", xml);
    }

    [Fact]
    public void ListPartsResult_WhenTruncatedWithoutValue_OmitsNextPartNumberMarker()
    {
        // Arrange
        var result = new ListPartsResult
        {
            Bucket = "test-bucket",
            Key = "test-key",
            UploadId = "test-upload-id",
            IsTruncated = true,
            NextPartNumberMarker = null
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<NextPartNumberMarker>", xml);
    }

    [Fact]
    public void ListMultipartUploadsResult_WhenNotTruncated_OmitsNextKeyMarker()
    {
        // Arrange
        var result = new ListMultipartUploadsResult
        {
            Bucket = "test-bucket",
            IsTruncated = false,
            NextKeyMarker = "some-key"
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<NextKeyMarker>", xml);
    }

    [Fact]
    public void ListMultipartUploadsResult_WhenTruncatedWithValue_IncludesNextKeyMarker()
    {
        // Arrange
        var result = new ListMultipartUploadsResult
        {
            Bucket = "test-bucket",
            IsTruncated = true,
            NextKeyMarker = "some-key"
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.Contains("<NextKeyMarker>some-key</NextKeyMarker>", xml);
    }

    [Fact]
    public void ListMultipartUploadsResult_WhenTruncatedWithEmptyValue_OmitsNextKeyMarker()
    {
        // Arrange
        var result = new ListMultipartUploadsResult
        {
            Bucket = "test-bucket",
            IsTruncated = true,
            NextKeyMarker = ""
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<NextKeyMarker>", xml);
    }

    [Fact]
    public void ListMultipartUploadsResult_WhenNotTruncated_OmitsNextUploadIdMarker()
    {
        // Arrange
        var result = new ListMultipartUploadsResult
        {
            Bucket = "test-bucket",
            IsTruncated = false,
            NextUploadIdMarker = "some-upload-id"
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<NextUploadIdMarker>", xml);
    }

    [Fact]
    public void ListMultipartUploadsResult_WhenTruncatedWithValue_IncludesNextUploadIdMarker()
    {
        // Arrange
        var result = new ListMultipartUploadsResult
        {
            Bucket = "test-bucket",
            IsTruncated = true,
            NextUploadIdMarker = "some-upload-id"
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.Contains("<NextUploadIdMarker>some-upload-id</NextUploadIdMarker>", xml);
    }

    [Fact]
    public void ListMultipartUploadsResult_WhenTruncatedWithNullValue_OmitsNextUploadIdMarker()
    {
        // Arrange
        var result = new ListMultipartUploadsResult
        {
            Bucket = "test-bucket",
            IsTruncated = true,
            NextUploadIdMarker = null
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<NextUploadIdMarker>", xml);
    }

    [Fact]
    public void ListMultipartUploadsResult_WhenNotTruncated_OmitsBothMarkers()
    {
        // Arrange
        var result = new ListMultipartUploadsResult
        {
            Bucket = "test-bucket",
            IsTruncated = false,
            NextKeyMarker = "some-key",
            NextUploadIdMarker = "some-upload-id"
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<NextKeyMarker>", xml);
        Assert.DoesNotContain("<NextUploadIdMarker>", xml);
    }

    [Fact]
    public void ListMultipartUploadsResult_WhenTruncatedWithBothValues_IncludesBothMarkers()
    {
        // Arrange
        var result = new ListMultipartUploadsResult
        {
            Bucket = "test-bucket",
            IsTruncated = true,
            NextKeyMarker = "some-key",
            NextUploadIdMarker = "some-upload-id"
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.Contains("<NextKeyMarker>some-key</NextKeyMarker>", xml);
        Assert.Contains("<NextUploadIdMarker>some-upload-id</NextUploadIdMarker>", xml);
    }

    [Fact]
    public void ListBucketResult_WhenNotTruncated_OmitsNextMarker()
    {
        // Arrange
        var result = new ListBucketResult
        {
            Name = "test-bucket",
            IsTruncated = false,
            NextMarker = "some-marker"
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<NextMarker>", xml);
    }

    [Fact]
    public void ListBucketResult_WhenTruncatedWithValue_IncludesNextMarker()
    {
        // Arrange
        var result = new ListBucketResult
        {
            Name = "test-bucket",
            IsTruncated = true,
            NextMarker = "some-marker"
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.Contains("<NextMarker>some-marker</NextMarker>", xml);
    }

    [Fact]
    public void ListBucketResult_WhenTruncatedWithEmptyValue_OmitsNextMarker()
    {
        // Arrange
        var result = new ListBucketResult
        {
            Name = "test-bucket",
            IsTruncated = true,
            NextMarker = ""
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<NextMarker>", xml);
    }

    [Fact]
    public void ListBucketResultV2_WhenNotTruncated_OmitsNextContinuationToken()
    {
        // Arrange
        var result = new ListBucketResultV2
        {
            Name = "test-bucket",
            IsTruncated = false,
            NextContinuationToken = "some-token"
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<NextContinuationToken>", xml);
    }

    [Fact]
    public void ListBucketResultV2_WhenTruncatedWithValue_IncludesNextContinuationToken()
    {
        // Arrange
        var result = new ListBucketResultV2
        {
            Name = "test-bucket",
            IsTruncated = true,
            NextContinuationToken = "some-token"
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.Contains("<NextContinuationToken>some-token</NextContinuationToken>", xml);
    }

    [Fact]
    public void ListBucketResultV2_WhenTruncatedWithNullValue_OmitsNextContinuationToken()
    {
        // Arrange
        var result = new ListBucketResultV2
        {
            Name = "test-bucket",
            IsTruncated = true,
            NextContinuationToken = null
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<NextContinuationToken>", xml);
    }
}
