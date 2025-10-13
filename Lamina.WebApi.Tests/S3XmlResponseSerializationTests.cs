using System.Xml.Serialization;
using Lamina.Core.Models;
using Xunit;

namespace Lamina.WebApi.Tests;

public class S3XmlResponseSerializationTests
{
    [Fact]
    public void CompleteMultipartUploadResult_WithNullChecksums_OmitsChecksumFieldsFromXml()
    {
        // Arrange
        var result = new CompleteMultipartUploadResult
        {
            Location = "http://example.com/bucket/key",
            Bucket = "bucket",
            Key = "key",
            ETag = "\"etag123\"",
            ChecksumCRC32 = null,
            ChecksumCRC32C = null,
            ChecksumCRC64NVME = null,
            ChecksumSHA1 = null,
            ChecksumSHA256 = null
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<ChecksumCRC32", xml);
        Assert.DoesNotContain("<ChecksumCRC32C", xml);
        Assert.DoesNotContain("<ChecksumCRC64NVME", xml);
        Assert.DoesNotContain("<ChecksumSHA1", xml);
        Assert.DoesNotContain("<ChecksumSHA256", xml);
        Assert.Contains("<ETag>", xml);
        Assert.Contains("<Bucket>", xml);
    }

    [Fact]
    public void CompleteMultipartUploadResult_WithEmptyChecksums_OmitsChecksumFieldsFromXml()
    {
        // Arrange
        var result = new CompleteMultipartUploadResult
        {
            Location = "http://example.com/bucket/key",
            Bucket = "bucket",
            Key = "key",
            ETag = "\"etag123\"",
            ChecksumCRC32 = "",
            ChecksumCRC32C = "",
            ChecksumCRC64NVME = "",
            ChecksumSHA1 = "",
            ChecksumSHA256 = ""
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<ChecksumCRC32", xml);
        Assert.DoesNotContain("<ChecksumCRC32C", xml);
        Assert.DoesNotContain("<ChecksumCRC64NVME", xml);
        Assert.DoesNotContain("<ChecksumSHA1", xml);
        Assert.DoesNotContain("<ChecksumSHA256", xml);
    }

    [Fact]
    public void CompleteMultipartUploadResult_WithChecksums_IncludesChecksumFieldsInXml()
    {
        // Arrange
        var result = new CompleteMultipartUploadResult
        {
            Location = "http://example.com/bucket/key",
            Bucket = "bucket",
            Key = "key",
            ETag = "\"etag123\"",
            ChecksumCRC32 = "crc32value",
            ChecksumCRC32C = "crc32cvalue",
            ChecksumCRC64NVME = "crc64value",
            ChecksumSHA1 = "sha1value",
            ChecksumSHA256 = "sha256value"
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.Contains("<ChecksumCRC32>crc32value</ChecksumCRC32>", xml);
        Assert.Contains("<ChecksumCRC32C>crc32cvalue</ChecksumCRC32C>", xml);
        Assert.Contains("<ChecksumCRC64NVME>crc64value</ChecksumCRC64NVME>", xml);
        Assert.Contains("<ChecksumSHA1>sha1value</ChecksumSHA1>", xml);
        Assert.Contains("<ChecksumSHA256>sha256value</ChecksumSHA256>", xml);
    }

    [Fact]
    public void CopyObjectResult_WithNullChecksums_OmitsChecksumFieldsFromXml()
    {
        // Arrange
        var result = new CopyObjectResult
        {
            ETag = "\"etag123\"",
            LastModified = "2025-01-01T00:00:00.000Z",
            ChecksumCRC32 = null,
            ChecksumCRC32C = null,
            ChecksumCRC64NVME = null,
            ChecksumSHA1 = null,
            ChecksumSHA256 = null
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<ChecksumCRC32", xml);
        Assert.DoesNotContain("<ChecksumCRC32C", xml);
        Assert.DoesNotContain("<ChecksumCRC64NVME", xml);
        Assert.DoesNotContain("<ChecksumSHA1", xml);
        Assert.DoesNotContain("<ChecksumSHA256", xml);
        Assert.Contains("<ETag>", xml);
        Assert.Contains("<LastModified>", xml);
    }

    [Fact]
    public void CopyObjectResult_WithChecksums_IncludesChecksumFieldsInXml()
    {
        // Arrange
        var result = new CopyObjectResult
        {
            ETag = "\"etag123\"",
            LastModified = "2025-01-01T00:00:00.000Z",
            ChecksumCRC32 = "crc32value",
            ChecksumSHA256 = "sha256value"
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.Contains("<ChecksumCRC32>crc32value</ChecksumCRC32>", xml);
        Assert.Contains("<ChecksumSHA256>sha256value</ChecksumSHA256>", xml);
    }

    [Fact]
    public void CopyPartResult_WithNullChecksums_OmitsChecksumFieldsFromXml()
    {
        // Arrange
        var result = new CopyPartResult
        {
            ETag = "\"etag123\"",
            LastModified = "2025-01-01T00:00:00.000Z",
            ChecksumCRC32 = null,
            ChecksumCRC32C = null,
            ChecksumCRC64NVME = null,
            ChecksumSHA1 = null,
            ChecksumSHA256 = null
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.DoesNotContain("<ChecksumCRC32", xml);
        Assert.DoesNotContain("<ChecksumCRC32C", xml);
        Assert.DoesNotContain("<ChecksumCRC64NVME", xml);
        Assert.DoesNotContain("<ChecksumSHA1", xml);
        Assert.DoesNotContain("<ChecksumSHA256", xml);
        Assert.Contains("<ETag>", xml);
        Assert.Contains("<LastModified>", xml);
    }

    [Fact]
    public void CopyPartResult_WithChecksums_IncludesChecksumFieldsInXml()
    {
        // Arrange
        var result = new CopyPartResult
        {
            ETag = "\"etag123\"",
            LastModified = "2025-01-01T00:00:00.000Z",
            ChecksumCRC32 = "crc32value",
            ChecksumCRC32C = "crc32cvalue",
            ChecksumCRC64NVME = "crc64value"
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.Contains("<ChecksumCRC32>crc32value</ChecksumCRC32>", xml);
        Assert.Contains("<ChecksumCRC32C>crc32cvalue</ChecksumCRC32C>", xml);
        Assert.Contains("<ChecksumCRC64NVME>crc64value</ChecksumCRC64NVME>", xml);
    }

    [Fact]
    public void CompletedPartXml_WithNullChecksums_OmitsChecksumFieldsFromXml()
    {
        // Arrange
        var part = new CompletedPartXml
        {
            PartNumber = 1,
            ETag = "etag123",
            ChecksumCRC32 = null,
            ChecksumCRC32C = null,
            ChecksumCRC64NVME = null,
            ChecksumSHA1 = null,
            ChecksumSHA256 = null
        };

        // Act
        var xml = SerializeToXml(part);

        // Assert
        Assert.DoesNotContain("<ChecksumCRC32", xml);
        Assert.DoesNotContain("<ChecksumCRC32C", xml);
        Assert.DoesNotContain("<ChecksumCRC64NVME", xml);
        Assert.DoesNotContain("<ChecksumSHA1", xml);
        Assert.DoesNotContain("<ChecksumSHA256", xml);
        Assert.Contains("<PartNumber>", xml);
        Assert.Contains("<ETag>", xml);
    }

    [Fact]
    public void CompletedPartXml_WithChecksums_IncludesChecksumFieldsInXml()
    {
        // Arrange
        var part = new CompletedPartXml
        {
            PartNumber = 1,
            ETag = "etag123",
            ChecksumCRC32 = "crc32value",
            ChecksumSHA1 = "sha1value"
        };

        // Act
        var xml = SerializeToXml(part);

        // Assert
        Assert.Contains("<ChecksumCRC32>crc32value</ChecksumCRC32>", xml);
        Assert.Contains("<ChecksumSHA1>sha1value</ChecksumSHA1>", xml);
    }

    [Fact]
    public void CompletedPartXmlNoNamespace_WithNullChecksums_OmitsChecksumFieldsFromXml()
    {
        // Arrange
        var part = new CompletedPartXmlNoNamespace
        {
            PartNumber = 1,
            ETag = "etag123",
            ChecksumCRC32 = null,
            ChecksumCRC32C = null,
            ChecksumCRC64NVME = null,
            ChecksumSHA1 = null,
            ChecksumSHA256 = null
        };

        // Act
        var xml = SerializeToXml(part);

        // Assert
        Assert.DoesNotContain("<ChecksumCRC32", xml);
        Assert.DoesNotContain("<ChecksumCRC32C", xml);
        Assert.DoesNotContain("<ChecksumCRC64NVME", xml);
        Assert.DoesNotContain("<ChecksumSHA1", xml);
        Assert.DoesNotContain("<ChecksumSHA256", xml);
        Assert.Contains("<PartNumber>", xml);
        Assert.Contains("<ETag>", xml);
    }

    [Fact]
    public void CompletedPartXmlNoNamespace_WithChecksums_IncludesChecksumFieldsInXml()
    {
        // Arrange
        var part = new CompletedPartXmlNoNamespace
        {
            PartNumber = 1,
            ETag = "etag123",
            ChecksumCRC32C = "crc32cvalue",
            ChecksumCRC64NVME = "crc64value",
            ChecksumSHA256 = "sha256value"
        };

        // Act
        var xml = SerializeToXml(part);

        // Assert
        Assert.Contains("<ChecksumCRC32C>crc32cvalue</ChecksumCRC32C>", xml);
        Assert.Contains("<ChecksumCRC64NVME>crc64value</ChecksumCRC64NVME>", xml);
        Assert.Contains("<ChecksumSHA256>sha256value</ChecksumSHA256>", xml);
    }

    [Fact]
    public void CompleteMultipartUploadResult_WithMixedChecksums_IncludesOnlyNonNullChecksums()
    {
        // Arrange
        var result = new CompleteMultipartUploadResult
        {
            Location = "http://example.com/bucket/key",
            Bucket = "bucket",
            Key = "key",
            ETag = "\"etag123\"",
            ChecksumCRC32 = "crc32value",
            ChecksumCRC32C = null,
            ChecksumCRC64NVME = "",
            ChecksumSHA1 = "sha1value",
            ChecksumSHA256 = null
        };

        // Act
        var xml = SerializeToXml(result);

        // Assert
        Assert.Contains("<ChecksumCRC32>crc32value</ChecksumCRC32>", xml);
        Assert.Contains("<ChecksumSHA1>sha1value</ChecksumSHA1>", xml);
        Assert.DoesNotContain("<ChecksumCRC32C", xml);
        Assert.DoesNotContain("<ChecksumCRC64NVME", xml);
        Assert.DoesNotContain("<ChecksumSHA256", xml);
    }

    [Fact]
    public void CompleteMultipartUploadXml_WithPartsHavingMixedChecksums_SerializesCorrectly()
    {
        // Arrange
        var completeRequest = new CompleteMultipartUploadXml
        {
            Parts = new List<CompletedPartXml>
            {
                new CompletedPartXml
                {
                    PartNumber = 1,
                    ETag = "etag1",
                    ChecksumCRC32 = "crc1",
                    ChecksumSHA256 = null
                },
                new CompletedPartXml
                {
                    PartNumber = 2,
                    ETag = "etag2",
                    ChecksumCRC32 = null,
                    ChecksumSHA256 = "sha256value"
                }
            }
        };

        // Act
        var xml = SerializeToXml(completeRequest);

        // Assert
        // First part should have CRC32 but not SHA256
        Assert.Contains("<ChecksumCRC32>crc1</ChecksumCRC32>", xml);

        // Second part should have SHA256 but not CRC32
        Assert.Contains("<ChecksumSHA256>sha256value</ChecksumSHA256>", xml);

        // Verify both parts are present
        Assert.Contains("<PartNumber>1</PartNumber>", xml);
        Assert.Contains("<PartNumber>2</PartNumber>", xml);
    }

    private static string SerializeToXml<T>(T obj)
    {
        var serializer = new XmlSerializer(typeof(T));
        using var writer = new StringWriter();
        serializer.Serialize(writer, obj);
        return writer.ToString();
    }
}
