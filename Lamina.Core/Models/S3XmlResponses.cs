using System.Xml.Serialization;

namespace Lamina.Core.Models;

[XmlRoot("ListAllMyBucketsResult", Namespace = "http://s3.amazonaws.com/doc/2006-03-01/")]
public class ListAllMyBucketsResult
{
    [XmlElement("Owner")]
    public Owner? Owner { get; set; }

    [XmlArray("Buckets")]
    [XmlArrayItem("Bucket")]
    public List<BucketInfo> Buckets { get; set; } = new();
}

[XmlRoot("Owner")]
public class Owner
{
    [XmlElement("ID")]
    public string ID { get; set; } = "anonymous";

    [XmlElement("DisplayName")]
    public string DisplayName { get; set; } = "anonymous";
    
    public Owner() { }
    
    public Owner(string id, string displayName)
    {
        ID = id ?? "anonymous";
        DisplayName = displayName ?? "anonymous";
    }
}

[XmlRoot("Bucket")]
public class BucketInfo
{
    [XmlElement("Name")]
    public string Name { get; set; } = string.Empty;

    [XmlElement("CreationDate")]
    public string CreationDate { get; set; } = string.Empty;

    [XmlElement("BucketType")]
    public string? BucketType { get; set; }
}

[XmlRoot("ListBucketResult", Namespace = "http://s3.amazonaws.com/doc/2006-03-01/")]
public class ListBucketResult
{
    [XmlElement("Name")]
    public string Name { get; set; } = string.Empty;

    [XmlElement("Prefix")]
    public string? Prefix { get; set; }

    [XmlElement("Marker")]
    public string? Marker { get; set; }

    [XmlElement("MaxKeys")]
    public int MaxKeys { get; set; }

    [XmlElement("EncodingType")]
    public string? EncodingType { get; set; }

    [XmlElement("IsTruncated")]
    public bool IsTruncated { get; set; }

    [XmlElement("NextMarker")]
    public string? NextMarker { get; set; }

    [XmlElement("Contents")]
    public List<Contents> ContentsList { get; set; } = new();

    [XmlElement("CommonPrefixes")]
    public List<CommonPrefixes> CommonPrefixesList { get; set; } = new();
}

[XmlRoot("Contents")]
public class Contents
{
    [XmlElement("Key")]
    public string Key { get; set; } = string.Empty;

    [XmlElement("LastModified")]
    public string LastModified { get; set; } = string.Empty;

    [XmlElement("ETag")]
    public string ETag { get; set; } = string.Empty;

    [XmlElement("Size")]
    public long Size { get; set; }

    [XmlElement("StorageClass")]
    public string StorageClass { get; set; } = "STANDARD";

    [XmlElement("Owner")]
    public Owner? Owner { get; set; }
}

[XmlRoot("ListBucketResult", Namespace = "http://s3.amazonaws.com/doc/2006-03-01/")]
public class ListBucketResultV2
{
    [XmlElement("Name")]
    public string Name { get; set; } = string.Empty;

    [XmlElement("Prefix")]
    public string? Prefix { get; set; }

    [XmlElement("StartAfter")]
    public string? StartAfter { get; set; }

    [XmlElement("ContinuationToken")]
    public string? ContinuationToken { get; set; }

    [XmlElement("NextContinuationToken")]
    public string? NextContinuationToken { get; set; }

    [XmlElement("KeyCount")]
    public int KeyCount { get; set; }

    [XmlElement("MaxKeys")]
    public int MaxKeys { get; set; }

    [XmlElement("EncodingType")]
    public string? EncodingType { get; set; }

    [XmlElement("IsTruncated")]
    public bool IsTruncated { get; set; }

    [XmlElement("Contents")]
    public List<Contents> ContentsList { get; set; } = new();

    [XmlElement("CommonPrefixes")]
    public List<CommonPrefixes> CommonPrefixesList { get; set; } = new();
}

[XmlRoot("CommonPrefixes")]
public class CommonPrefixes
{
    [XmlElement("Prefix")]
    public string Prefix { get; set; } = string.Empty;
}

[XmlRoot("InitiateMultipartUploadResult", Namespace = "http://s3.amazonaws.com/doc/2006-03-01/")]
public class InitiateMultipartUploadResult
{
    [XmlElement("Bucket")]
    public string Bucket { get; set; } = string.Empty;

    [XmlElement("Key")]
    public string Key { get; set; } = string.Empty;

    [XmlElement("UploadId")]
    public string UploadId { get; set; } = string.Empty;
}

[XmlRoot("CompleteMultipartUploadResult", Namespace = "http://s3.amazonaws.com/doc/2006-03-01/")]
public class CompleteMultipartUploadResult
{
    [XmlElement("Location")]
    public string Location { get; set; } = string.Empty;

    [XmlElement("Bucket")]
    public string Bucket { get; set; } = string.Empty;

    [XmlElement("Key")]
    public string Key { get; set; } = string.Empty;

    [XmlElement("ETag")]
    public string ETag { get; set; } = string.Empty;

    [XmlElement("ChecksumCRC32")]
    public string? ChecksumCRC32 { get; set; }

    [XmlElement("ChecksumCRC32C")]
    public string? ChecksumCRC32C { get; set; }

    [XmlElement("ChecksumCRC64NVME")]
    public string? ChecksumCRC64NVME { get; set; }

    [XmlElement("ChecksumSHA1")]
    public string? ChecksumSHA1 { get; set; }

    [XmlElement("ChecksumSHA256")]
    public string? ChecksumSHA256 { get; set; }
}

[XmlRoot("CopyObjectResult", Namespace = "http://s3.amazonaws.com/doc/2006-03-01/")]
public class CopyObjectResult
{
    [XmlElement("ETag")]
    public string ETag { get; set; } = string.Empty;

    [XmlElement("LastModified")]
    public string LastModified { get; set; } = string.Empty;

    [XmlElement("ChecksumCRC32")]
    public string? ChecksumCRC32 { get; set; }

    [XmlElement("ChecksumCRC32C")]
    public string? ChecksumCRC32C { get; set; }

    [XmlElement("ChecksumCRC64NVME")]
    public string? ChecksumCRC64NVME { get; set; }

    [XmlElement("ChecksumSHA1")]
    public string? ChecksumSHA1 { get; set; }

    [XmlElement("ChecksumSHA256")]
    public string? ChecksumSHA256 { get; set; }
}

[XmlRoot("ListPartsResult", Namespace = "http://s3.amazonaws.com/doc/2006-03-01/")]
public class ListPartsResult
{
    [XmlElement("Bucket")]
    public string Bucket { get; set; } = string.Empty;

    [XmlElement("Key")]
    public string Key { get; set; } = string.Empty;

    [XmlElement("UploadId")]
    public string UploadId { get; set; } = string.Empty;

    [XmlElement("Initiator")]
    public Owner? Initiator { get; set; }

    [XmlElement("Owner")]
    public Owner? Owner { get; set; }

    [XmlElement("StorageClass")]
    public string StorageClass { get; set; } = "STANDARD";

    [XmlElement("PartNumberMarker")]
    public int PartNumberMarker { get; set; }

    [XmlElement("NextPartNumberMarker")]
    public int? NextPartNumberMarker { get; set; }

    [XmlElement("MaxParts")]
    public int MaxParts { get; set; }

    [XmlElement("IsTruncated")]
    public bool IsTruncated { get; set; }

    [XmlElement("Part")]
    public List<Part> Parts { get; set; } = new();
}

[XmlRoot("Part")]
public class Part
{
    [XmlElement("PartNumber")]
    public int PartNumber { get; set; }

    [XmlElement("LastModified")]
    public string LastModified { get; set; } = string.Empty;

    [XmlElement("ETag")]
    public string ETag { get; set; } = string.Empty;

    [XmlElement("Size")]
    public long Size { get; set; }
}

[XmlRoot("ListMultipartUploadsResult", Namespace = "http://s3.amazonaws.com/doc/2006-03-01/")]
public class ListMultipartUploadsResult
{
    [XmlElement("Bucket")]
    public string Bucket { get; set; } = string.Empty;

    [XmlElement("KeyMarker")]
    public string? KeyMarker { get; set; }

    [XmlElement("UploadIdMarker")]
    public string? UploadIdMarker { get; set; }

    [XmlElement("NextKeyMarker")]
    public string? NextKeyMarker { get; set; }

    [XmlElement("NextUploadIdMarker")]
    public string? NextUploadIdMarker { get; set; }

    [XmlElement("MaxUploads")]
    public int MaxUploads { get; set; }

    [XmlElement("IsTruncated")]
    public bool IsTruncated { get; set; }

    [XmlElement("Upload")]
    public List<Upload> Uploads { get; set; } = new();
}

[XmlRoot("Upload")]
public class Upload
{
    [XmlElement("Key")]
    public string Key { get; set; } = string.Empty;

    [XmlElement("UploadId")]
    public string UploadId { get; set; } = string.Empty;

    [XmlElement("Initiator")]
    public Owner? Initiator { get; set; }

    [XmlElement("Owner")]
    public Owner? Owner { get; set; }

    [XmlElement("StorageClass")]
    public string StorageClass { get; set; } = "STANDARD";

    [XmlElement("Initiated")]
    public string Initiated { get; set; } = string.Empty;
}

[XmlRoot("Error")]
public class S3Error
{
    [XmlElement("Code")]
    public string Code { get; set; } = string.Empty;

    [XmlElement("Message")]
    public string Message { get; set; } = string.Empty;

    [XmlElement("Resource")]
    public string? Resource { get; set; }

    [XmlElement("RequestId")]
    public string RequestId { get; set; } = Guid.NewGuid().ToString();

    [XmlElement("HostId")]
    public string HostId { get; set; } = string.Empty;
}

// Version with namespace for S3 spec compliance
[XmlRoot("CompleteMultipartUpload", Namespace = "http://s3.amazonaws.com/doc/2006-03-01/")]
public class CompleteMultipartUploadXml
{
    [XmlElement("Part")]
    public List<CompletedPartXml> Parts { get; set; } = new();
}

// Version without namespace for compatibility
[XmlRoot("CompleteMultipartUpload")]
public class CompleteMultipartUploadXmlNoNamespace
{
    [XmlElement("Part")]
    public List<CompletedPartXmlNoNamespace> Parts { get; set; } = new();
}

[XmlRoot("Part")]
public class CompletedPartXml
{
    [XmlElement("PartNumber")]
    public int PartNumber { get; set; }

    [XmlElement("ETag")]
    public string ETag { get; set; } = string.Empty;

    [XmlElement("ChecksumCRC32")]
    public string? ChecksumCRC32 { get; set; }

    [XmlElement("ChecksumCRC32C")]
    public string? ChecksumCRC32C { get; set; }

    [XmlElement("ChecksumCRC64NVME")]
    public string? ChecksumCRC64NVME { get; set; }

    [XmlElement("ChecksumSHA1")]
    public string? ChecksumSHA1 { get; set; }

    [XmlElement("ChecksumSHA256")]
    public string? ChecksumSHA256 { get; set; }
}

[XmlRoot("Part")]
public class CompletedPartXmlNoNamespace
{
    [XmlElement("PartNumber")]
    public int PartNumber { get; set; }

    [XmlElement("ETag")]
    public string ETag { get; set; } = string.Empty;

    [XmlElement("ChecksumCRC32")]
    public string? ChecksumCRC32 { get; set; }

    [XmlElement("ChecksumCRC32C")]
    public string? ChecksumCRC32C { get; set; }

    [XmlElement("ChecksumCRC64NVME")]
    public string? ChecksumCRC64NVME { get; set; }

    [XmlElement("ChecksumSHA1")]
    public string? ChecksumSHA1 { get; set; }

    [XmlElement("ChecksumSHA256")]
    public string? ChecksumSHA256 { get; set; }
}

[XmlRoot("LocationConstraint", Namespace = "http://s3.amazonaws.com/doc/2006-03-01/")]
public class LocationConstraintResult
{
    [XmlText]
    public string? Region { get; set; }
}

[XmlRoot("VersioningConfiguration", Namespace = "http://s3.amazonaws.com/doc/2006-03-01/")]
public class VersioningConfiguration
{
    [XmlElement("Status")]
    public string? Status { get; set; }

    [XmlElement("MfaDelete")]
    public string? MfaDelete { get; set; }
}

// Delete Multiple Objects XML models
[XmlRoot("Delete", Namespace = "http://s3.amazonaws.com/doc/2006-03-01/")]
public class DeleteMultipleObjectsRequest
{
    [XmlElement("Object")]
    public List<ObjectIdentifier> Objects { get; set; } = new();

    [XmlElement("Quiet")]
    public bool Quiet { get; set; }
}

// Version without namespace for compatibility
[XmlRoot("Delete")]
public class DeleteMultipleObjectsRequestNoNamespace
{
    [XmlElement("Object")]
    public List<ObjectIdentifier> Objects { get; set; } = new();

    [XmlElement("Quiet")]
    public bool Quiet { get; set; }
}

[XmlRoot("Object")]
public class ObjectIdentifier
{
    [XmlElement("Key")]
    public string Key { get; set; } = string.Empty;

    [XmlElement("VersionId")]
    public string? VersionId { get; set; }
}

[XmlRoot("DeleteResult", Namespace = "http://s3.amazonaws.com/doc/2006-03-01/")]
public class DeleteMultipleObjectsResult
{
    [XmlElement("Deleted")]
    public List<DeletedObject> Deleted { get; set; } = new();

    [XmlElement("Error")]
    public List<DeleteError> Errors { get; set; } = new();
}

[XmlRoot("Deleted")]
public class DeletedObject
{
    [XmlElement("Key")]
    public string Key { get; set; } = string.Empty;

    [XmlElement("VersionId")]
    public string? VersionId { get; set; }

    [XmlElement("DeleteMarker")]
    public bool? DeleteMarker { get; set; }

    [XmlElement("DeleteMarkerVersionId")]
    public string? DeleteMarkerVersionId { get; set; }

    public bool ShouldSerializeDeleteMarker() => DeleteMarker.HasValue;

    public bool ShouldSerializeDeleteMarkerVersionId() => !string.IsNullOrEmpty(DeleteMarkerVersionId);
}

[XmlRoot("Error")]
public class DeleteError
{
    [XmlElement("Key")]
    public string Key { get; set; } = string.Empty;

    [XmlElement("Code")]
    public string Code { get; set; } = string.Empty;

    [XmlElement("Message")]
    public string Message { get; set; } = string.Empty;

    [XmlElement("VersionId")]
    public string? VersionId { get; set; }
}