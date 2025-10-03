namespace Lamina.Core.Models;

public class MultipartUpload
{
    public string UploadId { get; set; } = string.Empty;
    public string BucketName { get; set; } = string.Empty;
    public string Key { get; set; } = string.Empty;
    public DateTime Initiated { get; set; }
    public Dictionary<string, string> Metadata { get; set; } = new();
    public string? ContentType { get; set; }
}

public class UploadPart
{
    public int PartNumber { get; set; }
    public string ETag { get; set; } = string.Empty;
    public long Size { get; set; }
    public DateTime LastModified { get; set; }
    public byte[] Data { get; set; } = Array.Empty<byte>();
}

public class InitiateMultipartUploadRequest
{
    public string Key { get; set; } = string.Empty;
    public string? ContentType { get; set; }
    public Dictionary<string, string>? Metadata { get; set; }
}

public class CompleteMultipartUploadRequest
{
    public string UploadId { get; set; } = string.Empty;
    public List<CompletedPart> Parts { get; set; } = new();
}

public class CompletedPart
{
    public int PartNumber { get; set; }
    public string ETag { get; set; } = string.Empty;
    public string? ChecksumCRC32 { get; set; }
    public string? ChecksumCRC32C { get; set; }
    public string? ChecksumCRC64NVME { get; set; }
    public string? ChecksumSHA1 { get; set; }
    public string? ChecksumSHA256 { get; set; }
}

public class CompleteMultipartUploadResponse
{
    public string Location { get; set; } = string.Empty;
    public string BucketName { get; set; } = string.Empty;
    public string Key { get; set; } = string.Empty;
    public string ETag { get; set; } = string.Empty;
    public string? ChecksumCRC32 { get; set; }
    public string? ChecksumCRC32C { get; set; }
    public string? ChecksumCRC64NVME { get; set; }
    public string? ChecksumSHA1 { get; set; }
    public string? ChecksumSHA256 { get; set; }
}