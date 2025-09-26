namespace Lamina.Models;

public class S3Object
{
    public string Key { get; set; } = string.Empty;
    public string BucketName { get; set; } = string.Empty;
    public long Size { get; set; }
    public DateTime LastModified { get; set; }
    public string ETag { get; set; } = string.Empty;
    public string ContentType { get; set; } = "application/octet-stream";
    public Dictionary<string, string> Metadata { get; set; } = new();
    public byte[] Data { get; set; } = Array.Empty<byte>();
}

public class ListObjectsRequest
{
    public string? Prefix { get; set; }
    public string? Delimiter { get; set; }
    public int MaxKeys { get; set; } = 1000;
    public string? ContinuationToken { get; set; }

    // V2-specific fields
    public int ListType { get; set; } = 1; // 1 for ListObjects, 2 for ListObjectsV2
    public string? StartAfter { get; set; }
    public bool FetchOwner { get; set; } = false; // Only for V2, defaults to false
    
    // Encoding type for object keys (only "url" is supported)
    public string? EncodingType { get; set; }
}

public class ListObjectsResponse
{
    public List<S3ObjectInfo> Contents { get; set; } = new();
    public List<string> CommonPrefixes { get; set; } = new();
    public bool IsTruncated { get; set; }
    public string? NextContinuationToken { get; set; }
    public string? Prefix { get; set; }
    public string? Delimiter { get; set; }
    public int MaxKeys { get; set; }
}

public class S3ObjectInfo
{
    public string Key { get; set; } = string.Empty;
    public long Size { get; set; }
    public DateTime LastModified { get; set; }
    public string ETag { get; set; } = string.Empty;
    public string ContentType { get; set; } = string.Empty;
    public Dictionary<string, string> Metadata { get; set; } = new();
}

public class PutObjectRequest
{
    public string Key { get; set; } = string.Empty;
    public string? ContentType { get; set; }
    public Dictionary<string, string>? Metadata { get; set; }
}

public class GetObjectResponse
{
    public byte[] Data { get; set; } = Array.Empty<byte>();
    public string ContentType { get; set; } = string.Empty;
    public long ContentLength { get; set; }
    public string ETag { get; set; } = string.Empty;
    public DateTime LastModified { get; set; }
    public Dictionary<string, string> Metadata { get; set; } = new();
}