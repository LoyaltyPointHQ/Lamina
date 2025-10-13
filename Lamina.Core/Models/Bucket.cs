namespace Lamina.Core.Models;

public enum BucketType
{
    GeneralPurpose,
    Directory
}

public class Bucket
{
    public string Name { get; set; } = string.Empty;
    public DateTime CreationDate { get; set; }
    public BucketType Type { get; set; } = BucketType.Directory;
    public string? StorageClass { get; set; }
    public Dictionary<string, string> Tags { get; set; } = new();
    public string? OwnerId { get; set; }
    public string? OwnerDisplayName { get; set; }
}

public class CreateBucketRequest
{
    public BucketType? Type { get; set; }
    public string? StorageClass { get; set; }
    public string? OwnerId { get; set; }
    public string? OwnerDisplayName { get; set; }
}

public class ListBucketsResponse
{
    public List<Bucket> Buckets { get; set; } = new();
}