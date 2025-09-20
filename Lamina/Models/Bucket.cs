namespace Lamina.Models;

public class Bucket
{
    public string Name { get; set; } = string.Empty;
    public DateTime CreationDate { get; set; }
    public string Region { get; set; } = "us-east-1";
    public Dictionary<string, string> Tags { get; set; } = new();
}

public class CreateBucketRequest
{
    public string? Region { get; set; }
}

public class UpdateBucketRequest
{
    public Dictionary<string, string>? Tags { get; set; }
}

public class ListBucketsResponse
{
    public List<Bucket> Buckets { get; set; } = new();
}