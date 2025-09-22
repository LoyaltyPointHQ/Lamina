namespace Lamina.Configuration;

public class AutoBucketCreationSettings
{
    public bool Enabled { get; set; } = false;
    public List<BucketConfiguration> Buckets { get; set; } = new List<BucketConfiguration>();
}

public class BucketConfiguration
{
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
    public Dictionary<string, string> Tags { get; set; } = new Dictionary<string, string>();
}