using Lamina.Models;

namespace Lamina.Configuration;

public class AutoBucketCreationSettings
{
    public bool Enabled { get; set; } = false;
    public List<BucketConfiguration> Buckets { get; set; } = new();
}

public class BucketConfiguration
{
    public string Name { get; set; } = string.Empty;
    public BucketType Type { get; set; }
}