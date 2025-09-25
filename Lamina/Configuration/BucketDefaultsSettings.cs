using Lamina.Models;

namespace Lamina.Configuration;

public class BucketDefaultsSettings
{
    public BucketType Type { get; set; } = BucketType.Directory;
    public string? StorageClass { get; set; }
}