using Lamina.Models;

namespace Lamina.Configuration;

public class BucketDefaultsSettings
{
    public BucketType Type { get; set; } = BucketType.GeneralPurpose;
    public string? StorageClass { get; set; }
}