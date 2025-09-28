using Lamina.Core.Models;

namespace Lamina.Storage.Core.Configuration;

public class BucketDefaultsSettings
{
    public BucketType Type { get; set; } = BucketType.Directory;
    public string? StorageClass { get; set; }
}