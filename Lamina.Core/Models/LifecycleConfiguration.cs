namespace Lamina.Core.Models;

public enum LifecycleRuleStatus
{
    Enabled,
    Disabled
}

public class LifecycleConfiguration
{
    public List<LifecycleRule> Rules { get; set; } = new();
}

public class LifecycleRule
{
    public string? Id { get; set; }
    public LifecycleRuleStatus Status { get; set; } = LifecycleRuleStatus.Enabled;
    public LifecycleFilter? Filter { get; set; }

    /// <summary>
    /// Legacy S3 format: Prefix directly on Rule (without Filter wrapper).
    /// When non-null, Filter is ignored.
    /// </summary>
    public string? Prefix { get; set; }

    public LifecycleExpiration? Expiration { get; set; }
    public LifecycleAbortIncompleteMultipartUpload? AbortIncompleteMultipartUpload { get; set; }
}

public class LifecycleFilter
{
    public string? Prefix { get; set; }
    public LifecycleTag? Tag { get; set; }
    public long? ObjectSizeGreaterThan { get; set; }
    public long? ObjectSizeLessThan { get; set; }
    public LifecycleAndOperator? And { get; set; }
}

public class LifecycleAndOperator
{
    public string? Prefix { get; set; }
    public List<LifecycleTag> Tags { get; set; } = new();
    public long? ObjectSizeGreaterThan { get; set; }
    public long? ObjectSizeLessThan { get; set; }
}

public class LifecycleTag
{
    public string Key { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
}

public class LifecycleExpiration
{
    public int? Days { get; set; }
    public DateTime? Date { get; set; }
}

public class LifecycleAbortIncompleteMultipartUpload
{
    public int DaysAfterInitiation { get; set; }
}
