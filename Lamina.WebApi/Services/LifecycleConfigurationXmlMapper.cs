using Lamina.Core.Models;

namespace Lamina.WebApi.Services;

public static class LifecycleConfigurationXmlMapper
{
    public static LifecycleConfigurationXml ToXml(LifecycleConfiguration config)
    {
        return new LifecycleConfigurationXml
        {
            Rules = config.Rules.Select(RuleToXml).ToList()
        };
    }

    private static LifecycleRuleXml RuleToXml(LifecycleRule rule)
    {
        var xml = new LifecycleRuleXml
        {
            Id = rule.Id,
            Status = rule.Status == LifecycleRuleStatus.Enabled ? "Enabled" : "Disabled",
            Prefix = rule.Prefix
        };

        if (rule.Filter != null)
        {
            xml.Filter = new LifecycleFilterXml
            {
                Prefix = rule.Filter.Prefix,
                ObjectSizeGreaterThan = rule.Filter.ObjectSizeGreaterThan,
                ObjectSizeLessThan = rule.Filter.ObjectSizeLessThan,
                Tag = rule.Filter.Tag == null ? null : new TagXml { Key = rule.Filter.Tag.Key, Value = rule.Filter.Tag.Value },
                And = rule.Filter.And == null ? null : new LifecycleAndOperatorXml
                {
                    Prefix = rule.Filter.And.Prefix,
                    ObjectSizeGreaterThan = rule.Filter.And.ObjectSizeGreaterThan,
                    ObjectSizeLessThan = rule.Filter.And.ObjectSizeLessThan,
                    Tags = rule.Filter.And.Tags.Select(t => new TagXml { Key = t.Key, Value = t.Value }).ToList()
                }
            };
        }

        if (rule.Expiration != null)
        {
            xml.Expiration = new LifecycleExpirationXml
            {
                Days = rule.Expiration.Days,
                Date = rule.Expiration.Date
            };
        }

        if (rule.AbortIncompleteMultipartUpload != null)
        {
            xml.AbortIncompleteMultipartUpload = new LifecycleAbortIncompleteMultipartUploadXml
            {
                DaysAfterInitiation = rule.AbortIncompleteMultipartUpload.DaysAfterInitiation
            };
        }

        return xml;
    }
}
