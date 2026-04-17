using System.Xml;
using System.Xml.Serialization;
using Lamina.Core.Models;

namespace Lamina.WebApi.Services;

public static class LifecycleConfigurationParser
{
    public const int MaxRules = 1000;
    public const int MaxIdLength = 255;

    public static LifecycleParseResult Parse(string xmlContent)
    {
        if (string.IsNullOrWhiteSpace(xmlContent))
        {
            return LifecycleParseResult.Failure("Empty lifecycle configuration body.");
        }

        // Reject unsupported elements early using raw XML inspection (XmlSerializer silently ignores unknown ones with our ShouldSerialize pattern).
        var rejectionResult = CheckUnsupportedElements(xmlContent);
        if (rejectionResult.HasValue)
        {
            return rejectionResult.Value;
        }

        LifecycleConfigurationXml? xmlDoc;
        try
        {
            var serializer = new XmlSerializer(typeof(LifecycleConfigurationXml));
            using var reader = new StringReader(xmlContent);
            xmlDoc = serializer.Deserialize(reader) as LifecycleConfigurationXml;
        }
        catch (Exception ex)
        {
            return LifecycleParseResult.Failure($"The XML you provided was not well-formed or did not validate against our published schema: {ex.Message}");
        }

        if (xmlDoc == null)
        {
            return LifecycleParseResult.Failure("The XML you provided was not well-formed or did not validate against our published schema.");
        }

        if (xmlDoc.Rules.Count > MaxRules)
        {
            return LifecycleParseResult.Failure($"Lifecycle configuration cannot exceed {MaxRules} rules.");
        }

        if (xmlDoc.Rules.Count == 0)
        {
            return LifecycleParseResult.Failure("Lifecycle configuration must contain at least one rule.");
        }

        var config = new LifecycleConfiguration();
        foreach (var ruleXml in xmlDoc.Rules)
        {
            var ruleResult = ConvertRule(ruleXml);
            if (!ruleResult.IsSuccess)
            {
                return LifecycleParseResult.Failure(ruleResult.ErrorMessage!);
            }
            config.Rules.Add(ruleResult.Rule!);
        }

        return LifecycleParseResult.Success(config);
    }

    private static LifecycleParseResult? CheckUnsupportedElements(string xml)
    {
        try
        {
            var doc = new XmlDocument();
            doc.LoadXml(xml);
            var unsupported = new[] { "Transition", "NoncurrentVersionExpiration", "NoncurrentVersionTransition" };
            foreach (var name in unsupported)
            {
                var nodes = doc.GetElementsByTagName(name);
                if (nodes.Count > 0)
                {
                    return LifecycleParseResult.NotImplemented(
                        $"Lifecycle element '{name}' is not supported by this server (no versioning or storage class transitions).");
                }
            }

            // ExpiredObjectDeleteMarker is only meaningful with versioning; reject it too.
            var edm = doc.GetElementsByTagName("ExpiredObjectDeleteMarker");
            if (edm.Count > 0)
            {
                return LifecycleParseResult.NotImplemented(
                    "Lifecycle element 'ExpiredObjectDeleteMarker' is not supported by this server (no versioning).");
            }
        }
        catch (XmlException)
        {
            // Let the main parser surface the malformed XML error.
            return null;
        }

        return null;
    }

    private static RuleConvertResult ConvertRule(LifecycleRuleXml ruleXml)
    {
        if (!string.IsNullOrEmpty(ruleXml.Id) && ruleXml.Id.Length > MaxIdLength)
        {
            return RuleConvertResult.Failure($"Rule ID exceeds maximum length of {MaxIdLength} characters.");
        }

        LifecycleRuleStatus status;
        if (string.Equals(ruleXml.Status, "Enabled", StringComparison.Ordinal))
        {
            status = LifecycleRuleStatus.Enabled;
        }
        else if (string.Equals(ruleXml.Status, "Disabled", StringComparison.Ordinal))
        {
            status = LifecycleRuleStatus.Disabled;
        }
        else
        {
            return RuleConvertResult.Failure($"Invalid rule status '{ruleXml.Status}'. Allowed values: Enabled, Disabled.");
        }

        var hasFilter = ruleXml.Filter != null;
        var hasLegacyPrefix = ruleXml.Prefix != null;
        if (!hasFilter && !hasLegacyPrefix)
        {
            return RuleConvertResult.Failure("Rule must contain a Filter or legacy Prefix element.");
        }

        LifecycleFilter? filter = null;
        if (hasFilter)
        {
            var filterResult = ConvertFilter(ruleXml.Filter!);
            if (!filterResult.IsSuccess)
            {
                return RuleConvertResult.Failure(filterResult.ErrorMessage!);
            }
            filter = filterResult.Filter;
        }

        LifecycleExpiration? expiration = null;
        if (ruleXml.Expiration != null)
        {
            var expResult = ConvertExpiration(ruleXml.Expiration);
            if (!expResult.IsSuccess)
            {
                return RuleConvertResult.Failure(expResult.ErrorMessage!);
            }
            expiration = expResult.Expiration;
        }

        LifecycleAbortIncompleteMultipartUpload? abortMpu = null;
        if (ruleXml.AbortIncompleteMultipartUpload != null)
        {
            if (ruleXml.AbortIncompleteMultipartUpload.DaysAfterInitiation <= 0)
            {
                return RuleConvertResult.Failure("AbortIncompleteMultipartUpload.DaysAfterInitiation must be greater than 0.");
            }
            abortMpu = new LifecycleAbortIncompleteMultipartUpload
            {
                DaysAfterInitiation = ruleXml.AbortIncompleteMultipartUpload.DaysAfterInitiation
            };
        }

        if (expiration == null && abortMpu == null)
        {
            return RuleConvertResult.Failure("Rule must specify at least one action (Expiration or AbortIncompleteMultipartUpload).");
        }

        return RuleConvertResult.Success(new LifecycleRule
        {
            Id = ruleXml.Id,
            Status = status,
            Filter = filter,
            Prefix = hasLegacyPrefix ? ruleXml.Prefix : null,
            Expiration = expiration,
            AbortIncompleteMultipartUpload = abortMpu
        });
    }

    private static FilterConvertResult ConvertFilter(LifecycleFilterXml filterXml)
    {
        var presentCriteria = 0;
        if (filterXml.Prefix != null) presentCriteria++;
        if (filterXml.Tag != null) presentCriteria++;
        if (filterXml.ObjectSizeGreaterThan.HasValue) presentCriteria++;
        if (filterXml.ObjectSizeLessThan.HasValue) presentCriteria++;
        if (filterXml.And != null) presentCriteria++;

        if (presentCriteria > 1)
        {
            return FilterConvertResult.Failure("Filter must contain exactly one of Prefix, Tag, ObjectSizeGreaterThan, ObjectSizeLessThan, or And.");
        }

        var filter = new LifecycleFilter
        {
            Prefix = filterXml.Prefix,
            ObjectSizeGreaterThan = filterXml.ObjectSizeGreaterThan,
            ObjectSizeLessThan = filterXml.ObjectSizeLessThan
        };

        if (filterXml.Tag != null)
        {
            filter.Tag = new LifecycleTag { Key = filterXml.Tag.Key, Value = filterXml.Tag.Value };
        }

        if (filterXml.And != null)
        {
            filter.And = new LifecycleAndOperator
            {
                Prefix = filterXml.And.Prefix,
                ObjectSizeGreaterThan = filterXml.And.ObjectSizeGreaterThan,
                ObjectSizeLessThan = filterXml.And.ObjectSizeLessThan,
                Tags = filterXml.And.Tags.Select(t => new LifecycleTag { Key = t.Key, Value = t.Value }).ToList()
            };
        }

        return FilterConvertResult.Success(filter);
    }

    private static ExpirationConvertResult ConvertExpiration(LifecycleExpirationXml expXml)
    {
        var hasDays = expXml.Days.HasValue;
        var hasDate = expXml.Date.HasValue;

        if (hasDays && hasDate)
        {
            return ExpirationConvertResult.Failure("Expiration must contain exactly one of Days or Date.");
        }

        if (!hasDays && !hasDate)
        {
            return ExpirationConvertResult.Failure("Expiration must contain Days or Date.");
        }

        if (hasDays && expXml.Days!.Value <= 0)
        {
            return ExpirationConvertResult.Failure("Expiration.Days must be greater than 0.");
        }

        return ExpirationConvertResult.Success(new LifecycleExpiration
        {
            Days = expXml.Days,
            Date = expXml.Date
        });
    }

    private readonly record struct RuleConvertResult(bool IsSuccess, LifecycleRule? Rule, string? ErrorMessage)
    {
        public static RuleConvertResult Success(LifecycleRule rule) => new(true, rule, null);
        public static RuleConvertResult Failure(string message) => new(false, null, message);
    }

    private readonly record struct FilterConvertResult(bool IsSuccess, LifecycleFilter? Filter, string? ErrorMessage)
    {
        public static FilterConvertResult Success(LifecycleFilter filter) => new(true, filter, null);
        public static FilterConvertResult Failure(string message) => new(false, null, message);
    }

    private readonly record struct ExpirationConvertResult(bool IsSuccess, LifecycleExpiration? Expiration, string? ErrorMessage)
    {
        public static ExpirationConvertResult Success(LifecycleExpiration expiration) => new(true, expiration, null);
        public static ExpirationConvertResult Failure(string message) => new(false, null, message);
    }
}

public readonly record struct LifecycleParseResult(bool IsSuccess, LifecycleConfiguration? Configuration, string? ErrorMessage, bool IsNotImplemented)
{
    public static LifecycleParseResult Success(LifecycleConfiguration config) => new(true, config, null, false);
    public static LifecycleParseResult Failure(string message) => new(false, null, message, false);
    public static LifecycleParseResult NotImplemented(string message) => new(false, null, message, true);
}
