using Lamina.Core.Models;

namespace Lamina.Storage.Core.Helpers;

/// <summary>
/// Validates a <see cref="LifecycleConfiguration"/> domain model against S3 semantic rules
/// that are independent of how it was parsed (XML body, JSON config, etc.).
/// Structural/XML-specific validation (unsupported elements, malformed XML) lives in the XML parser.
/// </summary>
public static class LifecycleConfigurationValidator
{
    public const int MaxRules = 1000;
    public const int MaxIdLength = 255;

    public static LifecycleValidationResult Validate(LifecycleConfiguration? configuration)
    {
        if (configuration == null)
        {
            return LifecycleValidationResult.Invalid("Lifecycle configuration is null.");
        }

        if (configuration.Rules.Count == 0)
        {
            return LifecycleValidationResult.Invalid("Lifecycle configuration must contain at least one rule.");
        }

        if (configuration.Rules.Count > MaxRules)
        {
            return LifecycleValidationResult.Invalid($"Lifecycle configuration cannot exceed {MaxRules} rules.");
        }

        foreach (var rule in configuration.Rules)
        {
            var ruleResult = ValidateRule(rule);
            if (!ruleResult.IsValid)
            {
                return ruleResult;
            }
        }

        return LifecycleValidationResult.Valid();
    }

    private static LifecycleValidationResult ValidateRule(LifecycleRule rule)
    {
        if (!string.IsNullOrEmpty(rule.Id) && rule.Id.Length > MaxIdLength)
        {
            return LifecycleValidationResult.Invalid($"Rule ID exceeds maximum length of {MaxIdLength} characters.");
        }

        if (rule.Expiration == null && rule.AbortIncompleteMultipartUpload == null)
        {
            return LifecycleValidationResult.Invalid("Rule must specify at least one action (Expiration or AbortIncompleteMultipartUpload).");
        }

        if (rule.Expiration != null)
        {
            var hasDays = rule.Expiration.Days.HasValue;
            var hasDate = rule.Expiration.Date.HasValue;

            if (hasDays && hasDate)
            {
                return LifecycleValidationResult.Invalid("Expiration must contain exactly one of Days or Date.");
            }
            if (!hasDays && !hasDate)
            {
                return LifecycleValidationResult.Invalid("Expiration must contain Days or Date.");
            }
            if (hasDays && rule.Expiration.Days!.Value <= 0)
            {
                return LifecycleValidationResult.Invalid("Expiration.Days must be greater than 0.");
            }
        }

        if (rule.AbortIncompleteMultipartUpload != null && rule.AbortIncompleteMultipartUpload.DaysAfterInitiation <= 0)
        {
            return LifecycleValidationResult.Invalid("AbortIncompleteMultipartUpload.DaysAfterInitiation must be greater than 0.");
        }

        return LifecycleValidationResult.Valid();
    }
}

public readonly record struct LifecycleValidationResult(bool IsValid, string? ErrorMessage)
{
    public static LifecycleValidationResult Valid() => new(true, null);
    public static LifecycleValidationResult Invalid(string message) => new(false, message);
}
