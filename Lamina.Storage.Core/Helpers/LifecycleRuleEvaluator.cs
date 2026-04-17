using Lamina.Core.Models;

namespace Lamina.Storage.Core.Helpers;

public static class LifecycleRuleEvaluator
{
    /// <summary>
    /// Determines whether the object is eligible for expiration under the given rule at the specified UTC moment.
    /// Returns false for Disabled rules, non-matching filters, and rules without Expiration action.
    /// </summary>
    public static bool IsEligibleForExpiration(S3ObjectInfo obj, LifecycleRule rule, DateTime nowUtc)
    {
        if (rule.Status != LifecycleRuleStatus.Enabled || rule.Expiration == null)
        {
            return false;
        }

        if (!MatchesFilter(obj, rule))
        {
            return false;
        }

        if (rule.Expiration.Days.HasValue)
        {
            var eligibleAt = EligibilityThresholdForDays(obj.LastModified, rule.Expiration.Days.Value);
            return nowUtc >= eligibleAt;
        }

        if (rule.Expiration.Date.HasValue)
        {
            return nowUtc >= rule.Expiration.Date.Value;
        }

        return false;
    }

    /// <summary>
    /// AWS formula: eligible_at = ceil_to_next_midnight_utc(lastModified + Days).
    /// Given a LastModified of 2026-04-14 14:00 UTC and Days=2, raw = 2026-04-16 14:00 UTC,
    /// rounded up to next midnight = 2026-04-17 00:00 UTC.
    /// </summary>
    private static DateTime EligibilityThresholdForDays(DateTime lastModifiedUtc, int days)
    {
        var normalized = lastModifiedUtc.Kind == DateTimeKind.Utc
            ? lastModifiedUtc
            : DateTime.SpecifyKind(lastModifiedUtc, DateTimeKind.Utc);
        var raw = normalized.AddDays(days);
        return raw.TimeOfDay == TimeSpan.Zero ? raw : raw.Date.AddDays(1);
    }

    private static bool MatchesFilter(S3ObjectInfo obj, LifecycleRule rule)
    {
        // Legacy Rule.Prefix takes precedence when present (AWS backward-compat format).
        if (rule.Prefix != null)
        {
            return string.IsNullOrEmpty(rule.Prefix) || obj.Key.StartsWith(rule.Prefix, StringComparison.Ordinal);
        }

        if (rule.Filter == null)
        {
            return true;
        }

        var f = rule.Filter;

        if (f.And != null)
        {
            return MatchesAnd(obj, f.And);
        }

        if (f.Prefix != null && !string.IsNullOrEmpty(f.Prefix) && !obj.Key.StartsWith(f.Prefix, StringComparison.Ordinal))
        {
            return false;
        }

        if (f.Tag != null && !MatchesTag(obj, f.Tag))
        {
            return false;
        }

        if (f.ObjectSizeGreaterThan.HasValue && obj.Size <= f.ObjectSizeGreaterThan.Value)
        {
            return false;
        }

        if (f.ObjectSizeLessThan.HasValue && obj.Size >= f.ObjectSizeLessThan.Value)
        {
            return false;
        }

        return true;
    }

    private static bool MatchesAnd(S3ObjectInfo obj, LifecycleAndOperator and)
    {
        if (!string.IsNullOrEmpty(and.Prefix) && !obj.Key.StartsWith(and.Prefix, StringComparison.Ordinal))
        {
            return false;
        }

        foreach (var tag in and.Tags)
        {
            if (!MatchesTag(obj, tag))
            {
                return false;
            }
        }

        if (and.ObjectSizeGreaterThan.HasValue && obj.Size <= and.ObjectSizeGreaterThan.Value)
        {
            return false;
        }

        if (and.ObjectSizeLessThan.HasValue && obj.Size >= and.ObjectSizeLessThan.Value)
        {
            return false;
        }

        return true;
    }

    private static bool MatchesTag(S3ObjectInfo obj, LifecycleTag tag)
    {
        return obj.Tags.TryGetValue(tag.Key, out var value) && string.Equals(value, tag.Value, StringComparison.Ordinal);
    }

    /// <summary>
    /// Determines whether the multipart upload is eligible for abort under the rule's AbortIncompleteMultipartUpload action.
    /// </summary>
    public static bool IsEligibleForMultipartAbort(MultipartUpload upload, LifecycleRule rule, DateTime nowUtc)
    {
        if (rule.Status != LifecycleRuleStatus.Enabled || rule.AbortIncompleteMultipartUpload == null)
        {
            return false;
        }

        // Apply prefix filter if set (tag filter is not supported for AbortIncompleteMultipartUpload per AWS spec).
        var prefix = rule.Prefix ?? rule.Filter?.Prefix ?? rule.Filter?.And?.Prefix;
        if (!string.IsNullOrEmpty(prefix) && !upload.Key.StartsWith(prefix, StringComparison.Ordinal))
        {
            return false;
        }

        var daysAfter = rule.AbortIncompleteMultipartUpload.DaysAfterInitiation;
        var eligibleAt = upload.Initiated.AddDays(daysAfter);
        return nowUtc >= eligibleAt;
    }
}
