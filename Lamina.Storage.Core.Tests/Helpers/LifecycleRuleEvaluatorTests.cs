using Lamina.Core.Models;
using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.Core.Tests.Helpers;

public class LifecycleRuleEvaluatorTests
{
    private static S3ObjectInfo MakeObject(string key, DateTime lastModified, long size = 100, Dictionary<string, string>? tags = null)
        => new()
        {
            Key = key,
            LastModified = lastModified,
            Size = size,
            Tags = tags ?? new Dictionary<string, string>()
        };

    private static LifecycleRule ExpireDays(int days, LifecycleFilter? filter = null, string? prefix = null, LifecycleRuleStatus status = LifecycleRuleStatus.Enabled)
        => new()
        {
            Status = status,
            Filter = filter,
            Prefix = prefix,
            Expiration = new LifecycleExpiration { Days = days }
        };

    [Fact]
    public void IsEligible_OlderThanDays_ReturnsTrue()
    {
        var obj = MakeObject("k", DateTime.UtcNow.AddDays(-5));
        var rule = ExpireDays(3, filter: new LifecycleFilter { Prefix = "" });

        Assert.True(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, DateTime.UtcNow));
    }

    [Fact]
    public void IsEligible_YoungerThanDays_ReturnsFalse()
    {
        var obj = MakeObject("k", DateTime.UtcNow.AddHours(-1));
        var rule = ExpireDays(3, filter: new LifecycleFilter { Prefix = "" });

        Assert.False(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, DateTime.UtcNow));
    }

    [Fact]
    public void IsEligible_DisabledRule_ReturnsFalse()
    {
        var obj = MakeObject("k", DateTime.UtcNow.AddDays(-100));
        var rule = ExpireDays(1, filter: new LifecycleFilter { Prefix = "" }, status: LifecycleRuleStatus.Disabled);

        Assert.False(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, DateTime.UtcNow));
    }

    [Fact]
    public void IsEligible_PrefixMatches_ReturnsTrue()
    {
        var obj = MakeObject("logs/app.log", DateTime.UtcNow.AddDays(-10));
        var rule = ExpireDays(1, filter: new LifecycleFilter { Prefix = "logs/" });

        Assert.True(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, DateTime.UtcNow));
    }

    [Fact]
    public void IsEligible_PrefixDoesNotMatch_ReturnsFalse()
    {
        var obj = MakeObject("other/file", DateTime.UtcNow.AddDays(-10));
        var rule = ExpireDays(1, filter: new LifecycleFilter { Prefix = "logs/" });

        Assert.False(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, DateTime.UtcNow));
    }

    [Fact]
    public void IsEligible_LegacyRulePrefix_Matches()
    {
        var obj = MakeObject("logs/a", DateTime.UtcNow.AddDays(-10));
        var rule = ExpireDays(1, prefix: "logs/");

        Assert.True(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, DateTime.UtcNow));
    }

    [Fact]
    public void IsEligible_TagFilterMatches_ReturnsTrue()
    {
        var obj = MakeObject("k", DateTime.UtcNow.AddDays(-10), tags: new() { { "env", "prod" } });
        var rule = ExpireDays(1, filter: new LifecycleFilter { Tag = new LifecycleTag { Key = "env", Value = "prod" } });

        Assert.True(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, DateTime.UtcNow));
    }

    [Fact]
    public void IsEligible_TagFilterMismatch_ReturnsFalse()
    {
        var obj = MakeObject("k", DateTime.UtcNow.AddDays(-10), tags: new() { { "env", "dev" } });
        var rule = ExpireDays(1, filter: new LifecycleFilter { Tag = new LifecycleTag { Key = "env", Value = "prod" } });

        Assert.False(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, DateTime.UtcNow));
    }

    [Fact]
    public void IsEligible_ObjectSizeGreaterThan_Matches()
    {
        var obj = MakeObject("k", DateTime.UtcNow.AddDays(-10), size: 2000);
        var rule = ExpireDays(1, filter: new LifecycleFilter { ObjectSizeGreaterThan = 1024 });

        Assert.True(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, DateTime.UtcNow));
    }

    [Fact]
    public void IsEligible_ObjectSizeGreaterThan_Mismatch()
    {
        var obj = MakeObject("k", DateTime.UtcNow.AddDays(-10), size: 500);
        var rule = ExpireDays(1, filter: new LifecycleFilter { ObjectSizeGreaterThan = 1024 });

        Assert.False(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, DateTime.UtcNow));
    }

    [Fact]
    public void IsEligible_AndOperator_AllMatch_ReturnsTrue()
    {
        var obj = MakeObject("logs/big.bin", DateTime.UtcNow.AddDays(-10), size: 5000, tags: new() { { "env", "prod" } });
        var rule = ExpireDays(1, filter: new LifecycleFilter
        {
            And = new LifecycleAndOperator
            {
                Prefix = "logs/",
                Tags = new() { new LifecycleTag { Key = "env", Value = "prod" } },
                ObjectSizeGreaterThan = 1024
            }
        });

        Assert.True(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, DateTime.UtcNow));
    }

    [Fact]
    public void IsEligible_AndOperator_OneMismatch_ReturnsFalse()
    {
        var obj = MakeObject("logs/big.bin", DateTime.UtcNow.AddDays(-10), size: 5000, tags: new() { { "env", "dev" } });
        var rule = ExpireDays(1, filter: new LifecycleFilter
        {
            And = new LifecycleAndOperator
            {
                Prefix = "logs/",
                Tags = new() { new LifecycleTag { Key = "env", Value = "prod" } }
            }
        });

        Assert.False(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, DateTime.UtcNow));
    }

    [Fact]
    public void IsEligible_ExpirationDate_Passed_ReturnsTrue()
    {
        var obj = MakeObject("k", new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var rule = new LifecycleRule
        {
            Status = LifecycleRuleStatus.Enabled,
            Filter = new LifecycleFilter { Prefix = "" },
            Expiration = new LifecycleExpiration { Date = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc) }
        };

        Assert.True(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc)));
    }

    [Fact]
    public void IsEligible_ExpirationDate_NotYet_ReturnsFalse()
    {
        var obj = MakeObject("k", new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var rule = new LifecycleRule
        {
            Status = LifecycleRuleStatus.Enabled,
            Filter = new LifecycleFilter { Prefix = "" },
            Expiration = new LifecycleExpiration { Date = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc) }
        };

        Assert.False(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc)));
    }

    [Fact]
    public void IsEligible_DaysRoundedToMidnightUtc_ReturnsTrue()
    {
        // AWS rounds the eligibility time up to next midnight UTC.
        // Object created 2026-04-14 14:00 UTC, Days=2 → eligible from 2026-04-17 00:00 UTC.
        var created = new DateTime(2026, 4, 14, 14, 0, 0, DateTimeKind.Utc);
        var obj = MakeObject("k", created);
        var rule = ExpireDays(2, filter: new LifecycleFilter { Prefix = "" });

        // At 2026-04-17 00:00:01 UTC - eligible.
        Assert.True(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, new DateTime(2026, 4, 17, 0, 0, 1, DateTimeKind.Utc)));
        // At 2026-04-16 23:59:59 UTC - NOT eligible yet.
        Assert.False(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, new DateTime(2026, 4, 16, 23, 59, 59, DateTimeKind.Utc)));
    }

    [Fact]
    public void IsEligible_NoExpiration_ReturnsFalse()
    {
        var obj = MakeObject("k", DateTime.UtcNow.AddDays(-100));
        var rule = new LifecycleRule
        {
            Status = LifecycleRuleStatus.Enabled,
            Filter = new LifecycleFilter { Prefix = "" }
            // No Expiration and no AbortMPU
        };

        Assert.False(LifecycleRuleEvaluator.IsEligibleForExpiration(obj, rule, DateTime.UtcNow));
    }
}
