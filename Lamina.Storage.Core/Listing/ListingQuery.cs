using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;

namespace Lamina.Storage.Core.Listing;

public enum ListingOrder
{
    Lexicographical,
    DirectoryHashV1
}

public sealed record ListingPosition(ListingOrder Order, string Name);

public readonly record struct ListingEntry(string Name, bool IsCommonPrefix);

/// <summary>Normalized selection parameters shared by every source of listing candidates.</summary>
public sealed class ListingQuery
{
    public ListingQuery(BucketType bucketType, string? prefix = null, string? delimiter = null,
        ListingPosition? after = null, int maxKeys = 1000)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxKeys);
        Order = bucketType switch
        {
            BucketType.GeneralPurpose => ListingOrder.Lexicographical,
            BucketType.Directory => ListingOrder.DirectoryHashV1,
            _ => throw new ArgumentOutOfRangeException(nameof(bucketType))
        };
        if (after != null && after.Order != Order)
            throw new ArgumentException("The continuation position uses a different listing order.", nameof(after));

        Prefix = prefix ?? string.Empty;
        Delimiter = string.IsNullOrEmpty(delimiter) ? null : delimiter;
        After = after;
        MaxKeys = Math.Min(maxKeys, 1000);
    }

    public ListingOrder Order { get; }
    public string Prefix { get; }
    public string? Delimiter { get; }
    public ListingPosition? After { get; }
    public int MaxKeys { get; }
    public int CandidateLimit => MaxKeys == 0 ? 0 : MaxKeys + 1;
}

public sealed class ListingStatistics
{
    public long ScannedEntries { get; set; }
    public long ExcludedEntries { get; set; }
    public long ExcludedSubtrees { get; set; }
    public long MultipartUploads { get; set; }
    public int PeakCandidates { get; set; }
}

public sealed record ListingCandidates(IReadOnlyList<ListingEntry> Entries, ListingStatistics Statistics)
{
    // Retained for internal callers that only need a data page, such as bucket emptiness checks.
    public ListDataResult ToDataPage(int maxKeys)
    {
        var result = new ListDataResult { IsTruncated = Entries.Count > maxKeys };
        foreach (var entry in Entries.Take(maxKeys))
        {
            if (entry.IsCommonPrefix)
                result.CommonPrefixes.Add(entry.Name);
            else
                result.Keys.Add(entry.Name);
            if (result.IsTruncated)
                result.StartAfter = entry.Name;
        }
        return result;
    }
}
