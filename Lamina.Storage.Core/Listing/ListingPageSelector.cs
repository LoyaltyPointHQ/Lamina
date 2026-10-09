namespace Lamina.Storage.Core.Listing;

/// <summary>Observes projected names before cursor filtering without allocating rejected names.</summary>
public delegate void ListingEntryObserver(ReadOnlySpan<char> name, bool isCommonPrefix);

/// <summary>Retains only the next page plus one distinct lookahead entry.</summary>
public sealed class ListingPageSelector
{
    private readonly ListingQuery _query;
    private readonly ListingEntryObserver? _observer;
    private readonly PriorityQueue<ListingEntry, SortKey> _candidates;
    private readonly HashSet<string> _names = new(StringComparer.Ordinal);
    private readonly uint _afterHash;

    private readonly record struct SortKey(uint Hash, string Name);

    public ListingPageSelector(ListingQuery query, ListingStatistics? statistics = null, ListingEntryObserver? observer = null)
    {
        _query = query;
        _observer = observer;
        Statistics = statistics ?? new ListingStatistics();
        _afterHash = query.Order == ListingOrder.DirectoryHashV1 && query.After != null
            ? ListingNameComparer.DirectoryHash(query.After.Name) : 0;
        _candidates = new PriorityQueue<ListingEntry, SortKey>(query.CandidateLimit,
            Comparer<SortKey>.Create((x, y) => Compare(y.Hash, y.Name, x.Hash, x.Name)));
    }

    public ListingStatistics Statistics { get; }

    public void ConsiderKey(ReadOnlySpan<char> key, bool prefixesOnly = false)
    {
        if (!key.StartsWith(_query.Prefix, StringComparison.Ordinal))
            return;

        if (_query.Delimiter != null)
        {
            var offset = key[_query.Prefix.Length..].IndexOf(_query.Delimiter, StringComparison.Ordinal);
            if (offset >= 0)
            {
                ConsiderEntry(key[..(_query.Prefix.Length + offset + _query.Delimiter.Length)], true);
                return;
            }
        }
        if (!prefixesOnly)
            ConsiderEntry(key, false);
    }

    // Entries passed here have already been grouped and filtered by their source.
    public void ConsiderEntry(ReadOnlySpan<char> name, bool isCommonPrefix)
    {
        _observer?.Invoke(name, isCommonPrefix);
        if (_query.CandidateLimit == 0)
            return;

        var hash = _query.Order == ListingOrder.DirectoryHashV1
            ? ListingNameComparer.DirectoryHash(name) : 0;
        if (_query.After != null && Compare(hash, name, _afterHash, _query.After.Name) <= 0)
            return;

        if (_candidates.Count == _query.CandidateLimit)
        {
            _candidates.TryPeek(out _, out var largest);
            if (Compare(hash, name, largest.Hash, largest.Name) >= 0)
                return;
        }

        // Alternate lookup avoids allocating a string for rejected duplicates.
        if (_names.GetAlternateLookup<ReadOnlySpan<char>>().Contains(name))
            return;
        if (_candidates.Count == _query.CandidateLimit)
            _names.Remove(_candidates.Dequeue().Name);

        var retainedName = name.ToString();
        _names.Add(retainedName);
        _candidates.Enqueue(new ListingEntry(retainedName, isCommonPrefix), new SortKey(hash, retainedName));
        Statistics.PeakCandidates = Math.Max(Statistics.PeakCandidates, _candidates.Count);
    }

    public ListingCandidates Finish()
    {
        var entries = new ListingEntry[_candidates.Count];
        // The max heap yields descending order; fill backwards without another sort.
        for (var i = entries.Length - 1; i >= 0; i--)
            entries[i] = _candidates.Dequeue();
        _names.Clear();
        return new ListingCandidates(entries, Statistics);
    }

    private static int Compare(uint hash, ReadOnlySpan<char> name, uint otherHash, ReadOnlySpan<char> otherName)
    {
        var comparison = hash.CompareTo(otherHash);
        return comparison != 0 ? comparison : ListingNameComparer.Compare(name, otherName);
    }
}
