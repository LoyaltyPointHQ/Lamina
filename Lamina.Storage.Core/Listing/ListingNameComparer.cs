using System.Text;

namespace Lamina.Storage.Core.Listing;

/// <summary>S3 compares valid Unicode keys by their UTF-8 bytes (Unicode scalar order).</summary>
public sealed class ListingNameComparer : IComparer<string>
{
    public static ListingNameComparer Instance { get; } = new();

    public int Compare(string? x, string? y) => Compare(x.AsSpan(), y.AsSpan());

    public static int Compare(ReadOnlySpan<char> x, ReadOnlySpan<char> y)
    {
        var length = Math.Min(x.Length, y.Length);
        for (var i = 0; i < length; i++)
        {
            if (x[i] == y[i])
                continue;
            // BMP characters preserve UTF-8 order. Decode at the first differing surrogate
            // to also order supplementary characters after the BMP private-use range.
            if (!char.IsSurrogate(x[i]) && !char.IsSurrogate(y[i]))
                return x[i].CompareTo(y[i]);

            var xv = ScalarAt(x[i..]);
            var yv = ScalarAt(y[i..]);
            var comparison = xv.CompareTo(yv);
            if (comparison != 0)
                return comparison;
        }
        return x.Length.CompareTo(y.Length);
    }

    private static int ScalarAt(ReadOnlySpan<char> value) =>
        value.Length >= 2 && Rune.TryCreate(value[0], value[1], out var rune) ? rune.Value : value[0];

    /// <summary>Frozen FNV-1a over UTF-16 little-endian code units, shared across replicas.</summary>
    public static uint DirectoryHash(ReadOnlySpan<char> key)
    {
        var hash = 2166136261u;
        unchecked
        {
            foreach (var c in key)
            {
                hash = (hash ^ (byte)c) * 16777619u;
                hash = (hash ^ (byte)(c >> 8)) * 16777619u;
            }
        }
        return hash;
    }
}
