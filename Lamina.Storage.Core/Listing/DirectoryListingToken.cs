using System.Text.Json;

namespace Lamina.Storage.Core.Listing;

/// <summary>Self-contained position in DirectoryHashV1 order; contains no server-local state.</summary>
public static class DirectoryListingToken
{
    private const string Prefix = "d1:";
    private sealed record Payload(string Bucket, string Prefix, string? Delimiter, string After);

    public static string Encode(string bucket, ListingQuery query, string after) => Prefix +
        Convert.ToBase64String(JsonSerializer.SerializeToUtf8Bytes(new Payload(bucket, query.Prefix, query.Delimiter, after)));

    public static bool TryDecode(string token, string bucket, string? prefix, string? delimiter,
        out ListingPosition? position)
    {
        position = null;
        if (!token.StartsWith(Prefix, StringComparison.Ordinal))
            return false;
        try
        {
            var payload = JsonSerializer.Deserialize<Payload>(Convert.FromBase64String(token[Prefix.Length..]));
            if (payload == null || payload.Bucket != bucket || payload.Prefix != (prefix ?? string.Empty)
                || payload.Delimiter != (string.IsNullOrEmpty(delimiter) ? null : delimiter)
                || string.IsNullOrEmpty(payload.After) || !payload.After.StartsWith(payload.Prefix, StringComparison.Ordinal))
                return false;

            position = new ListingPosition(ListingOrder.DirectoryHashV1, payload.After);
            return true;
        }
        catch (Exception ex) when (ex is FormatException or JsonException)
        {
            return false;
        }
    }
}
