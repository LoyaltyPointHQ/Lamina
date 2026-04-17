using System.Web;

namespace Lamina.WebApi.Services;

public static class TaggingHeaderParser
{
    public static TaggingParseResult Parse(string? header)
    {
        var tags = new Dictionary<string, string>();

        if (string.IsNullOrEmpty(header))
        {
            return TaggingParseResult.Success(tags);
        }

        var pairs = header.Split('&');
        foreach (var pair in pairs)
        {
            if (string.IsNullOrEmpty(pair))
            {
                continue;
            }

            var equalsIndex = pair.IndexOf('=');
            if (equalsIndex < 0)
            {
                return TaggingParseResult.Failure(
                    $"Malformed tagging header: segment '{pair}' is missing '='.");
            }

            var rawKey = pair[..equalsIndex];
            var rawValue = pair[(equalsIndex + 1)..];
            var key = HttpUtility.UrlDecode(rawKey);
            var value = HttpUtility.UrlDecode(rawValue);

            if (tags.ContainsKey(key))
            {
                return TaggingParseResult.Failure(
                    $"Duplicate tag key '{key}' in tagging header.");
            }

            tags[key] = value;
        }

        return TaggingParseResult.Success(tags);
    }

    public static string Serialize(Dictionary<string, string> tags)
    {
        if (tags.Count == 0)
        {
            return string.Empty;
        }

        return string.Join("&", tags.Select(kvp =>
            $"{Uri.EscapeDataString(kvp.Key)}={Uri.EscapeDataString(kvp.Value)}"));
    }
}

public readonly record struct TaggingParseResult(bool IsSuccess, Dictionary<string, string>? Tags, string? ErrorMessage)
{
    public static TaggingParseResult Success(Dictionary<string, string> tags) => new(true, tags, null);
    public static TaggingParseResult Failure(string message) => new(false, null, message);
}
