using System.Text;

namespace Lamina.Storage.Core.Helpers;

public static class ContinuationToken
{
    private const string TokenPrefix = "v1:";

    public static string Encode(string key)
    {
        if (string.IsNullOrEmpty(key))
            return string.Empty;

        var keyBytes = Encoding.UTF8.GetBytes(key);
        var base64 = Convert.ToBase64String(keyBytes);
        return $"{TokenPrefix}{base64}";
    }

    public static string? Decode(string? token)
    {
        if (string.IsNullOrEmpty(token))
            return null;

        if (!token.StartsWith(TokenPrefix, StringComparison.Ordinal))
            return null;

        try
        {
            var base64 = token[TokenPrefix.Length..];
            var keyBytes = Convert.FromBase64String(base64);
            return Encoding.UTF8.GetString(keyBytes);
        }
        catch (FormatException)
        {
            return null;
        }
    }

    public static bool IsOpaqueToken(string? token)
    {
        return !string.IsNullOrEmpty(token) && token.StartsWith(TokenPrefix, StringComparison.Ordinal);
    }
}
