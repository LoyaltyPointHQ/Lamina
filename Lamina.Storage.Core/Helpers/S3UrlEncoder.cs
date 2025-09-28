namespace Lamina.Storage.Core.Helpers;

public static class S3UrlEncoder
{
    /// <summary>
    /// Encodes a string for use in S3 XML responses when encoding-type=url is specified.
    /// This follows the S3-compliant URL encoding rules.
    /// </summary>
    public static string? Encode(string? value)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        // S3 uses percent-encoding for non-ASCII characters
        // We use Uri.EscapeDataString which follows RFC 3986
        // This encodes all characters except unreserved characters (A-Z, a-z, 0-9, -, _, ., ~)
        return Uri.EscapeDataString(value);
    }

    /// <summary>
    /// Conditionally encodes a string based on whether encoding is requested.
    /// </summary>
    public static string? ConditionalEncode(string? value, string? encodingType)
    {
        if (encodingType?.Equals("url", StringComparison.OrdinalIgnoreCase) == true)
        {
            return Encode(value);
        }
        return value;
    }
}