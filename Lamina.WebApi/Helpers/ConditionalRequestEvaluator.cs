namespace Lamina.WebApi.Helpers;

public static class ConditionalRequestEvaluator
{
    // S3 GET/HEAD conditional evaluation.
    // Returns 304 (NotModified), 412 (PreconditionFailed), or null to continue.
    // Evaluation order per RFC 7232: If-Match → If-Unmodified-Since → If-None-Match → If-Modified-Since
    public static int? EvaluateGet(
        string etag,
        DateTime lastModified,
        string? ifMatch,
        string? ifNoneMatch,
        string? ifModifiedSince,
        string? ifUnmodifiedSince)
    {
        var normalizedEtag = NormalizeETag(etag);

        if (!string.IsNullOrEmpty(ifMatch))
        {
            if (!ETagMatches(normalizedEtag, ifMatch))
                return 412;
        }

        if (!string.IsNullOrEmpty(ifUnmodifiedSince))
        {
            if (TryParseDate(ifUnmodifiedSince, out var since) && lastModified > since)
                return 412;
        }

        if (!string.IsNullOrEmpty(ifNoneMatch))
        {
            if (ETagMatches(normalizedEtag, ifNoneMatch))
                return 304;
        }

        if (!string.IsNullOrEmpty(ifModifiedSince))
        {
            if (TryParseDate(ifModifiedSince, out var since) && lastModified <= since)
                return 304;
        }

        return null;
    }

    // Copy-source conditional evaluation (x-amz-copy-source-if-*).
    // Returns 412 (PreconditionFailed) or null to continue.
    public static int? EvaluateCopySource(
        string etag,
        DateTime lastModified,
        string? ifMatch,
        string? ifNoneMatch,
        string? ifModifiedSince,
        string? ifUnmodifiedSince)
    {
        var normalizedEtag = NormalizeETag(etag);

        if (!string.IsNullOrEmpty(ifMatch) && !ETagMatches(normalizedEtag, ifMatch))
            return 412;

        if (!string.IsNullOrEmpty(ifUnmodifiedSince))
        {
            if (TryParseDate(ifUnmodifiedSince, out var since) && lastModified > since)
                return 412;
        }

        if (!string.IsNullOrEmpty(ifNoneMatch) && ETagMatches(normalizedEtag, ifNoneMatch))
            return 412;

        if (!string.IsNullOrEmpty(ifModifiedSince))
        {
            if (TryParseDate(ifModifiedSince, out var since) && lastModified <= since)
                return 412;
        }

        return null;
    }

    private static string NormalizeETag(string etag) =>
        etag.TrimStart('"').TrimEnd('"');

    private static bool ETagMatches(string normalizedEtag, string headerValue)
    {
        if (headerValue == "*")
            return true;

        foreach (var candidate in headerValue.Split(','))
        {
            var normalized = candidate.Trim().TrimStart('"').TrimEnd('"');
            if (string.Equals(normalized, normalizedEtag, StringComparison.Ordinal))
                return true;
        }
        return false;
    }

    private static bool TryParseDate(string value, out DateTime result) =>
        DateTime.TryParse(value, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal,
            out result);
}
