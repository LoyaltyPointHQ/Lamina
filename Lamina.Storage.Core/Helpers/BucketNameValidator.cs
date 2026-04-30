using System.Text.RegularExpressions;

namespace Lamina.Storage.Core.Helpers;

public static class BucketNameValidator
{
    private static readonly Regex NameRegex = new(@"^[a-z0-9][a-z0-9.-]*[a-z0-9]$", RegexOptions.Compiled);
    private static readonly Regex IpAddressRegex = new(@"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", RegexOptions.Compiled);

    private static readonly string[] ReservedPrefixes = ["xn--", "sthree-", "amzn-s3-demo-"];
    private static readonly string[] ReservedSuffixes = ["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"];

    public static bool IsValid(string bucketName)
    {
        if (string.IsNullOrWhiteSpace(bucketName) || bucketName.Length < 3 || bucketName.Length > 63)
            return false;

        if (!NameRegex.IsMatch(bucketName))
            return false;

        if (bucketName.Contains("..") || bucketName.Contains(".-") || bucketName.Contains("-."))
            return false;

        if (IpAddressRegex.IsMatch(bucketName))
            return false;

        foreach (var prefix in ReservedPrefixes)
            if (bucketName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return false;

        foreach (var suffix in ReservedSuffixes)
            if (bucketName.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
                return false;

        return true;
    }
}
