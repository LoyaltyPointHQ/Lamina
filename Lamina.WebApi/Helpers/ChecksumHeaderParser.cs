using Lamina.Storage.Core.Helpers;
using Microsoft.AspNetCore.Http;

namespace Lamina.WebApi.Helpers;

public record ChecksumParseResult(
    string? Algorithm,
    string? CRC32,
    string? CRC32C,
    string? CRC64NVME,
    string? SHA1,
    string? SHA256,
    string? ErrorCode,
    string? ErrorMessage)
{
    public bool IsValid => ErrorCode == null;

    public static ChecksumParseResult Success(
        string? algorithm = null,
        string? crc32 = null,
        string? crc32c = null,
        string? crc64nvme = null,
        string? sha1 = null,
        string? sha256 = null) =>
        new(algorithm, crc32, crc32c, crc64nvme, sha1, sha256, null, null);

    public static ChecksumParseResult Error(string errorCode, string errorMessage) =>
        new(null, null, null, null, null, null, errorCode, errorMessage);
}

public static class ChecksumHeaderParser
{
    public static ChecksumParseResult Parse(IHeaderDictionary headers)
    {
        headers.TryGetValue("x-amz-checksum-algorithm", out var algorithmValues);
        headers.TryGetValue("x-amz-checksum-crc32", out var crc32Values);
        headers.TryGetValue("x-amz-checksum-crc32c", out var crc32cValues);
        headers.TryGetValue("x-amz-checksum-crc64nvme", out var crc64nvmeValues);
        headers.TryGetValue("x-amz-checksum-sha1", out var sha1Values);
        headers.TryGetValue("x-amz-checksum-sha256", out var sha256Values);

        var algorithm = NullIfEmpty(algorithmValues.ToString());
        var crc32 = NullIfEmpty(crc32Values.ToString());
        var crc32c = NullIfEmpty(crc32cValues.ToString());
        var crc64nvme = NullIfEmpty(crc64nvmeValues.ToString());
        var sha1 = NullIfEmpty(sha1Values.ToString());
        var sha256 = NullIfEmpty(sha256Values.ToString());

        if (!string.IsNullOrEmpty(algorithm) && !StreamingChecksumCalculator.IsValidAlgorithm(algorithm))
        {
            return ChecksumParseResult.Error(
                "InvalidArgument",
                $"Invalid checksum algorithm: {algorithm}. Valid values are: CRC32, CRC32C, SHA1, SHA256, CRC64NVME");
        }

        return ChecksumParseResult.Success(algorithm, crc32, crc32c, crc64nvme, sha1, sha256);
    }

    private static string? NullIfEmpty(string? value) =>
        string.IsNullOrEmpty(value) ? null : value;
}
