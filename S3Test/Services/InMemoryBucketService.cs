using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using S3Test.Models;

namespace S3Test.Services;

public class InMemoryBucketService : IBucketService
{
    private readonly ConcurrentDictionary<string, Bucket> _buckets = new();
    private static readonly Regex BucketNameRegex = new(@"^[a-z0-9][a-z0-9.-]*[a-z0-9]$", RegexOptions.Compiled);
    private static readonly Regex IpAddressRegex = new(@"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", RegexOptions.Compiled);

    public Task<Bucket?> CreateBucketAsync(string bucketName, CreateBucketRequest? request = null, CancellationToken cancellationToken = default)
    {
        if (!IsValidBucketName(bucketName))
        {
            return Task.FromResult<Bucket?>(null);
        }

        var bucket = new Bucket
        {
            Name = bucketName,
            CreationDate = DateTime.UtcNow,
            Region = request?.Region ?? "us-east-1",
            Tags = new Dictionary<string, string>()
        };

        if (_buckets.TryAdd(bucketName, bucket))
        {
            return Task.FromResult<Bucket?>(bucket);
        }

        return Task.FromResult<Bucket?>(null);
    }

    public Task<Bucket?> GetBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        _buckets.TryGetValue(bucketName, out var bucket);
        return Task.FromResult(bucket);
    }

    public Task<ListBucketsResponse> ListBucketsAsync(CancellationToken cancellationToken = default)
    {
        var response = new ListBucketsResponse
        {
            Buckets = _buckets.Values.OrderBy(b => b.Name).ToList()
        };
        return Task.FromResult(response);
    }

    public Task<bool> DeleteBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_buckets.TryRemove(bucketName, out _));
    }

    public Task<bool> BucketExistsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_buckets.ContainsKey(bucketName));
    }

    public Task<Bucket?> UpdateBucketTagsAsync(string bucketName, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        if (_buckets.TryGetValue(bucketName, out var bucket))
        {
            bucket.Tags = tags ?? new Dictionary<string, string>();
            return Task.FromResult<Bucket?>(bucket);
        }

        return Task.FromResult<Bucket?>(null);
    }

    private static bool IsValidBucketName(string bucketName)
    {
        if (string.IsNullOrWhiteSpace(bucketName))
            return false;

        if (bucketName.Length < 3 || bucketName.Length > 63)
            return false;

        if (!BucketNameRegex.IsMatch(bucketName))
            return false;

        if (bucketName.Contains("..") || bucketName.Contains(".-") || bucketName.Contains("-."))
            return false;

        if (IpAddressRegex.IsMatch(bucketName))
            return false;

        string[] reservedPrefixes = { "xn--", "sthree-", "amzn-s3-demo-" };
        foreach (var prefix in reservedPrefixes)
        {
            if (bucketName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return true;
    }
}