namespace S3Test.Configuration;

public class StorageLimits
{
    public int MaxBuckets { get; set; } = 100;
    public long MaxObjectSizeBytes { get; set; } = 5L * 1024 * 1024 * 1024; // 5GB
    public long MaxTotalStorageBytes { get; set; } = 100L * 1024 * 1024 * 1024; // 100GB
    public int MaxObjectsPerBucket { get; set; } = 100000;
    public int MaxConcurrentMultipartUploads { get; set; } = 1000;
    public int MaxPartsPerUpload { get; set; } = 10000;
}