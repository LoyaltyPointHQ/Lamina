using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace Lamina.Storage.Filesystem.Tests;

public class FilesystemMultipartUploadDataStorageTests : IDisposable
{
    private readonly string _testDataDirectory;
    private readonly string _testMetadataDirectory;
    private readonly FilesystemMultipartUploadDataStorage _storage;

    public FilesystemMultipartUploadDataStorageTests()
    {
        _testDataDirectory = Path.Combine(Path.GetTempPath(), $"lamina_mpu_test_{Guid.NewGuid():N}");
        _testMetadataDirectory = Path.Combine(Path.GetTempPath(), $"lamina_mpu_test_meta_{Guid.NewGuid():N}");

        var settings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = _testDataDirectory,
            MetadataDirectory = _testMetadataDirectory,
            MetadataMode = MetadataStorageMode.SeparateDirectory,
            InlineMetadataDirectoryName = ".lamina-meta",
            NetworkMode = NetworkFileSystemMode.None
        });

        var networkHelper = new NetworkFileSystemHelper(settings, NullLogger<NetworkFileSystemHelper>.Instance);
        _storage = new FilesystemMultipartUploadDataStorage(settings, networkHelper, NullLogger<FilesystemMultipartUploadDataStorage>.Instance);
    }

    public void Dispose()
    {
        TryDelete(_testDataDirectory);
        TryDelete(_testMetadataDirectory);
    }

    private static void TryDelete(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task HasAnyPartsAsync_ReturnsFalse_WhenUploadDirectoryMissing()
    {
        var hasAny = await _storage.HasAnyPartsAsync("bucket", "key", "nonexistent-upload");
        Assert.False(hasAny);
    }

    [Fact]
    public async Task HasAnyPartsAsync_ReturnsTrue_AfterPartStored()
    {
        const string bucketName = "bucket";
        const string key = "object";
        const string uploadId = "upload-123";

        await StorePartAsync(bucketName, key, uploadId, partNumber: 1, content: "hello");

        var hasAny = await _storage.HasAnyPartsAsync(bucketName, key, uploadId);
        Assert.True(hasAny);
    }

    [Fact]
    public async Task GetStoredPartsAsync_WithoutKnownMetadata_ComputesEtagFromFile()
    {
        // When no metadata hint is provided the filesystem backend must recompute ETag by reading
        // the part file - preserving backward-compatible behavior for old uploads / fallback.
        const string bucketName = "bucket";
        const string key = "object";
        const string uploadId = "upload-no-hint";

        var storeResult = await StorePartAsync(bucketName, key, uploadId, partNumber: 1, content: "hello");
        var expectedEtag = storeResult.ETag;

        var parts = await _storage.GetStoredPartsAsync(bucketName, key, uploadId, knownMetadata: null);

        var only = Assert.Single(parts);
        Assert.Equal(1, only.PartNumber);
        Assert.Equal(expectedEtag, only.ETag);
        Assert.Equal("hello".Length, only.Size);
    }

    [Fact]
    public async Task GetStoredPartsAsync_WithKnownMetadataEtag_UsesHintInsteadOfRecomputing()
    {
        // This is the core perf optimization: when the caller provides an ETag via metadata hint,
        // the filesystem backend MUST NOT re-read the part file for MD5. We prove it by supplying a
        // deliberately bogus ETag in the hint - if the returned ETag matches that bogus value, we
        // know the file was not rehashed.
        const string bucketName = "bucket";
        const string key = "object";
        const string uploadId = "upload-with-hint";
        const string hintedEtag = "fakeetagthatisnotanymd5";

        await StorePartAsync(bucketName, key, uploadId, partNumber: 1, content: "hello");

        var hint = new Dictionary<int, PartMetadata>
        {
            [1] = new PartMetadata { ETag = hintedEtag }
        };

        var parts = await _storage.GetStoredPartsAsync(bucketName, key, uploadId, hint);

        var only = Assert.Single(parts);
        Assert.Equal(hintedEtag, only.ETag);
    }

    [Fact]
    public async Task GetStoredPartsAsync_WithEmptyHintedEtag_FallsBackToCompute()
    {
        // Data-first resilience: if metadata exists but has no ETag recorded (legacy upload from before
        // this was persisted), fall back to computing from the file rather than returning empty string.
        const string bucketName = "bucket";
        const string key = "object";
        const string uploadId = "upload-empty-hint";

        var storeResult = await StorePartAsync(bucketName, key, uploadId, partNumber: 1, content: "hello");
        var expectedEtag = storeResult.ETag;

        var hint = new Dictionary<int, PartMetadata>
        {
            [1] = new PartMetadata { ETag = string.Empty }
        };

        var parts = await _storage.GetStoredPartsAsync(bucketName, key, uploadId, hint);

        var only = Assert.Single(parts);
        Assert.Equal(expectedEtag, only.ETag);
    }

    [Fact]
    public async Task StorePartDataAsync_MatchingContentMd5_Succeeds()
    {
        const string content = "hello";
        var expectedMd5 = System.Security.Cryptography.MD5.HashData(System.Text.Encoding.UTF8.GetBytes(content));

        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(System.Text.Encoding.UTF8.GetBytes(content));
        await pipe.Writer.CompleteAsync();

        var result = await _storage.StorePartDataAsync("bucket", "key", "upload-md5-ok", 1, pipe.Reader, checksumRequest: null, expectedMd5: expectedMd5);

        Assert.True(result.IsSuccess);
        Assert.Equal(Convert.ToHexString(expectedMd5).ToLowerInvariant(), result.Value!.ETag);
    }

    [Fact]
    public async Task StorePartDataAsync_MismatchingContentMd5_ReturnsBadDigestAndRemovesFile()
    {
        const string content = "hello";
        var wrongMd5 = new byte[16]; // all zeros - definitely not MD5("hello")

        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(System.Text.Encoding.UTF8.GetBytes(content));
        await pipe.Writer.CompleteAsync();

        var result = await _storage.StorePartDataAsync("bucket", "key", "upload-md5-bad", 1, pipe.Reader, checksumRequest: null, expectedMd5: wrongMd5);

        Assert.False(result.IsSuccess);
        Assert.Equal("BadDigest", result.ErrorCode);

        // The orphaned part file must be cleaned up so the upload doesn't leak data on the disk.
        var hasAny = await _storage.HasAnyPartsAsync("bucket", "key", "upload-md5-bad");
        Assert.False(hasAny);
    }

    private async Task<UploadPart> StorePartAsync(string bucketName, string key, string uploadId, int partNumber, string content)
    {
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(System.Text.Encoding.UTF8.GetBytes(content));
        await pipe.Writer.CompleteAsync();

        var result = await _storage.StorePartDataAsync(bucketName, key, uploadId, partNumber, pipe.Reader, checksumRequest: null);
        Assert.True(result.IsSuccess, result.ErrorMessage);
        Assert.NotNull(result.Value);
        return result.Value!;
    }
}
