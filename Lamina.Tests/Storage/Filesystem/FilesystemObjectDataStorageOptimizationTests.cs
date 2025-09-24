using System.IO.Pipelines;
using System.Text;
using Lamina.Models;
using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Streaming.Chunked;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace Lamina.Tests.Storage.Filesystem;

/// <summary>
/// Tests for the optimized delimiter-based listing functionality in FilesystemObjectDataStorage.
/// These tests verify that the single-directory scan optimization works correctly for both
/// general-purpose and directory buckets while maintaining S3 compatibility.
/// </summary>
public class FilesystemObjectDataStorageOptimizationTests : IDisposable
{
    private readonly string _testDataDirectory;
    private readonly FilesystemObjectDataStorage _storage;

    public FilesystemObjectDataStorageOptimizationTests()
    {
        // Create temporary directory for testing
        _testDataDirectory = Path.Combine(Path.GetTempPath(), $"lamina_opt_test_{Guid.NewGuid():N}");

        var settings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = _testDataDirectory,
            MetadataMode = MetadataStorageMode.SeparateDirectory,
            InlineMetadataDirectoryName = ".lamina-meta",
            NetworkMode = NetworkFileSystemMode.None
        });

        var networkHelper = new NetworkFileSystemHelper(settings, NullLogger<NetworkFileSystemHelper>.Instance);
        var mockChunkedDataParser = new Mock<IChunkedDataParser>();
        _storage = new FilesystemObjectDataStorage(settings, networkHelper, NullLogger<FilesystemObjectDataStorage>.Instance, mockChunkedDataParser.Object);
    }

    [Fact]
    public async Task OptimizedListing_WithForwardSlashDelimiter_ReturnsCorrectResults()
    {
        // Arrange
        const string bucketName = "test-bucket";
        await CreateTestHierarchy(bucketName);

        // Act - List with prefix "a/b/c" and delimiter "/"
        var result = await _storage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose, prefix: "a/b/c", delimiter: "/");

        // Assert
        var keys = result.Keys.OrderBy(k => k).ToList();
        var commonPrefixes = result.CommonPrefixes.OrderBy(p => p).ToList();

        // Should find direct files matching "a/b/c*" at the "a/b/" level
        Assert.Contains("a/b/c_important.log", keys);

        // Should find directories matching "a/b/c*" as common prefixes
        Assert.Contains("a/b/c/", commonPrefixes);
        Assert.Contains("a/b/cat/", commonPrefixes);
        Assert.Contains("a/b/coffee/", commonPrefixes);

        // Should NOT contain files from deeper levels (they become common prefixes)
        Assert.DoesNotContain("a/b/c/file1.txt", keys);
        Assert.DoesNotContain("a/b/cat/file.txt", keys);
    }

    [Fact]
    public async Task OptimizedListing_WithRootPrefix_WorksCorrectly()
    {
        // Arrange
        const string bucketName = "test-bucket";
        await CreateTestHierarchy(bucketName);

        // Act - List with prefix "a" and delimiter "/"
        var result = await _storage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose, prefix: "a", delimiter: "/");

        // Assert
        var commonPrefixes = result.CommonPrefixes.OrderBy(p => p).ToList();

        // Should find "a/" as common prefix
        Assert.Contains("a/", commonPrefixes);

        // Should also find any files starting with "a" at root level
        // (none in our test data, but the logic should handle it)
        Assert.Empty(result.Keys);
    }

    [Fact]
    public async Task OptimizedListing_DirectoryBucket_MaintainsFilesystemOrder()
    {
        // Arrange
        const string bucketName = "dir-bucket";

        // Create files in non-lexicographical order to test filesystem ordering
        await StoreObject(bucketName, "docs/zebra.txt", "zebra");
        await StoreObject(bucketName, "docs/alpha.txt", "alpha");
        await StoreObject(bucketName, "docs/beta.txt", "beta");

        // Act - Directory bucket should maintain filesystem enumeration order
        var result = await _storage.ListDataKeysAsync(bucketName, BucketType.Directory, prefix: "docs/", delimiter: "/");

        // Assert - For directory buckets, order depends on filesystem enumeration
        // We just verify all files are present (exact order is filesystem-dependent)
        var keys = result.Keys;
        Assert.Contains("docs/zebra.txt", keys);
        Assert.Contains("docs/alpha.txt", keys);
        Assert.Contains("docs/beta.txt", keys);
        Assert.Equal(3, keys.Count);
    }

    [Fact]
    public async Task OptimizedListing_GeneralPurposeBucket_MaintainsLexicographicalOrder()
    {
        // Arrange
        const string bucketName = "gp-bucket";

        // Create files in non-lexicographical order
        await StoreObject(bucketName, "docs/zebra.txt", "zebra");
        await StoreObject(bucketName, "docs/alpha.txt", "alpha");
        await StoreObject(bucketName, "docs/beta.txt", "beta");

        // Act - General-purpose bucket should sort lexicographically
        var result = await _storage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose, prefix: "docs/", delimiter: "/");

        // Assert - Should be in lexicographical order
        var keys = result.Keys;
        Assert.Equal(new[] { "docs/alpha.txt", "docs/beta.txt", "docs/zebra.txt" }, keys);
    }

    [Fact]
    public async Task OptimizedListing_WithMaxKeys_RespectsLimits()
    {
        // Arrange
        const string bucketName = "max-keys-test";
        await CreateTestHierarchy(bucketName);

        // Act - List with maxKeys=2
        var result = await _storage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose,
            prefix: "a/b/c", delimiter: "/", maxKeys: 2);

        // Assert - Should return at most 2 items total (keys + common prefixes)
        var totalItems = result.Keys.Count + result.CommonPrefixes.Count;
        Assert.True(totalItems <= 2, $"Expected at most 2 items, got {totalItems}");
    }

    [Fact]
    public async Task OptimizedListing_WithStartAfter_RespectsStartAfter()
    {
        // Arrange
        const string bucketName = "start-after-test";
        await CreateTestHierarchy(bucketName);

        // Act - List with startAfter="a/b/cat"
        var result = await _storage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose,
            prefix: "a/b/c", delimiter: "/", startAfter: "a/b/cat");

        // Assert - Should only return items after "a/b/cat"
        var allItems = result.Keys.Concat(result.CommonPrefixes).ToList();
        foreach (var item in allItems)
        {
            Assert.True(string.Compare(item, "a/b/cat", StringComparison.Ordinal) > 0,
                $"Item '{item}' should be after 'a/b/cat'");
        }
    }

    [Fact]
    public async Task OptimizedListing_NonSlashDelimiter_FallsBackToFullEnumeration()
    {
        // Arrange
        const string bucketName = "delimiter-test";
        await StoreObject(bucketName, "a.b.c.file1.txt", "content1");
        await StoreObject(bucketName, "a.b.d.file2.txt", "content2");

        // Act - Use non-slash delimiter (should fall back to full enumeration)
        var result = await _storage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose,
            prefix: "a.b.c", delimiter: ".");

        // Assert - Should still work correctly even with fallback
        // With prefix "a.b.c" and delimiter ".", should find:
        // - "a.b.c.file1.txt" as a direct match (no more delimiters after "a.b.c")
        var allItems = result.Keys.Concat(result.CommonPrefixes).ToList();
        Assert.NotEmpty(allItems);
        Assert.True(allItems.All(item => item.StartsWith("a.b.c")),
            "All items should start with the prefix 'a.b.c'");
    }

    [Fact]
    public async Task OptimizedListing_EmptyPrefix_WorksCorrectly()
    {
        // Arrange
        const string bucketName = "empty-prefix-test";
        await StoreObject(bucketName, "file1.txt", "content1");
        await StoreObject(bucketName, "dir/file2.txt", "content2");

        // Act - Empty prefix with delimiter
        var result = await _storage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose,
            prefix: null, delimiter: "/");

        // Assert
        Assert.Contains("file1.txt", result.Keys);
        Assert.Contains("dir/", result.CommonPrefixes);
    }

    [Fact]
    public async Task OptimizedListing_PrefixWithoutDelimiter_ScansCorrectDirectory()
    {
        // Arrange
        const string bucketName = "no-delimiter-prefix";
        await CreateTestHierarchy(bucketName);

        // Act - Prefix "docs" (no delimiter in prefix) with delimiter "/"
        var result = await _storage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose,
            prefix: "docs", delimiter: "/");

        // Assert - Should scan bucket root for entries starting with "docs"
        // This test verifies the GetParentDirectoryFromPrefix logic
        var allItems = result.Keys.Concat(result.CommonPrefixes);
        foreach (var item in allItems)
        {
            Assert.True(item.StartsWith("docs"), $"Item '{item}' should start with 'docs'");
        }
    }

    [Fact]
    public async Task OptimizedListing_SkipsTemporaryFiles()
    {
        // Arrange
        const string bucketName = "temp-files-test";
        await StoreObject(bucketName, "docs/normal.txt", "content");

        // Create temporary file manually
        var docsDir = Path.Combine(_testDataDirectory, bucketName, "docs");
        var tempFile = Path.Combine(docsDir, ".lamina-tmp-abc123");
        File.WriteAllText(tempFile, "temp content");

        // Act
        var result = await _storage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose,
            prefix: "docs/", delimiter: "/");

        // Assert - Should not include temporary files
        Assert.Contains("docs/normal.txt", result.Keys);
        Assert.DoesNotContain("docs/.lamina-tmp-abc123", result.Keys);
    }

    [Fact]
    public async Task OptimizedListing_SkipsInlineMetadataDirectories()
    {
        // Arrange - Create storage with inline metadata mode
        var inlineSettings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = _testDataDirectory,
            MetadataMode = MetadataStorageMode.Inline,
            InlineMetadataDirectoryName = ".lamina-meta",
            NetworkMode = NetworkFileSystemMode.None
        });

        var networkHelper = new NetworkFileSystemHelper(inlineSettings, NullLogger<NetworkFileSystemHelper>.Instance);
        var mockParser = new Mock<IChunkedDataParser>();
        var inlineStorage = new FilesystemObjectDataStorage(inlineSettings, networkHelper,
            NullLogger<FilesystemObjectDataStorage>.Instance, mockParser.Object);

        const string bucketName = "inline-meta-test";
        await StoreObjectWithStorage(inlineStorage, bucketName, "docs/normal.txt", "content");

        // Create inline metadata directory manually
        var docsDir = Path.Combine(_testDataDirectory, bucketName, "docs");
        var metaDir = Path.Combine(docsDir, ".lamina-meta");
        Directory.CreateDirectory(metaDir);
        File.WriteAllText(Path.Combine(metaDir, "test.json"), "{}");

        // Act
        var result = await inlineStorage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose,
            prefix: "docs/", delimiter: "/");

        // Assert - Should not include metadata directories as common prefixes
        Assert.Contains("docs/normal.txt", result.Keys);
        Assert.DoesNotContain("docs/.lamina-meta/", result.CommonPrefixes);
    }

    [Fact]
    public async Task OptimizedListing_NonExistentDirectory_ReturnsEmpty()
    {
        // Arrange
        const string bucketName = "non-existent-test";

        // Act - Try to list from non-existent prefix
        var result = await _storage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose,
            prefix: "nonexistent/path/", delimiter: "/");

        // Assert - Should return empty results gracefully
        Assert.Empty(result.Keys);
        Assert.Empty(result.CommonPrefixes);
    }

    private async Task CreateTestHierarchy(string bucketName)
    {
        // Create the test hierarchy from our examples
        await StoreObject(bucketName, "a/b/c/file1.txt", "content1");
        await StoreObject(bucketName, "a/b/c/file2.txt", "content2");
        await StoreObject(bucketName, "a/b/cat/file.txt", "cat content");
        await StoreObject(bucketName, "a/b/coffee/file.txt", "coffee content");
        await StoreObject(bucketName, "a/b/c_important.log", "important log");
    }

    private async Task StoreObject(string bucketName, string key, string content)
    {
        await StoreObjectWithStorage(_storage, bucketName, key, content);
    }

    private static async Task StoreObjectWithStorage(FilesystemObjectDataStorage storage, string bucketName, string key, string content)
    {
        var bytes = Encoding.UTF8.GetBytes(content);
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(bytes);
        await pipe.Writer.CompleteAsync();
        await storage.StoreDataAsync(bucketName, key, pipe.Reader);
    }

    public void Dispose()
    {
        // Clean up test directory
        try
        {
            if (Directory.Exists(_testDataDirectory))
            {
                Directory.Delete(_testDataDirectory, recursive: true);
            }
        }
        catch
        {
            // Ignore cleanup errors in tests
        }
    }
}