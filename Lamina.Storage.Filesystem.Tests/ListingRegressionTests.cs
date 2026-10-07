using System.Globalization;
using System.Buffers;
using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.Core.Listing;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.InMemory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Lamina.Storage.Filesystem.Tests;

public sealed class ListingRegressionTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "lamina-listing-tests", Guid.NewGuid().ToString("N"));
    private const string Bucket = "bucket";

    private FilesystemObjectDataStorage Filesystem(MetadataStorageMode mode = MetadataStorageMode.Inline,
        string metadataName = ".lamina-meta", string tempPrefix = ".lamina-tmp-")
    {
        var settings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = _root,
            MetadataMode = mode,
            InlineMetadataDirectoryName = metadataName,
            TempFilePrefix = tempPrefix
        });
        return new FilesystemObjectDataStorage(settings,
            new NetworkFileSystemHelper(settings, NullLogger<NetworkFileSystemHelper>.Instance),
            new LinuxZeroCopyHelper(NullLogger<LinuxZeroCopyHelper>.Instance),
            NullLogger<FilesystemObjectDataStorage>.Instance);
    }

    private IObjectDataStorage Storage(bool memory) => memory
        ? new InMemoryObjectDataStorage(NullLogger<InMemoryObjectDataStorage>.Instance)
        : Filesystem();

    private static async Task Store(IObjectDataStorage storage, params string[] keys)
    {
        foreach (var key in keys)
        {
            var pipe = new Pipe();
            await pipe.Writer.WriteAsync(new byte[] { 1 });
            await pipe.Writer.CompleteAsync();
            await storage.StoreDataAsync(Bucket, key, pipe.Reader);
        }
    }

    private void File(string key)
    {
        var path = Path.Combine(_root, Bucket, key);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        System.IO.File.WriteAllBytes(path, []);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DirectoryPagination_ReturnsEveryKeyExactlyOnce(bool memory)
    {
        var storage = Storage(memory);
        var keys = Enumerable.Range(0, 75).Select(i => $"wal_005/{i:D8}.lz4").ToArray();
        await Store(storage, keys);
        var seen = new HashSet<string>();
        string? after = null;
        for (var page = 0; page <= keys.Length; page++)
        {
            var result = await storage.ListDataKeysAsync(Bucket, BucketType.Directory, "wal_005/", "/", after, 3);
            foreach (var key in result.Keys)
                Assert.True(seen.Add(key), $"Repeated key {key} on page {page}");
            if (!result.IsTruncated)
            {
                Assert.Equal(keys.Order(StringComparer.Ordinal), seen.Order(StringComparer.Ordinal));
                return;
            }
            Assert.NotNull(result.StartAfter);
            Assert.NotEqual(after, result.StartAfter);
            after = result.StartAfter;
        }
        Assert.Fail("Pagination did not terminate.");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GroupedPagination_DoesNotRepeatPrefixesOrInventAnotherPage(bool memory)
    {
        var storage = Storage(memory);
        await Store(storage, "a/one", "a/two", "b.txt", "c/one", "c/two");
        var all = new List<string>();
        string? after = null;
        for (var page = 0; page < 4; page++)
        {
            var result = await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose, delimiter: "/", startAfter: after, maxKeys: 1);
            var items = result.Keys.Concat(result.CommonPrefixes).ToArray();
            Assert.Single(items);
            all.AddRange(items);
            if (!result.IsTruncated)
                break;
            after = result.StartAfter;
        }
        Assert.Equal(new[] { "a/", "b.txt", "c/" }, all);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OnlyOneDistinctPrefix_IsNotTruncated(bool memory)
    {
        var storage = Storage(memory);
        await Store(storage, "wal--one", "wal--two", "wal--three");
        var page = await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose, delimiter: "--", maxKeys: 1);
        Assert.Equal(new[] { "wal--" }, page.CommonPrefixes);
        Assert.False(page.IsTruncated);
        Assert.Null(page.StartAfter);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EmptyDelimiter_IsEquivalentToNoDelimiter(bool memory)
    {
        var storage = Storage(memory);
        await Store(storage, "wal/a", "wal/b");
        var page = await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose, "wal/", "");
        Assert.Equal(new[] { "wal/a", "wal/b" }, page.Keys);
        Assert.Empty(page.CommonPrefixes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ZeroLimit_AndCancellation_DoNotEnumerate(bool memory)
    {
        var storage = Storage(memory);
        await Store(storage, "file");
        var page = await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose, maxKeys: 0);
        Assert.Empty(page.Keys);
        Assert.False(page.IsTruncated);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => storage.ListDataKeysAsync(
            Bucket, BucketType.GeneralPurpose, cancellationToken: cancelled.Token));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LexicalOrder_UsesUtf8ScalarOrderAndIsCultureIndependent(bool memory)
    {
        var storage = Storage(memory);
        var expected = new[] { "A", "a", "z", "\uE000", "\U00010000", "\U00010001", "\U0001F600" };
        await Store(storage, expected.Reverse().ToArray());
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("tr-TR");
            var page = await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose);
            Assert.Equal(expected, page.Keys);
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Theory]
    [InlineData(MetadataStorageMode.Inline)]
    [InlineData(MetadataStorageMode.SeparateDirectory)]
    [InlineData(MetadataStorageMode.Xattr)]
    public async Task InternalEntries_AreExcludedIncludingExplicitPrefixes(MetadataStorageMode mode)
    {
        var storage = Filesystem(mode, ".custom-meta", "_pending-");
        foreach (var key in new[] { ".lamina-meta/secret", ".custom-meta/secret", "wal/.lamina-meta/secret",
                     "wal/.custom-meta/secret", "_pending-tree/hidden", "wal/_PENDING-file", "a.tmp", ".lamina-meta-backup/visible", "wal/visible" })
            File(key);

        var page = await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose);
        Assert.Equal(new[] { ".lamina-meta-backup/visible", "a.tmp", "wal/visible" }, page.Keys);
        var grouped = await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose, delimiter: "/");
        Assert.Equal(new[] { ".lamina-meta-backup/", "wal/" }, grouped.CommonPrefixes);
        foreach (var prefix in new[] { ".lamina-meta/", ".custom-meta/", "wal/.lamina-meta/", "_pending-tree/" })
        {
            var hidden = await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose, prefix);
            Assert.Empty(hidden.Keys);
            Assert.Empty(hidden.CommonPrefixes);
            Assert.False(hidden.IsTruncated);
        }
    }

    [Fact]
    public async Task InternalEntries_DoNotConsumeThePageOrSetTruncated()
    {
        var storage = Filesystem();
        File("wal/visible");
        File("wal/.lamina-meta/nested/hidden");
        File("wal/.lamina-tmp-0000");
        var page = await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose, "wal/", "/", maxKeys: 1);
        Assert.Equal(new[] { "wal/visible" }, page.Keys);
        Assert.Empty(page.CommonPrefixes);
        Assert.False(page.IsTruncated);
    }

    [Fact]
    public async Task LargeFlatDirectory_DoesNotAllocateEveryRejectedName()
    {
        var storage = Filesystem();
        for (var i = 0; i < 10_000; i++)
            File($"wal/.lamina-tmp-{i:D8}");
        File("wal/visible");
        await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose, "wal/", "/", maxKeys: 1);
        // Both implementations complete this filesystem operation synchronously.
        var before = GC.GetAllocatedBytesForCurrentThread();
        var page = await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose, "wal/", "/", maxKeys: 1);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(new[] { "wal/visible" }, page.Keys);
        Assert.True(allocated < 1_000_000, $"Allocated {allocated:N0} bytes for a one-key page dominated by temporary files.");
    }

    [Theory]
    [InlineData(".lamina-meta")]
    [InlineData(".custom-meta")]
    public async Task PartialMetadataNamePrefix_DoesNotHideLegalSimilarNames(string prefix)
    {
        var storage = Filesystem(metadataName: ".custom-meta");
        File(prefix + "/hidden");
        File(prefix + "-backup/visible");
        var result = await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose, prefix);
        Assert.Equal(new[] { prefix + "-backup/visible" }, result.Keys);
        var internalOnly = await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose, prefix + "/");
        Assert.Empty(internalOnly.Keys);
    }

    [Fact]
    public async Task MetadataSubtrees_AreNotVisited_AndCandidateLimitIsEnforced()
    {
        var storage = Filesystem();
        for (var i = 0; i < 1500; i++)
        {
            File($"wal/.lamina-meta/deep/{i:D5}");
            File($"wal/{i:D5}");
        }
        var result = await storage.ListDataCandidatesAsync(Bucket, new ListingQuery(BucketType.GeneralPurpose, "wal/", maxKeys: 7));
        Assert.Equal(1501, result.Statistics.ScannedEntries); // The metadata directory, never its children.
        Assert.Equal(1, result.Statistics.ExcludedSubtrees);
        Assert.Equal(8, result.Statistics.PeakCandidates);
        Assert.Equal(Enumerable.Range(0, 8).Select(i => $"wal/{i:D5}"), result.Entries.Select(e => e.Name));
    }

    [Fact]
    public async Task Symlinks_AreListedUnderTheirLogicalName_AndCyclesFail()
    {
        if (OperatingSystem.IsWindows())
            return; // Creating symlinks requires privileges on Windows runners.
        var storage = Filesystem();
        File("original/file");
        var bucketPath = Path.Combine(_root, Bucket);
        Directory.CreateSymbolicLink(Path.Combine(bucketPath, "alias"), Path.Combine(bucketPath, "original"));
        var result = await storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose);
        Assert.Equal(new[] { "alias/file", "original/file" }, result.Keys);
        Directory.CreateSymbolicLink(Path.Combine(bucketPath, "original", "loop"), bucketPath);
        await Assert.ThrowsAsync<IOException>(() => storage.ListDataKeysAsync(Bucket, BucketType.GeneralPurpose));
    }

    [Fact]
    public void KeyBufferRentFailure_DoesNotReturnTheOldBufferTwice()
    {
        _ = Filesystem();
        File(new string('a', 150) + "/" + new string('b', 150));
        var pool = new FailingPool();
        var query = new ListingQuery(BucketType.GeneralPurpose);
        var walker = new FilesystemListingWalker(Path.Combine(_root, Bucket), ".lamina-meta", ".lamina-tmp-",
            query, new ListingPageSelector(query), CancellationToken.None, pool);
        Assert.Throws<OutOfMemoryException>(() => walker.Walk());
        walker.Dispose();
        Assert.Single(pool.Returned);
    }

    private sealed class FailingPool : ArrayPool<char>
    {
        private int _rents;
        public List<char[]> Returned { get; } = [];
        public override char[] Rent(int minimumLength) => ++_rents == 1
            ? new char[minimumLength] : throw new OutOfMemoryException("Simulated rent failure.");
        public override void Return(char[] array, bool clearArray = false) => Returned.Add(array);
    }

    [Fact]
    public void CancellationDuringTraversal_UnwindsEnumeratorsAndReturnsBuffers()
    {
        _ = Filesystem();
        File(new string('a', 150) + "/" + new string('b', 150));
        using var cancellation = new CancellationTokenSource();
        var pool = new CancellingPool(cancellation);
        var query = new ListingQuery(BucketType.GeneralPurpose);
        using (var walker = new FilesystemListingWalker(Path.Combine(_root, Bucket), ".lamina-meta", ".lamina-tmp-",
                   query, new ListingPageSelector(query), cancellation.Token, pool))
        {
            // The second buffer rental happens inside enumeration, not before Walk starts.
            Assert.ThrowsAny<OperationCanceledException>(() => walker.Walk());
        }
        Assert.Equal(2, pool.Returned.Count);
        Assert.NotSame(pool.Returned[0], pool.Returned[1]);
        // No retained directory enumerator should prevent fixture cleanup (also on Windows).
        Directory.Delete(Path.Combine(_root, Bucket), true);
    }

    private sealed class CancellingPool(CancellationTokenSource cancellation) : ArrayPool<char>
    {
        private int _rents;
        public List<char[]> Returned { get; } = [];
        public override char[] Rent(int minimumLength)
        {
            if (++_rents == 2)
                cancellation.Cancel();
            return new char[minimumLength];
        }
        public override void Return(char[] array, bool clearArray = false) => Returned.Add(array);
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
            Directory.Delete(_root, true);
    }
}
