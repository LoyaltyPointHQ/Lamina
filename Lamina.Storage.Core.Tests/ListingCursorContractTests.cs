using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Listing;
using Lamina.Storage.InMemory;
using Lamina.WebApi.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Lamina.Storage.Core.Tests;

public class ListingCursorContractTests
{
    private const string BucketName = "bucket";
    private const string Prefix = "wal_005/";

    [Fact]
    public async Task DirectoryCursor_ResumesAcrossIndependentReplicasAndRestarts_WithPageSizeChanges()
    {
        var keys = Enumerable.Range(0, 37).Select(i => Prefix + i.ToString("D4")).ToArray();
        var visited = new List<string>();
        string? token = null;
        var pageNumber = 0;
        do
        {
            // Each page uses a fresh facade and backend, populated in a different order.
            // There is no shared cache, cursor state or dictionary instance between requests.
            var replica = await CreateReplicaAsync(pageNumber % 2 == 0 ? keys : keys.Reverse());
            var result = await replica.ListObjectsAsync(BucketName, Request(token, pageNumber % 2 == 0 ? 3 : 7));
            Assert.True(result.IsSuccess);
            var page = Assert.IsType<ListObjectsResponse>(result.Value);
            visited.AddRange(page.Contents.Select(item => item.Key));
            Assert.InRange(page.Contents.Count, 1, pageNumber % 2 == 0 ? 3 : 7);
            token = page.NextContinuationToken;
            Assert.Equal(page.IsTruncated, token is not null);
            Assert.True(++pageNumber < 20, "Cursor failed to make progress.");
        } while (token is not null);

        Assert.Equal(keys.Length, visited.Count);
        Assert.Equal(keys.Order(StringComparer.Ordinal), visited.Order(StringComparer.Ordinal));
        Assert.Equal(keys.Length, visited.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task DirectoryCursor_ContinuesWhenBoundaryObjectWasDeletedBeforeRestart()
    {
        var keys = Enumerable.Range(0, 15).Select(i => Prefix + i.ToString("D4")).ToArray();
        var firstReplica = await CreateReplicaAsync(keys);
        var first = (await firstReplica.ListObjectsAsync(BucketName, Request(null, 4))).Value!;
        Assert.True(first.IsTruncated);
        var boundary = first.Contents[^1].Key;
        var remaining = keys.Where(key => key != boundary).Reverse().ToArray();
        var visited = first.Contents.Select(item => item.Key).ToList();
        var token = first.NextContinuationToken;
        var count = 0;
        while (token is not null)
        {
            var restartedReplica = await CreateReplicaAsync(remaining);
            var result = await restartedReplica.ListObjectsAsync(BucketName, Request(token, 3));
            Assert.True(result.IsSuccess);
            visited.AddRange(result.Value!.Contents.Select(item => item.Key));
            token = result.Value.NextContinuationToken;
            Assert.True(++count < 10);
        }
        // Deleted boundary was already returned; it must not prevent visiting later keys.
        Assert.Equal(keys.Order(StringComparer.Ordinal), visited.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task DirectoryCursor_HashCollisionUsesFullKeyTieBreaker()
    {
        const string first = "wal_005/039E5A50523FEBE4476345EC";
        const string second = "wal_005/D11964BC68707CFC54E4A316";
        Assert.Equal(3133902065u, ListingNameComparer.DirectoryHash(first));
        Assert.Equal(3133902065u, ListingNameComparer.DirectoryHash(second));
        var replica = await CreateReplicaAsync([second, first]);
        var page = (await replica.ListObjectsAsync(BucketName, Request(null, 1))).Value!;
        Assert.Equal(first, Assert.Single(page.Contents).Key);
        Assert.True(page.IsTruncated);

        var restarted = await CreateReplicaAsync([first, second]);
        var next = (await restarted.ListObjectsAsync(BucketName, Request(page.NextContinuationToken, 1))).Value!;
        Assert.Equal(second, Assert.Single(next.Contents).Key);
        Assert.False(next.IsTruncated);
        Assert.Null(next.NextContinuationToken);
    }

    [Fact]
    public async Task MetadataIsFetchedForFinalPageOnly_NotLookaheadCandidate()
    {
        var data = new Mock<IObjectDataStorage>(MockBehavior.Strict);
        data.Setup(x => x.GetDataInfoAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync((1L, DateTime.UnixEpoch));
        var metadata = new Mock<IObjectMetadataStorage>(MockBehavior.Strict);
        var uploads = new Mock<IMultipartUploadStorageFacade>(MockBehavior.Strict);
        data.Setup(x => x.ListDataCandidatesAsync(BucketName, It.IsAny<ListingQuery>(), default))
            .ReturnsAsync(new ListingCandidates([new("a", false), new("b", false), new("c", false)], new()));
        metadata.Setup(x => x.GetMetadataAsync(BucketName, "a", default)).ReturnsAsync(new ObjectMetadataSnapshot(new S3ObjectInfo { Key = "a", ETag = "stored", Size = 1 }, DateTime.MaxValue));
        metadata.Setup(x => x.GetMetadataAsync(BucketName, "b", default)).ReturnsAsync(new ObjectMetadataSnapshot(new S3ObjectInfo { Key = "b", ETag = "stored", Size = 1 }, DateTime.MaxValue));
        var facade = CreateFacade(data.Object, metadata.Object, uploads.Object, BucketType.GeneralPurpose);
        var result = await facade.ListObjectsAsync(BucketName, new ListObjectsRequest { ListType = 2, MaxKeys = 2 });
        Assert.True(result.IsSuccess);
        Assert.Equal(new[] { "a", "b" }, result.Value!.Contents.Select(item => item.Key));
        Assert.True(result.Value.IsTruncated);
        metadata.Verify(x => x.GetMetadataAsync(BucketName, "a", default), Times.Once);
        metadata.Verify(x => x.GetMetadataAsync(BucketName, "b", default), Times.Once);
        metadata.VerifyNoOtherCalls();
        uploads.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task BatchMetadataExcludesLookaheadAndCommonPrefixes()
    {
        var data = new Mock<IObjectDataStorage>(MockBehavior.Strict);
        data.Setup(x => x.GetDataInfoAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync((1L, DateTime.UnixEpoch));
        var metadata = new Mock<IObjectMetadataStorage>(MockBehavior.Strict);
        var batch = metadata.As<IBatchObjectMetadataStorage>();
        data.Setup(x => x.ListDataCandidatesAsync(BucketName, It.IsAny<ListingQuery>(), default))
            .ReturnsAsync(new ListingCandidates([new("a", false), new("b/", true), new("c", false)], new()));
        batch.Setup(x => x.GetMetadataBatchAsync(BucketName,
                It.Is<IEnumerable<string>>(keys => keys.SequenceEqual(new[] { "a" })), default))
            .ReturnsAsync(new Dictionary<string, ObjectMetadataSnapshot?> { ["a"] = new(new S3ObjectInfo { Key = "a", ETag = "stored", Size = 1 }, DateTime.MaxValue) });
        var facade = CreateFacade(data.Object, metadata.Object, Mock.Of<IMultipartUploadStorageFacade>(), BucketType.GeneralPurpose);
        var result = await facade.ListObjectsAsync(BucketName, new ListObjectsRequest { ListType = 2, MaxKeys = 2, Delimiter = "/" });
        Assert.True(result.IsSuccess);
        Assert.True(result.Value!.IsTruncated);
        Assert.Equal("a", Assert.Single(result.Value.Contents).Key);
        Assert.Equal("b/", Assert.Single(result.Value.CommonPrefixes));
        batch.Verify(x => x.GetMetadataBatchAsync(BucketName,
            It.Is<IEnumerable<string>>(keys => keys.SequenceEqual(new[] { "a" })), default), Times.Once);
        metadata.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ZeroLimitDoesNotEnumerateDataUploadsOrMetadata()
    {
        var data = new Mock<IObjectDataStorage>(MockBehavior.Strict);
        data.Setup(x => x.GetDataInfoAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync((1L, DateTime.UnixEpoch));
        var metadata = new Mock<IObjectMetadataStorage>(MockBehavior.Strict);
        var uploads = new Mock<IMultipartUploadStorageFacade>(MockBehavior.Strict);
        var facade = CreateFacade(data.Object, metadata.Object, uploads.Object);
        var request = Request(null, 0);
        request.Delimiter = "/";
        var result = await facade.ListObjectsAsync(BucketName, request);
        Assert.True(result.IsSuccess);
        Assert.Empty(result.Value!.Contents);
        Assert.Empty(result.Value.CommonPrefixes);
        Assert.False(result.Value.IsTruncated);
        data.VerifyNoOtherCalls();
        metadata.VerifyNoOtherCalls();
        uploads.VerifyNoOtherCalls();
    }

    [Theory]
    [InlineData("data")]
    [InlineData("uploads")]
    [InlineData("metadata")]
    public async Task CancellationIsPropagatedAcrossListingStages(string stage)
    {
        using var cancellation = new CancellationTokenSource();
        var data = new Mock<IObjectDataStorage>(MockBehavior.Strict);
        data.Setup(x => x.GetDataInfoAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync((1L, DateTime.UnixEpoch));
        var metadata = new Mock<IObjectMetadataStorage>(MockBehavior.Strict);
        var uploads = new Mock<IMultipartUploadStorageFacade>(MockBehavior.Strict);
        data.Setup(x => x.ListDataCandidatesAsync(BucketName, It.IsAny<ListingQuery>(), cancellation.Token))
            .Returns(() =>
            {
                if (stage == "data")
                {
                    cancellation.Cancel();
                    return Task.FromCanceled<ListingCandidates>(cancellation.Token);
                }
                return Task.FromResult(new ListingCandidates([new(Prefix + "key", false)], new()));
            });
        if (stage == "uploads")
            uploads.Setup(x => x.EnumerateUploadKeysAsync(BucketName, cancellation.Token))
                .Returns(CancelledUploads(cancellation, cancellation.Token));
        if (stage == "metadata")
            metadata.Setup(x => x.GetMetadataAsync(BucketName, Prefix + "key", cancellation.Token))
                .Returns(() =>
                {
                    cancellation.Cancel();
                    return Task.FromCanceled<ObjectMetadataSnapshot?>(cancellation.Token);
                });
        var facade = CreateFacade(data.Object, metadata.Object, uploads.Object);
        var request = Request(null, 2);
        if (stage == "uploads") request.Delimiter = "/";
        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => facade.ListObjectsAsync(BucketName, request, cancellation.Token));
        Assert.Equal(cancellation.Token, exception.CancellationToken);
    }

    private static async IAsyncEnumerable<string> CancelledUploads(CancellationTokenSource cancellation,
        [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        cancellation.Cancel();
        token.ThrowIfCancellationRequested();
        yield break;
    }

    private static ListObjectsRequest Request(string? token, int limit) => new()
    {
        ListType = 2,
        Prefix = Prefix,
        MaxKeys = limit,
        ContinuationToken = token
    };

    private static async Task<ObjectStorageFacade> CreateReplicaAsync(IEnumerable<string> keys)
    {
        var data = new InMemoryObjectDataStorage(NullLogger<InMemoryObjectDataStorage>.Instance);
        foreach (var key in keys)
        {
            using var bytes = new MemoryStream([1]);
            var reader = PipeReader.Create(bytes);
            await data.StoreDataAsync(BucketName, key, reader);
            await reader.CompleteAsync();
        }
        var metadata = new Mock<IObjectMetadataStorage>(MockBehavior.Strict);
        metadata.Setup(x => x.GetMetadataAsync(BucketName, It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string _, string key, CancellationToken _) => new ObjectMetadataSnapshot(new S3ObjectInfo { Key = key, ETag = "stored", Size = 1 }, DateTime.MaxValue));
        return CreateFacade(data, metadata.Object, Mock.Of<IMultipartUploadStorageFacade>());
    }

    private static ObjectStorageFacade CreateFacade(IObjectDataStorage data, IObjectMetadataStorage metadata,
        IMultipartUploadStorageFacade uploads, BucketType bucketType = BucketType.Directory)
    {
        var buckets = new Mock<IBucketStorageFacade>();
        buckets.Setup(x => x.GetBucketAsync(BucketName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new Bucket { Name = BucketName, Type = bucketType });
        return new ObjectStorageFacade(data, metadata, buckets.Object, uploads,
            NullLogger<ObjectStorageFacade>.Instance, new FileExtensionContentTypeDetector());
    }
}
