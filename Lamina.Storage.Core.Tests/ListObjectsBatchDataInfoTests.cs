using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Listing;
using Lamina.WebApi.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Lamina.Storage.Core.Tests;

public class ListObjectsBatchDataInfoTests
{
    [Fact]
    public async Task ListingBatchesOnlyFinalPageAndDoesNotRepeatIndividualStats()
    {
        var data = new Mock<IObjectDataStorage>();
        var batch = data.As<IBatchObjectDataInfoStorage>();
        data.Setup(x => x.ListDataCandidatesAsync("b", It.IsAny<ListingQuery>(), default))
            .ReturnsAsync(new ListingCandidates([new("a", false), new("b", false)], new()));
        batch.Setup(x => x.GetDataInfoBatchAsync("b", It.IsAny<IReadOnlyList<string>>(), default))
            .ReturnsAsync((string bucket, IReadOnlyList<string> keys, CancellationToken ct) =>
            {
                Assert.Equal(new[] { "a" }, keys);
                return new Dictionary<string, (long size, DateTime lastModified)?> { ["a"] = (3, DateTime.UnixEpoch) };
            });
        var metadata = new Mock<IObjectMetadataStorage>();
        metadata.Setup(x => x.GetMetadataAsync("b", "a", default)).ReturnsAsync(new ObjectMetadataSnapshot(
            new() { Key = "a", ETag = "etag", Size = 3 }, DateTime.UnixEpoch));
        var facade = new ObjectStorageFacade(data.Object, metadata.Object, Mock.Of<IBucketStorageFacade>(),
            Mock.Of<IMultipartUploadStorageFacade>(), NullLogger<ObjectStorageFacade>.Instance, new FileExtensionContentTypeDetector());
        var result = await facade.ListObjectsAsync("b", new ListObjectsRequest { MaxKeys = 1 });
        Assert.True(result.IsSuccess);
        Assert.Equal("a", Assert.Single(result.Value!.Contents).Key);
        batch.Verify(x => x.GetDataInfoBatchAsync("b", It.IsAny<IReadOnlyList<string>>(), default), Times.Once);
        data.Verify(x => x.GetDataInfoAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}
