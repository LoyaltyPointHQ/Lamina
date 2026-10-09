using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.InMemory;
using Lamina.WebApi.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Lamina.Storage.Core.Tests;

public class DefaultOverwriteMetadataTests
{
    [Fact]
    public async Task FailedOldMetadataRemovalDoesNotReportSuccessfulPut()
    {
        var data = new InMemoryObjectDataStorage(NullLogger<InMemoryObjectDataStorage>.Instance);
        var metadata = new Mock<IObjectMetadataStorage>();
        metadata.Setup(x => x.DeleteMetadataAsync("b", "target", default)).ReturnsAsync(false);
        metadata.Setup(x => x.MetadataExistsAsync("b", "target", default)).ReturnsAsync(true);
        var facade = new ObjectStorageFacade(data, metadata.Object, Mock.Of<IBucketStorageFacade>(), Mock.Of<IMultipartUploadStorageFacade>(),
            NullLogger<ObjectStorageFacade>.Instance, new FileExtensionContentTypeDetector());
        await Assert.ThrowsAsync<IOException>(() => facade.PutObjectAsync("b", "target", PipeReader.Create(new MemoryStream([1, 2, 3]))));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DefaultPublicationRemovesMetadataFromPreviousVersion(bool copy)
    {
        var data = new InMemoryObjectDataStorage(NullLogger<InMemoryObjectDataStorage>.Instance);
        var metadata = new InMemoryObjectMetadataStorage();
        var facade = new ObjectStorageFacade(data, metadata, Mock.Of<IBucketStorageFacade>(), Mock.Of<IMultipartUploadStorageFacade>(),
            NullLogger<ObjectStorageFacade>.Instance, new FileExtensionContentTypeDetector());
        await metadata.StoreMetadataAsync("b", "target", "old", 3, new PutObjectRequest { Tags = new() { ["old"] = "tag" } }, lastModified: DateTime.UtcNow.AddDays(1));
        if (copy)
        {
            Assert.True((await facade.PutObjectAsync("b", "source", PipeReader.Create(new MemoryStream([1, 2, 3])))).IsSuccess);
            Assert.NotNull(await facade.CopyObjectAsync("b", "source", "b", "target", "REPLACE", new(), "REPLACE"));
        }
        else Assert.True((await facade.PutObjectAsync("b", "target", PipeReader.Create(new MemoryStream([1, 2, 3])))).IsSuccess);
        Assert.Null(await metadata.GetMetadataAsync("b", "target"));
        var result = await facade.GetObjectInfoAsync("b", "target");
        Assert.NotNull(result);
        Assert.NotEqual("old", result.ETag);
        Assert.Empty(result.Tags);
    }
}
