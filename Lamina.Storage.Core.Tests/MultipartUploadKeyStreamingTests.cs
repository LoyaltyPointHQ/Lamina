using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Lamina.Storage.Core.Tests;

public class MultipartUploadKeyStreamingTests
{
    [Fact]
    public void Facade_DelegatesStreamAndCancellationWithoutMaterializing()
    {
        var metadata = new Mock<IMultipartUploadMetadataStorage>();
        var stream = new Mock<IAsyncEnumerable<string>>().Object;
        using var cancellation = new CancellationTokenSource();
        metadata.Setup(x => x.EnumerateUploadKeysAsync("bucket", cancellation.Token)).Returns(stream);
        var facade = new MultipartUploadStorageFacade(
            Mock.Of<IMultipartUploadDataStorage>(), metadata.Object, Mock.Of<IObjectDataStorage>(),
            Mock.Of<IObjectMetadataStorage>(), NullLogger<MultipartUploadStorageFacade>.Instance, Mock.Of<IChunkedDataParser>());
        Assert.Same(stream, facade.EnumerateUploadKeysAsync("bucket", cancellation.Token));
        metadata.Verify(x => x.EnumerateUploadKeysAsync("bucket", cancellation.Token), Times.Once);
        metadata.VerifyNoOtherCalls();
    }
}
