using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Lamina.Storage.Core.Tests;

public class CompleteMetadataFailureTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MetadataFailure_DoesNotCompleteUploadAndCleansOnlyPreparedObject(bool requiresData)
    {
        const string bucket = "bucket", key = "key";
        var upload = Guid.NewGuid().ToString("N");
        const string etag = "900150983cd24fb0d6963f7d28e17f72";
        var data = new Mock<IMultipartUploadDataStorage>();
        var metadata = new Mock<IMultipartUploadMetadataStorage>();
        var objectData = new Mock<IObjectDataStorage>();
        var objectMetadata = new Mock<IObjectMetadataStorage>();
        if (requiresData) objectMetadata.As<IRequiresDataFileForMetadata>();
        data.Setup(x => x.HasAnyPartsAsync(bucket, key, upload, It.IsAny<CancellationToken>())).ReturnsAsync(true);
        data.Setup(x => x.GetStoredPartsAsync(bucket, key, upload, It.IsAny<CancellationToken>()))
            .ReturnsAsync([new UploadPart { PartNumber = 1, ETag = etag, Size = 3 }]);
        var reader = PipeReader.Create(new MemoryStream("abc"u8.ToArray()));
        data.Setup(x => x.GetPartReadersAsync(bucket, key, upload, It.IsAny<List<CompletedPart>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([reader]);
        var prepared = new PreparedData { BucketName = bucket, Key = key, Size = 3 };
        var disposed = false;
        prepared.SetDisposeAction(() => disposed = true);
        objectData.Setup(x => x.PrepareMultipartDataAsync(bucket, key, It.IsAny<IEnumerable<PipeReader>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(prepared);
        objectMetadata.Setup(x => x.StoreMetadataAsync(bucket, key, It.IsAny<string>(), 3,
                It.IsAny<PutObjectRequest>(), It.IsAny<Dictionary<string, string>?>(), It.IsAny<DateTime?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((S3Object?)null);
        var facade = new MultipartUploadStorageFacade(data.Object, metadata.Object, objectData.Object, objectMetadata.Object,
            NullLogger<MultipartUploadStorageFacade>.Instance, Mock.Of<IChunkedDataParser>());
        try
        {
            var result = await facade.CompleteMultipartUploadAsync(bucket, key,
                new CompleteMultipartUploadRequest { UploadId = upload, Parts = [new CompletedPart { PartNumber = 1, ETag = etag }] });
            Assert.False(result.IsSuccess);
            Assert.Equal("InternalError", result.ErrorCode);
            Assert.True(disposed);
            objectData.Verify(x => x.CommitPreparedDataAsync(prepared, It.IsAny<CancellationToken>()), requiresData ? Times.Once : Times.Never);
            objectData.Verify(x => x.DeleteDataAsync(bucket, key, It.IsAny<CancellationToken>()), requiresData ? Times.Once : Times.Never);
            data.Verify(x => x.DeleteAllPartsAsync(bucket, key, upload, It.IsAny<CancellationToken>()), Times.Never);
            metadata.Verify(x => x.DeleteUploadMetadataAsync(bucket, key, upload, It.IsAny<CancellationToken>()), Times.Never);
        }
        finally
        {
            await reader.CompleteAsync();
        }
    }
}
