using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.InMemory;
using Lamina.WebApi.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Lamina.Storage.Core.Tests;

public class IntegrationIntegrityPublicationTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), $"lamina-integrity-publication-{Guid.NewGuid():N}");

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    public async Task RejectedOverwrite_PreservesPublishedBytesAndMetadata(bool filesystem, bool multipart, bool invalidMd5)
    {
        var settings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = Path.Combine(_root, "data"),
            MetadataDirectory = Path.Combine(_root, "meta"),
            MetadataMode = MetadataStorageMode.Inline
        });
        IObjectDataStorage objects;
        IMultipartUploadDataStorage parts;
        if (filesystem)
        {
            var network = new NetworkFileSystemHelper(settings, NullLogger<NetworkFileSystemHelper>.Instance);
            objects = new FilesystemObjectDataStorage(settings, network,
                new LinuxZeroCopyHelper(NullLogger<LinuxZeroCopyHelper>.Instance), NullLogger<FilesystemObjectDataStorage>.Instance);
            parts = new FilesystemMultipartUploadDataStorage(settings, network, NullLogger<FilesystemMultipartUploadDataStorage>.Instance);
        }
        else
        {
            objects = new InMemoryObjectDataStorage(NullLogger<InMemoryObjectDataStorage>.Instance);
            parts = new InMemoryMultipartUploadDataStorage();
        }
        var metadata = new InMemoryObjectMetadataStorage();
        var uploadMetadata = new InMemoryMultipartUploadMetadataStorage();
        var uploads = new MultipartUploadStorageFacade(parts, uploadMetadata, objects, metadata,
            NullLogger<MultipartUploadStorageFacade>.Instance, Mock.Of<IChunkedDataParser>());
        var facade = new ObjectStorageFacade(objects, metadata, Mock.Of<IBucketStorageFacade>(), uploads,
            NullLogger<ObjectStorageFacade>.Instance, new FileExtensionContentTypeDetector());
        var expectedError = invalidMd5 ? "BadDigest" : "InvalidChecksum";
        if (multipart)
        {
            var upload = await uploads.InitiateMultipartUploadAsync("bucket", "key", new InitiateMultipartUploadRequest { Key = "key" });
            var original = await uploads.UploadPartAsync("bucket", "key", upload.UploadId, 1, Reader("original"));
            Assert.True(original.IsSuccess);
            var request = invalidMd5 ? null : new ChecksumRequest { ProvidedChecksums = new() { ["SHA256"] = Convert.ToBase64String(new byte[32]) } };
            var rejected = await uploads.UploadPartAsync("bucket", "key", upload.UploadId, 1, Reader("replacement"), request,
                expectedMd5: invalidMd5 ? new byte[16] : null);
            Assert.Equal(expectedError, rejected.ErrorCode);
            var readers = await parts.GetPartReadersAsync("bucket", "key", upload.UploadId, [new CompletedPart { PartNumber = 1 }]);
            Assert.Equal("original"u8.ToArray(), await PipeReaderHelper.ReadAllBytesAsync(Assert.Single(readers), true));
            var after = await uploads.ListPartsAsync("bucket", "key", upload.UploadId);
            Assert.Equal(original.Value!.ETag, Assert.Single(after.Value!).ETag);
        }
        else
        {
            var original = await facade.PutObjectAsync("bucket", "key", Reader("original"), new PutObjectRequest
            {
                Key = "key",
                Metadata = new() { ["marker"] = "original" }
            });
            Assert.True(original.IsSuccess);
            var rejected = await facade.PutObjectAsync("bucket", "key", Reader("replacement"), new PutObjectRequest
            {
                Key = "key",
                ChecksumSHA256 = invalidMd5 ? null : Convert.ToBase64String(new byte[32]),
                Metadata = new() { ["marker"] = "replacement" }
            }, expectedMd5: invalidMd5 ? new byte[16] : null);
            Assert.Equal(expectedError, rejected.ErrorCode);
            await using var stream = await objects.OpenReadAsync("bucket", "key");
            Assert.Equal("original", await new StreamReader(stream!).ReadToEndAsync());
            var after = await metadata.GetMetadataAsync("bucket", "key");
            Assert.Equal(original.Value!.ETag, after!.Metadata.ETag);
            Assert.Equal("original", after.Metadata.Metadata["marker"]);
        }
        if (filesystem) Assert.Empty(Directory.EnumerateFiles(_root, ".lamina-tmp-*", SearchOption.AllDirectories));
    }

    private static PipeReader Reader(string value) => PipeReader.Create(new MemoryStream(System.Text.Encoding.UTF8.GetBytes(value)));

    public void Dispose()
    {
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }
}
