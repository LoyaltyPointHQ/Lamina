using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Configuration;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Lamina.Storage.Filesystem.Tests;

public sealed class MetadataEnumerationTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "lamina-enumeration-" + Guid.NewGuid().ToString("N"));

    private InlineObjectMetadataStorage Create(string metadataName)
    {
        var options = Options.Create(new FilesystemStorageSettings { DataDirectory = _root, InlineMetadataDirectoryName = metadataName });
        return new(options, Options.Create(new MetadataCacheSettings { Enabled = false }),
            Mock.Of<IBucketStorageFacade>(), Mock.Of<IObjectDataStorage>(), Mock.Of<IFileSystemLockManager>(),
            new NetworkFileSystemHelper(options, NullLogger<NetworkFileSystemHelper>.Instance),
            NullLogger<InlineObjectMetadataStorage>.Instance);
    }

    private void Seed(string relativePath)
    {
        var path = Path.Combine(_root, relativePath);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, "{}");
    }

    [Theory]
    [InlineData(".lamina-meta")]
    [InlineData("custom-meta")]
    public async Task Inline_DoesNotTreatInternalMetadataRootAsBucket(string metadataName)
    {
        var storage = Create(metadataName);
        Seed($"bucket/{metadataName}/object.json");
        Seed($"{metadataName}/_multipart_uploads/upload/{metadataName}/upload.metadata.json");
        var entries = new List<(string, string)>();
        await foreach (var entry in storage.ListAllMetadataKeysAsync()) entries.Add(entry);
        Assert.Equal(new[] { ("bucket", "object") }, entries);
    }

    [Fact]
    public async Task Inline_DisappearingCurrentDirectory_DoesNotStopOtherBuckets()
    {
        var storage = Create(".lamina-meta");
        Seed("one/.lamina-meta/object.json");
        Seed("two/.lamina-meta/object.json");
        var entries = new List<(string, string)>();
        await foreach (var entry in storage.ListAllMetadataKeysAsync())
        {
            entries.Add(entry);
            Directory.Delete(Path.Combine(_root, entry.bucketName), true);
        }
        Assert.Equal(2, entries.Count);
    }

    [Fact]
    public async Task SeparateDirectory_SkipsMultipartRootAndContinuesAfterNestedDirectoryDisappears()
    {
        var options = Options.Create(new FilesystemStorageSettings { MetadataDirectory = _root });
        var storage = new SeparateDirectoryObjectMetadataStorage(options,
            Options.Create(new MetadataCacheSettings { Enabled = false }),
            Mock.Of<IBucketStorageFacade>(), Mock.Of<IObjectDataStorage>(), Mock.Of<IFileSystemLockManager>(),
            new NetworkFileSystemHelper(options, NullLogger<NetworkFileSystemHelper>.Instance),
            NullLogger<SeparateDirectoryObjectMetadataStorage>.Instance);
        Seed("bucket/one/object.json");
        Seed("bucket/two/object.json");
        Seed("_multipart_uploads/upload/upload.metadata.json");
        var entries = new List<(string, string)>();
        await foreach (var entry in storage.ListAllMetadataKeysAsync())
        {
            Assert.Equal("bucket", entry.bucketName);
            entries.Add(entry);
            Directory.Delete(Path.Combine(_root, entry.bucketName, Path.GetDirectoryName(entry.key)!), true);
        }
        Assert.Equal(2, entries.Count);
    }

    public void Dispose() { if (Directory.Exists(_root)) Directory.Delete(_root, true); }
}
