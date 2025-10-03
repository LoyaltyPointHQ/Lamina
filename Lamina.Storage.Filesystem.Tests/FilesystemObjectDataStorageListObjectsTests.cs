using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Lamina.Storage.Filesystem.Tests;

public class FilesystemObjectDataStorageListObjectsTests : IAsyncLifetime
{
    private readonly string _testDataDirectory;
    private readonly string _testMetadataDirectory;
    private readonly byte[] _testContent = "test content"u8.ToArray();

    public const string BucketName = "test-bucket";

    private readonly FilesystemObjectDataStorage _storage;

    public FilesystemObjectDataStorageListObjectsTests()
    {
        // Create temporary directories for testing
        _testDataDirectory = Path.Combine(Path.GetTempPath(), $"lamina_test_{Guid.NewGuid():N}");
        _testMetadataDirectory = Path.Combine(Path.GetTempPath(), $"lamina_test_meta_{Guid.NewGuid():N}");

        var settings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = _testDataDirectory,
            MetadataDirectory = _testMetadataDirectory,
            MetadataMode = MetadataStorageMode.SeparateDirectory,
            InlineMetadataDirectoryName = ".lamina-meta",
            NetworkMode = NetworkFileSystemMode.None
        });

        var networkHelper = new NetworkFileSystemHelper(settings, NullLogger<NetworkFileSystemHelper>.Instance);
        var mockChunkedDataParser = new Mock<IChunkedDataParser>();
        _storage = new FilesystemObjectDataStorage(settings, networkHelper, NullLogger<FilesystemObjectDataStorage>.Instance, mockChunkedDataParser.Object);
    }
    public async Task InitializeAsync()
    {
        async Task WriteFile(string key)
        {
            var filePipe = new Pipe();
            await filePipe.Writer.WriteAsync(_testContent);
            await filePipe.Writer.CompleteAsync();
            var result = await _storage.StoreDataAsync(BucketName, key, filePipe.Reader, null);
            if (!result.IsSuccess)
            {
                throw new InvalidOperationException($"Failed to store test data for key '{key}': {result.ErrorMessage}");
            }
        }

        await WriteFile("a/b/c/file1.txt");
        await WriteFile("a/b/c/file2.txt");
        await WriteFile("a/b/c/d/file3.txt");
        await WriteFile("a/b/c_file.txt");
        await WriteFile("a/b/cow.txt");
        await WriteFile("a/b/just_a_file.txt");
        
    }

    [Fact]
    public async Task CorrectlyListsWithPrefixEndingWithSlashAndDelimiter()
    {
        var result = await _storage.ListDataKeysAsync(BucketName, BucketType.GeneralPurpose, "a/b/c/", "/");
        Assert.Equal(2, result.Keys.Count);
        Assert.Equal("a/b/c/file1.txt", result.Keys[0]);
        Assert.Equal("a/b/c/file2.txt", result.Keys[1]);
        Assert.Single(result.CommonPrefixes);
        Assert.Equal("a/b/c/d/", result.CommonPrefixes[0]);
        
        result = await _storage.ListDataKeysAsync(BucketName, BucketType.GeneralPurpose, "a/b/", "/");
        Assert.Equal(3, result.Keys.Count);
        Assert.Equal("a/b/c_file.txt", result.Keys[0]);
        Assert.Equal("a/b/cow.txt", result.Keys[1]);
        Assert.Equal("a/b/just_a_file.txt", result.Keys[2]);
        Assert.Single(result.CommonPrefixes);
        Assert.Equal("a/b/c/", result.CommonPrefixes[0]);
    }
    
    [Fact]
    public async Task CorrectlyListsAllRecursiveWithPrefixEndingWithSlashAndNoDelimiter()
    {
        var result = await _storage.ListDataKeysAsync(BucketName, BucketType.GeneralPurpose, "a/b/c/");
        Assert.Equal(3, result.Keys.Count);
        Assert.Equal("a/b/c/d/file3.txt", result.Keys[0]);
        Assert.Equal("a/b/c/file1.txt", result.Keys[1]);
        Assert.Equal("a/b/c/file2.txt", result.Keys[2]);
        Assert.Empty(result.CommonPrefixes);
        
        result = await _storage.ListDataKeysAsync(BucketName, BucketType.GeneralPurpose, "a/b/c");
        Assert.Equal(5, result.Keys.Count);
        Assert.Equal("a/b/c/d/file3.txt", result.Keys[0]);
        Assert.Equal("a/b/c/file1.txt", result.Keys[1]);
        Assert.Equal("a/b/c/file2.txt", result.Keys[2]);
        Assert.Equal("a/b/c_file.txt", result.Keys[3]);
        Assert.Equal("a/b/cow.txt", result.Keys[4]);
        Assert.Empty(result.CommonPrefixes);
    }

    [Fact]
    public async Task CorrectlyHandlesDelimiterDifferentThanSlash()
    {
        var result = await _storage.ListDataKeysAsync(BucketName, BucketType.GeneralPurpose, "a/b", "_");
        Assert.Equal(4, result.Keys.Count);
        Assert.Equal("a/b/c/d/file3.txt", result.Keys[0]);
        Assert.Equal("a/b/c/file1.txt", result.Keys[1]);
        Assert.Equal("a/b/c/file2.txt", result.Keys[2]);
        Assert.Equal("a/b/cow.txt", result.Keys[3]);
        Assert.Equal(2, result.CommonPrefixes.Count);
        Assert.Equal("a/b/c_", result.CommonPrefixes[0]);
        Assert.Equal("a/b/just_", result.CommonPrefixes[1]);
        
        result = await _storage.ListDataKeysAsync(BucketName, BucketType.GeneralPurpose, "a/b/c_", "_");
        Assert.Single(result.Keys);
        Assert.Equal("a/b/c_file.txt", result.Keys[0]);
        Assert.Empty(result.CommonPrefixes);
    }

    public Task DisposeAsync()
    {
        // Clean up test directories
        try
        {
            if (Directory.Exists(_testDataDirectory))
            {
                Directory.Delete(_testDataDirectory, recursive: true);
            }

            if (Directory.Exists(_testMetadataDirectory))
            {
                Directory.Delete(_testMetadataDirectory, recursive: true);
            }
        }
        catch
        {
            // Ignore cleanup errors in tests
        }
        return Task.CompletedTask;
    }
}