using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace Lamina.Tests.Storage.Filesystem;

public class FilesystemObjectDataStorageTests : IDisposable
{
    private readonly string _testDataDirectory;
    private readonly string _testMetadataDirectory;
    private readonly FilesystemObjectDataStorage _storage;

    public FilesystemObjectDataStorageTests()
    {
        // Create temporary directories for testing
        _testDataDirectory = Path.Combine(Path.GetTempPath(), $"lamina_test_{Guid.NewGuid():N}");
        _testMetadataDirectory = Path.Combine(Path.GetTempPath(), $"lamina_test_meta_{Guid.NewGuid():N}");

        var settings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = _testDataDirectory,
            MetadataDirectory = _testMetadataDirectory,
            MetadataMode = MetadataStorageMode.SeparateDirectory,
            InlineMetadataDirectoryName = ".lamina-meta"
        });

        _storage = new FilesystemObjectDataStorage(settings, NullLogger<FilesystemObjectDataStorage>.Instance);
    }

    [Fact]
    public async Task DeleteDataAsync_PreservesBucketDirectory_WhenLastObjectDeleted()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        var content = "test content"u8.ToArray();

        // Create bucket directory
        var bucketDirectory = Path.Combine(_testDataDirectory, bucketName);
        Directory.CreateDirectory(bucketDirectory);

        // Store an object
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await pipe.Writer.CompleteAsync();

        await _storage.StoreDataAsync(bucketName, key, pipe.Reader);

        // Verify object exists
        Assert.True(await _storage.DataExistsAsync(bucketName, key));
        Assert.True(Directory.Exists(bucketDirectory));

        // Act - Delete the object (last object in bucket)
        var result = await _storage.DeleteDataAsync(bucketName, key);

        // Assert
        Assert.True(result); // Deletion was successful
        Assert.False(await _storage.DataExistsAsync(bucketName, key)); // Object is gone
        Assert.True(Directory.Exists(bucketDirectory)); // Bucket directory should still exist
    }

    [Fact]
    public async Task DeleteDataAsync_RemovesEmptySubdirectories_ButNotBucketDirectory()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "path/to/nested/object.txt";
        var content = "test content"u8.ToArray();

        // Create bucket directory
        var bucketDirectory = Path.Combine(_testDataDirectory, bucketName);
        var nestedDirectory = Path.Combine(bucketDirectory, "path", "to", "nested");

        // Store an object in nested path
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await pipe.Writer.CompleteAsync();

        await _storage.StoreDataAsync(bucketName, key, pipe.Reader);

        // Verify directories exist
        Assert.True(Directory.Exists(bucketDirectory));
        Assert.True(Directory.Exists(nestedDirectory));
        Assert.True(await _storage.DataExistsAsync(bucketName, key));

        // Act - Delete the object
        var result = await _storage.DeleteDataAsync(bucketName, key);

        // Assert
        Assert.True(result); // Deletion was successful
        Assert.False(await _storage.DataExistsAsync(bucketName, key)); // Object is gone
        Assert.False(Directory.Exists(nestedDirectory)); // Nested directories should be cleaned up
        Assert.True(Directory.Exists(bucketDirectory)); // Bucket directory should still exist
    }

    [Fact]
    public async Task DeleteDataAsync_PreservesNonEmptyDirectories()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key1 = "path/to/object1.txt";
        const string key2 = "path/to/object2.txt";
        var content = "test content"u8.ToArray();

        var bucketDirectory = Path.Combine(_testDataDirectory, bucketName);
        var sharedDirectory = Path.Combine(bucketDirectory, "path", "to");

        // Store two objects in the same directory
        var pipe1 = new Pipe();
        await pipe1.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await pipe1.Writer.CompleteAsync();
        await _storage.StoreDataAsync(bucketName, key1, pipe1.Reader);

        var pipe2 = new Pipe();
        await pipe2.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await pipe2.Writer.CompleteAsync();
        await _storage.StoreDataAsync(bucketName, key2, pipe2.Reader);

        // Verify both objects exist
        Assert.True(await _storage.DataExistsAsync(bucketName, key1));
        Assert.True(await _storage.DataExistsAsync(bucketName, key2));
        Assert.True(Directory.Exists(sharedDirectory));

        // Act - Delete only the first object
        var result = await _storage.DeleteDataAsync(bucketName, key1);

        // Assert
        Assert.True(result); // Deletion was successful
        Assert.False(await _storage.DataExistsAsync(bucketName, key1)); // First object is gone
        Assert.True(await _storage.DataExistsAsync(bucketName, key2)); // Second object still exists
        Assert.True(Directory.Exists(sharedDirectory)); // Shared directory still exists
        Assert.True(Directory.Exists(bucketDirectory)); // Bucket directory still exists
    }

    [Fact]
    public async Task DeleteDataAsync_HandlesInlineMetadataMode_PreservesBucketDirectory()
    {
        // Arrange - Create storage with inline metadata mode
        var settings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = _testDataDirectory,
            MetadataMode = MetadataStorageMode.Inline,
            InlineMetadataDirectoryName = ".lamina-meta"
        });

        var inlineStorage = new FilesystemObjectDataStorage(settings, NullLogger<FilesystemObjectDataStorage>.Instance);

        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        var content = "test content"u8.ToArray();

        var bucketDirectory = Path.Combine(_testDataDirectory, bucketName);

        // Store an object
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await pipe.Writer.CompleteAsync();

        await inlineStorage.StoreDataAsync(bucketName, key, pipe.Reader);

        // Create a metadata directory to simulate inline metadata
        var metadataDir = Path.Combine(bucketDirectory, ".lamina-meta");
        Directory.CreateDirectory(metadataDir);
        var metadataFile = Path.Combine(metadataDir, "test-object.txt.json");
        File.WriteAllText(metadataFile, "{}");

        // Act - Delete the object (which should clean up directories but preserve bucket)
        var dataResult = await inlineStorage.DeleteDataAsync(bucketName, key);

        // Manually delete the metadata file and clean up its directory to simulate what metadata storage would do
        File.Delete(metadataFile);
        // Try to delete the metadata directory if empty
        if (!Directory.EnumerateFileSystemEntries(metadataDir).Any())
        {
            Directory.Delete(metadataDir);
        }

        // Assert
        Assert.True(dataResult); // Data deletion was successful
        Assert.False(await inlineStorage.DataExistsAsync(bucketName, key)); // Object is gone
        Assert.True(Directory.Exists(bucketDirectory)); // Bucket directory should still exist even after metadata cleanup
    }

    public void Dispose()
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
    }
}