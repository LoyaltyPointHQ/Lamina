using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace Lamina.Storage.Filesystem.Tests;

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
            InlineMetadataDirectoryName = ".lamina-meta",
            NetworkMode = NetworkFileSystemMode.None
        });

        var networkHelper = new NetworkFileSystemHelper(settings, NullLogger<NetworkFileSystemHelper>.Instance);
        var mockChunkedDataParser = new Mock<IChunkedDataParser>();
        _storage = new FilesystemObjectDataStorage(settings, networkHelper, NullLogger<FilesystemObjectDataStorage>.Instance, mockChunkedDataParser.Object);
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

        var storeResult = await _storage.StoreDataAsync(bucketName, key, pipe.Reader, null, null);
        Assert.True(storeResult.IsSuccess);

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

        var storeResult = await _storage.StoreDataAsync(bucketName, key, pipe.Reader, null, null);
        Assert.True(storeResult.IsSuccess);

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
        var storeResult1 = await _storage.StoreDataAsync(bucketName, key1, pipe1.Reader, null, null);
        Assert.True(storeResult1.IsSuccess);

        var pipe2 = new Pipe();
        await pipe2.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await pipe2.Writer.CompleteAsync();
        var storeResult2 = await _storage.StoreDataAsync(bucketName, key2, pipe2.Reader, null, null);
        Assert.True(storeResult2.IsSuccess);

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
            InlineMetadataDirectoryName = ".lamina-meta",
            NetworkMode = NetworkFileSystemMode.None
        });

        var networkHelper = new NetworkFileSystemHelper(settings, NullLogger<NetworkFileSystemHelper>.Instance);
        var mockChunkedDataParser = new Mock<IChunkedDataParser>();
        var inlineStorage = new FilesystemObjectDataStorage(settings, networkHelper, NullLogger<FilesystemObjectDataStorage>.Instance, mockChunkedDataParser.Object);

        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        var content = "test content"u8.ToArray();

        var bucketDirectory = Path.Combine(_testDataDirectory, bucketName);

        // Store an object
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await pipe.Writer.CompleteAsync();

        var storeResult = await inlineStorage.StoreDataAsync(bucketName, key, pipe.Reader, null, null);
        Assert.True(storeResult.IsSuccess);

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

    [Fact]
    public async Task StoreDataAsync_ThrowsException_WhenKeyMatchesTempFilePrefix()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = ".lamina-tmp-test-object.txt"; // Uses default temp file prefix
        var content = "test content"u8.ToArray();

        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await pipe.Writer.CompleteAsync();

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _storage.StoreDataAsync(bucketName, key, pipe.Reader, null, null));

        Assert.Contains("conflicts with temporary file pattern", exception.Message);
        Assert.Contains(".lamina-tmp-", exception.Message);
    }

    [Fact]
    public async Task StoreDataAsync_ThrowsException_WhenKeyPathContainsTempFilePrefix()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "path/.lamina-tmp-partial/object.txt"; // Temp file prefix in path
        var content = "test content"u8.ToArray();

        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await pipe.Writer.CompleteAsync();

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _storage.StoreDataAsync(bucketName, key, pipe.Reader, null, null));

        Assert.Contains("conflicts with temporary file pattern", exception.Message);
    }

    [Fact]
    public async Task ListDataKeysAsync_ExcludesTemporaryFiles()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string normalKey = "normal-object.txt";
        var content = "test content"u8.ToArray();

        // Store a normal object
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await pipe.Writer.CompleteAsync();
        var storeResult = await _storage.StoreDataAsync(bucketName, normalKey, pipe.Reader, null, null);
        Assert.True(storeResult.IsSuccess);

        // Manually create a temporary file to simulate what happens during write operations
        var bucketDirectory = Path.Combine(_testDataDirectory, bucketName);
        var tempFilePath = Path.Combine(bucketDirectory, ".lamina-tmp-abc123");
        File.WriteAllText(tempFilePath, "temporary content");

        // Act
        var keys = await _storage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose);

        // Assert
        var keyList = keys.Keys;
        Assert.Single(keyList); // Only the normal object should be listed
        Assert.Contains(normalKey, keyList);
        Assert.DoesNotContain(".lamina-tmp-abc123", keyList);
    }

    [Fact]
    public async Task DataExistsAsync_ReturnsFalse_ForTemporaryFiles()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string tempKey = ".lamina-tmp-test123";

        // Manually create a temporary file to simulate what happens during write operations
        var bucketDirectory = Path.Combine(_testDataDirectory, bucketName);
        Directory.CreateDirectory(bucketDirectory);
        var tempFilePath = Path.Combine(bucketDirectory, tempKey);
        File.WriteAllText(tempFilePath, "temporary content");

        // Act
        var exists = await _storage.DataExistsAsync(bucketName, tempKey);

        // Assert
        Assert.False(exists); // Temporary files should be invisible
    }

    [Fact]
    public async Task WriteDataToPipeAsync_ReturnsFalse_ForTemporaryFiles()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string tempKey = ".lamina-tmp-test123";

        // Manually create a temporary file to simulate what happens during write operations
        var bucketDirectory = Path.Combine(_testDataDirectory, bucketName);
        Directory.CreateDirectory(bucketDirectory);
        var tempFilePath = Path.Combine(bucketDirectory, tempKey);
        File.WriteAllText(tempFilePath, "temporary content");

        var pipe = new Pipe();

        // Act
        var result = await _storage.WriteDataToPipeAsync(bucketName, tempKey, pipe.Writer);

        // Assert
        Assert.False(result); // Should return false as if file doesn't exist
    }

    [Fact]
    public async Task GetDataInfoAsync_ReturnsNull_ForTemporaryFiles()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string tempKey = ".lamina-tmp-test123";

        // Manually create a temporary file to simulate what happens during write operations
        var bucketDirectory = Path.Combine(_testDataDirectory, bucketName);
        Directory.CreateDirectory(bucketDirectory);
        var tempFilePath = Path.Combine(bucketDirectory, tempKey);
        File.WriteAllText(tempFilePath, "temporary content");

        // Act
        var info = await _storage.GetDataInfoAsync(bucketName, tempKey);

        // Assert
        Assert.Null(info); // Should return null as if file doesn't exist
    }

    [Fact]
    public async Task DeleteDataAsync_ReturnsFalse_ForTemporaryFiles()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string tempKey = ".lamina-tmp-test123";

        // Manually create a temporary file to simulate what happens during write operations
        var bucketDirectory = Path.Combine(_testDataDirectory, bucketName);
        Directory.CreateDirectory(bucketDirectory);
        var tempFilePath = Path.Combine(bucketDirectory, tempKey);
        File.WriteAllText(tempFilePath, "temporary content");

        // Act
        var result = await _storage.DeleteDataAsync(bucketName, tempKey);

        // Assert
        Assert.False(result); // Should return false as if file doesn't exist
    }

    [Fact]
    public async Task ComputeETagAsync_ReturnsNull_ForTemporaryFiles()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string tempKey = ".lamina-tmp-test123";

        // Manually create a temporary file to simulate what happens during write operations
        var bucketDirectory = Path.Combine(_testDataDirectory, bucketName);
        Directory.CreateDirectory(bucketDirectory);
        var tempFilePath = Path.Combine(bucketDirectory, tempKey);
        File.WriteAllText(tempFilePath, "temporary content");

        // Act
        var etag = await _storage.ComputeETagAsync(bucketName, tempKey);

        // Assert
        Assert.Null(etag); // Should return null as if file doesn't exist
    }

    [Fact]
    public async Task CustomTempFilePrefix_FiltersCorrectly()
    {
        // Arrange - Create storage with custom temp file prefix
        var settings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = _testDataDirectory,
            MetadataDirectory = _testMetadataDirectory,
            MetadataMode = MetadataStorageMode.SeparateDirectory,
            TempFilePrefix = ".custom-temp-",
            NetworkMode = NetworkFileSystemMode.None
        });

        var networkHelper = new NetworkFileSystemHelper(settings, NullLogger<NetworkFileSystemHelper>.Instance);
        var mockChunkedDataParser = new Mock<IChunkedDataParser>();
        var customStorage = new FilesystemObjectDataStorage(settings, networkHelper, NullLogger<FilesystemObjectDataStorage>.Instance, mockChunkedDataParser.Object);

        const string bucketName = "test-bucket";
        const string normalKey = "normal-object.txt";
        var content = "test content"u8.ToArray();

        // Store a normal object
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await pipe.Writer.CompleteAsync();
        var storeResult = await customStorage.StoreDataAsync(bucketName, normalKey, pipe.Reader, null, null);
        Assert.True(storeResult.IsSuccess);

        // Manually create files with different prefixes
        var bucketDirectory = Path.Combine(_testDataDirectory, bucketName);
        var defaultTempFile = Path.Combine(bucketDirectory, ".lamina-tmp-abc123");
        var customTempFile = Path.Combine(bucketDirectory, ".custom-temp-xyz789");
        File.WriteAllText(defaultTempFile, "default temp content");
        File.WriteAllText(customTempFile, "custom temp content");

        // Act
        var keys = await customStorage.ListDataKeysAsync(bucketName, BucketType.GeneralPurpose);
        var defaultTempExists = await customStorage.DataExistsAsync(bucketName, ".lamina-tmp-abc123");
        var customTempExists = await customStorage.DataExistsAsync(bucketName, ".custom-temp-xyz789");

        // Assert
        var keyList = keys.Keys;
        Assert.Contains(normalKey, keyList); // Normal object should be listed
        Assert.Contains(".lamina-tmp-abc123", keyList); // Default prefix should be listed (not filtered)
        Assert.DoesNotContain(".custom-temp-xyz789", keyList); // Custom prefix should be filtered

        Assert.True(defaultTempExists); // Default prefix should be visible (not filtered)
        Assert.False(customTempExists); // Custom prefix should be invisible (filtered)
    }

    [Fact]
    public async Task StoreDataAsync_AcceptsKeysWithDefaultTempPrefix_WhenUsingCustomPrefix()
    {
        // Arrange - Create storage with custom temp file prefix
        var settings = Options.Create(new FilesystemStorageSettings
        {
            DataDirectory = _testDataDirectory,
            MetadataDirectory = _testMetadataDirectory,
            MetadataMode = MetadataStorageMode.SeparateDirectory,
            TempFilePrefix = ".custom-temp-",
            NetworkMode = NetworkFileSystemMode.None
        });

        var networkHelper = new NetworkFileSystemHelper(settings, NullLogger<NetworkFileSystemHelper>.Instance);
        var mockChunkedDataParser = new Mock<IChunkedDataParser>();
        var customStorage = new FilesystemObjectDataStorage(settings, networkHelper, NullLogger<FilesystemObjectDataStorage>.Instance, mockChunkedDataParser.Object);

        const string bucketName = "test-bucket";
        const string key = ".lamina-tmp-legitimate-file.txt"; // This should be allowed with custom prefix
        var content = "test content"u8.ToArray();

        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await pipe.Writer.CompleteAsync();

        // Act & Assert - Should not throw because we're using a different temp prefix
        var storeResult = await customStorage.StoreDataAsync(bucketName, key, pipe.Reader, null, null);
        Assert.True(storeResult.IsSuccess);

        // Verify the object was actually stored and is accessible
        var exists = await customStorage.DataExistsAsync(bucketName, key);
        Assert.True(exists);
    }


    [Fact]
    public async Task WriteDataToPipeAsync_WithByteRange_ReadsOnlySpecifiedBytes()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "byte-range-test.txt";
        var content = Encoding.UTF8.GetBytes("0123456789ABCDEFGHIJ"); // 20 bytes

        var storePipe = new Pipe();
        await storePipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await storePipe.Writer.CompleteAsync();

        var storeResult = await _storage.StoreDataAsync(bucketName, key, storePipe.Reader, null, null);
        Assert.True(storeResult.IsSuccess);

        var readPipe = new Pipe();

        // Act - Read bytes 5-14 (middle 10 bytes: "56789ABCDE")
        var success = await _storage.WriteDataToPipeAsync(bucketName, key, readPipe.Writer, 5, 14, default);
        Assert.True(success);
        await readPipe.Writer.CompleteAsync();

        // Read the result
        var resultBuffer = new List<byte>();
        while (true)
        {
            var result = await readPipe.Reader.ReadAsync();
            var buffer = result.Buffer;

            foreach (var segment in buffer)
            {
                resultBuffer.AddRange(segment.ToArray());
            }

            readPipe.Reader.AdvanceTo(buffer.End);

            if (result.IsCompleted)
            {
                break;
            }
        }

        await readPipe.Reader.CompleteAsync();

        // Assert
        Assert.Equal(10, resultBuffer.Count);
        Assert.Equal("56789ABCDE", Encoding.UTF8.GetString(resultBuffer.ToArray()));
    }

    [Fact]
    public async Task WriteDataToPipeAsync_MultipleConcurrentByteRangeReads_AllSucceed()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "large-file.bin";

        // Create a large file with distinct regions
        var contentBuilder = new StringBuilder();
        contentBuilder.Append(new string('A', 10000)); // Bytes 0-9999
        contentBuilder.Append(new string('B', 10000)); // Bytes 10000-19999
        contentBuilder.Append(new string('C', 10000)); // Bytes 20000-29999
        var content = Encoding.UTF8.GetBytes(contentBuilder.ToString());

        var storePipe = new Pipe();
        await storePipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await storePipe.Writer.CompleteAsync();

        var storeResult = await _storage.StoreDataAsync(bucketName, key, storePipe.Reader, null, null);
        Assert.True(storeResult.IsSuccess);

        // Act - Read three different byte ranges concurrently (simulating parallel UploadPartCopy)
        var pipe1 = new Pipe();
        var pipe2 = new Pipe();
        var pipe3 = new Pipe();

        var write1Task = _storage.WriteDataToPipeAsync(bucketName, key, pipe1.Writer, 0, 9999, default);
        var write2Task = _storage.WriteDataToPipeAsync(bucketName, key, pipe2.Writer, 10000, 19999, default);
        var write3Task = _storage.WriteDataToPipeAsync(bucketName, key, pipe3.Writer, 20000, 29999, default);

        // Read from all pipes concurrently
        var read1Task = Task.Run(async () =>
        {
            var buffer = new List<byte>();
            while (true)
            {
                var result = await pipe1.Reader.ReadAsync();
                var readBuffer = result.Buffer;

                foreach (var segment in readBuffer)
                {
                    buffer.AddRange(segment.ToArray());
                }

                pipe1.Reader.AdvanceTo(readBuffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }

            await pipe1.Reader.CompleteAsync();
            return buffer.ToArray();
        });

        var read2Task = Task.Run(async () =>
        {
            var buffer = new List<byte>();
            while (true)
            {
                var result = await pipe2.Reader.ReadAsync();
                var readBuffer = result.Buffer;

                foreach (var segment in readBuffer)
                {
                    buffer.AddRange(segment.ToArray());
                }

                pipe2.Reader.AdvanceTo(readBuffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }

            await pipe2.Reader.CompleteAsync();
            return buffer.ToArray();
        });

        var read3Task = Task.Run(async () =>
        {
            var buffer = new List<byte>();
            while (true)
            {
                var result = await pipe3.Reader.ReadAsync();
                var readBuffer = result.Buffer;

                foreach (var segment in readBuffer)
                {
                    buffer.AddRange(segment.ToArray());
                }

                pipe3.Reader.AdvanceTo(readBuffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }

            await pipe3.Reader.CompleteAsync();
            return buffer.ToArray();
        });

        await Task.WhenAll(write1Task, write2Task, write3Task);
        var result1 = await read1Task;
        var result2 = await read2Task;
        var result3 = await read3Task;

        // Assert
        Assert.Equal(10000, result1.Length);
        Assert.Equal(10000, result2.Length);
        Assert.Equal(10000, result3.Length);

        var str1 = Encoding.UTF8.GetString(result1);
        var str2 = Encoding.UTF8.GetString(result2);
        var str3 = Encoding.UTF8.GetString(result3);

        Assert.True(str1.All(c => c == 'A'));
        Assert.True(str2.All(c => c == 'B'));
        Assert.True(str3.All(c => c == 'C'));
    }

    [Fact]
    public async Task WriteDataToPipeAsync_InvalidByteRange_ReturnsFalse()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-file.txt";
        var content = Encoding.UTF8.GetBytes("0123456789"); // 10 bytes

        var storePipe = new Pipe();
        await storePipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await storePipe.Writer.CompleteAsync();

        var storeResult = await _storage.StoreDataAsync(bucketName, key, storePipe.Reader, null, null);
        Assert.True(storeResult.IsSuccess);

        var readPipe = new Pipe();

        // Act - Try to read beyond file size (bytes 5-100, but file is only 10 bytes)
        var success = await _storage.WriteDataToPipeAsync(bucketName, key, readPipe.Writer, 5, 100, default);

        // Assert
        Assert.False(success);
    }

    [Fact]
    public async Task WriteDataToPipeAsync_ByteRangeWithStartGreaterThanEnd_ReturnsFalse()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-file.txt";
        var content = Encoding.UTF8.GetBytes("0123456789"); // 10 bytes

        var storePipe = new Pipe();
        await storePipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await storePipe.Writer.CompleteAsync();

        var storeResult = await _storage.StoreDataAsync(bucketName, key, storePipe.Reader, null, null);
        Assert.True(storeResult.IsSuccess);

        var readPipe = new Pipe();

        // Act - Invalid range: start > end
        var success = await _storage.WriteDataToPipeAsync(bucketName, key, readPipe.Writer, 8, 3, default);

        // Assert
        Assert.False(success);
    }

    [Fact]
    public async Task WriteDataToPipeAsync_SingleByteRange_ReturnsOneByte()
    {
        // Arrange
        const string bucketName = "test-bucket";
        const string key = "test-file.txt";
        var content = Encoding.UTF8.GetBytes("0123456789"); // 10 bytes

        var storePipe = new Pipe();
        await storePipe.Writer.WriteAsync(new ReadOnlyMemory<byte>(content));
        await storePipe.Writer.CompleteAsync();

        var storeResult = await _storage.StoreDataAsync(bucketName, key, storePipe.Reader, null, null);
        Assert.True(storeResult.IsSuccess);

        var readPipe = new Pipe();

        // Act - Read single byte at position 5 ('5')
        var success = await _storage.WriteDataToPipeAsync(bucketName, key, readPipe.Writer, 5, 5, default);
        Assert.True(success);
        await readPipe.Writer.CompleteAsync();

        // Read the result
        var resultBuffer = new List<byte>();
        while (true)
        {
            var result = await readPipe.Reader.ReadAsync();
            var buffer = result.Buffer;

            foreach (var segment in buffer)
            {
                resultBuffer.AddRange(segment.ToArray());
            }

            readPipe.Reader.AdvanceTo(buffer.End);

            if (result.IsCompleted)
            {
                break;
            }
        }

        await readPipe.Reader.CompleteAsync();

        // Assert
        Assert.Single(resultBuffer);
        Assert.Equal("5", Encoding.UTF8.GetString(resultBuffer.ToArray()));
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