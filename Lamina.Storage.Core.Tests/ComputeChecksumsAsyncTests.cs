using System.IO.Pipelines;
using System.Text;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.Storage.InMemory;
using Moq;

namespace Lamina.Storage.Core.Tests;

/// <summary>
/// Verifies ComputeChecksumsAsync on IObjectDataStorage. The method is what lets metadata
/// storages heal stale metadata without reaching into the data backend's physical layout.
/// </summary>
public class ComputeChecksumsAsyncTests
{
    private const string BucketName = "checksum-bucket";
    private const string Key = "file.bin";
    private static readonly byte[] Payload = Encoding.UTF8.GetBytes("hello world");

    // Known base64-encoded checksums for "hello world":
    //   CRC32  (IEEE)    : 0x0d4a1185  →  "DUoRhQ=="
    //   SHA256           :  base64 of raw digest
    // These are verified below by a fixed fixture - we only assert they are non-empty
    // and stable (same input produces same output across calls).

    private static async Task<IObjectDataStorage> CreateInMemoryStorageWithDataAsync()
    {
        var storage = new InMemoryObjectDataStorage(Mock.Of<IChunkedDataParser>());
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(Payload);
        await pipe.Writer.CompleteAsync();

        var prepared = await storage.PrepareDataAsync(BucketName, Key, pipe.Reader,
            chunkValidator: null, checksumRequest: null);
        await storage.CommitPreparedDataAsync(prepared.Value!);
        return storage;
    }

    [Fact]
    public async Task ComputeChecksumsAsync_EmptyAlgorithms_ReturnsEmptyDictionary()
    {
        var storage = await CreateInMemoryStorageWithDataAsync();

        var result = await storage.ComputeChecksumsAsync(BucketName, Key, Array.Empty<string>());

        Assert.Empty(result);
    }

    [Fact]
    public async Task ComputeChecksumsAsync_NonExistentObject_ReturnsEmptyDictionary()
    {
        var storage = new InMemoryObjectDataStorage(Mock.Of<IChunkedDataParser>());

        var result = await storage.ComputeChecksumsAsync(BucketName, Key, new[] { "CRC32", "SHA256" });

        Assert.Empty(result);
    }

    [Fact]
    public async Task ComputeChecksumsAsync_ReturnsRequestedAlgorithms()
    {
        var storage = await CreateInMemoryStorageWithDataAsync();

        var result = await storage.ComputeChecksumsAsync(BucketName, Key, new[] { "CRC32", "SHA256" });

        Assert.Equal(2, result.Count);
        Assert.True(result.ContainsKey("CRC32"));
        Assert.True(result.ContainsKey("SHA256"));
        Assert.False(string.IsNullOrWhiteSpace(result["CRC32"]));
        Assert.False(string.IsNullOrWhiteSpace(result["SHA256"]));
    }

    [Fact]
    public async Task ComputeChecksumsAsync_DeterministicForSameInput()
    {
        var storage = await CreateInMemoryStorageWithDataAsync();

        var first = await storage.ComputeChecksumsAsync(BucketName, Key, new[] { "CRC32", "SHA256" });
        var second = await storage.ComputeChecksumsAsync(BucketName, Key, new[] { "CRC32", "SHA256" });

        Assert.Equal(first["CRC32"], second["CRC32"]);
        Assert.Equal(first["SHA256"], second["SHA256"]);
    }

    [Fact]
    public async Task ComputeChecksumsAsync_MatchesStreamingCalculatorOutput()
    {
        // Sanity: the result must match what StreamingChecksumCalculator produces directly on
        // the same payload. This pins the contract: we are computing over the actual object bytes.
        var storage = await CreateInMemoryStorageWithDataAsync();

        using var calculator = new StreamingChecksumCalculator(new List<string> { "CRC32", "SHA256" });
        calculator.Append(Payload);
        var reference = calculator.Finish().CalculatedChecksums;

        var result = await storage.ComputeChecksumsAsync(BucketName, Key, new[] { "CRC32", "SHA256" });

        Assert.Equal(reference["CRC32"], result["CRC32"]);
        Assert.Equal(reference["SHA256"], result["SHA256"]);
    }

    [Fact]
    public async Task ComputeETagAndChecksumsAsync_ReturnsETagMatchingComputeETagAsync()
    {
        var storage = await CreateInMemoryStorageWithDataAsync();

        var (etag, _) = await storage.ComputeETagAndChecksumsAsync(BucketName, Key, Array.Empty<string>());
        var expectedETag = await storage.ComputeETagAsync(BucketName, Key);

        Assert.Equal(expectedETag, etag);
    }

    [Fact]
    public async Task ComputeETagAndChecksumsAsync_ReturnsChecksumsMatchingComputeChecksumsAsync()
    {
        var storage = await CreateInMemoryStorageWithDataAsync();

        var (_, checksums) = await storage.ComputeETagAndChecksumsAsync(BucketName, Key, new[] { "CRC32", "SHA256" });
        var expectedChecksums = await storage.ComputeChecksumsAsync(BucketName, Key, new[] { "CRC32", "SHA256" });

        Assert.Equal(expectedChecksums["CRC32"], checksums["CRC32"]);
        Assert.Equal(expectedChecksums["SHA256"], checksums["SHA256"]);
    }

    [Fact]
    public async Task ComputeETagAndChecksumsAsync_NonExistentObject_ReturnsNullETagAndEmptyChecksums()
    {
        var storage = new InMemoryObjectDataStorage(Mock.Of<IChunkedDataParser>());

        var (etag, checksums) = await storage.ComputeETagAndChecksumsAsync(BucketName, Key, new[] { "CRC32" });

        Assert.Null(etag);
        Assert.Empty(checksums);
    }

    [Fact]
    public async Task ComputeETagAndChecksumsAsync_EmptyAlgorithms_ReturnsETagWithEmptyChecksums()
    {
        var storage = await CreateInMemoryStorageWithDataAsync();

        var (etag, checksums) = await storage.ComputeETagAndChecksumsAsync(BucketName, Key, Array.Empty<string>());

        Assert.NotNull(etag);
        Assert.Empty(checksums);
    }
}
