using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.InMemory;

public class InMemoryMultipartUploadDataStorage : IMultipartUploadDataStorage
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, UploadPart>> _uploadParts = new();

    public async Task<StorageResult<UploadPart>> StorePartDataAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, ChecksumRequest? checksumRequest, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        var parts = _uploadParts.GetOrAdd(uploadKey, _ => new ConcurrentDictionary<int, UploadPart>());

        var combinedData = await PipeReaderHelper.ReadAllBytesAsync(dataReader, false, cancellationToken);

        var etag = ETagHelper.ComputeETag(combinedData);

        var part = new UploadPart
        {
            PartNumber = partNumber,
            ETag = etag,
            Size = combinedData.Length,
            LastModified = DateTime.UtcNow,
            Data = combinedData
        };

        // Calculate checksums if requested
        if (checksumRequest != null)
        {
            using var calculator = new StreamingChecksumCalculator(checksumRequest.Algorithm, checksumRequest.ProvidedChecksums);
            if (calculator.HasChecksums)
            {
                calculator.Append(combinedData);
                var result = calculator.Finish();

                if (!result.IsValid)
                {
                    // Validation failed - return error
                    return StorageResult<UploadPart>.Error("InvalidChecksum", result.ErrorMessage ?? "Checksum validation failed");
                }

                // Populate checksum fields from calculated values
                if (result.CalculatedChecksums.TryGetValue("CRC32", out var crc32))
                    part.ChecksumCRC32 = crc32;
                if (result.CalculatedChecksums.TryGetValue("CRC32C", out var crc32c))
                    part.ChecksumCRC32C = crc32c;
                if (result.CalculatedChecksums.TryGetValue("CRC64NVME", out var crc64))
                    part.ChecksumCRC64NVME = crc64;
                if (result.CalculatedChecksums.TryGetValue("SHA1", out var sha1))
                    part.ChecksumSHA1 = sha1;
                if (result.CalculatedChecksums.TryGetValue("SHA256", out var sha256))
                    part.ChecksumSHA256 = sha256;
            }
        }

        parts[partNumber] = part;
        return StorageResult<UploadPart>.Success(part);
    }

    public async Task<StorageResult<UploadPart>> StorePartDataAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkedDataParser chunkedDataParser, IChunkSignatureValidator chunkValidator, ChecksumRequest? checksumRequest, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        var parts = _uploadParts.GetOrAdd(uploadKey, _ => new ConcurrentDictionary<int, UploadPart>());

        // For in-memory storage, using MemoryStream is acceptable
        using var memoryStream = new MemoryStream();

        var parseResult = await chunkedDataParser.ParseChunkedDataToStreamAsync(dataReader, memoryStream, chunkValidator, null, cancellationToken);

        // Check if validation succeeded
        if (!parseResult.Success)
        {
            // Invalid chunk signature
            return StorageResult<UploadPart>.Error("SignatureDoesNotMatch", "Chunk signature validation failed");
        }

        var combinedData = memoryStream.ToArray();
        var etag = ETagHelper.ComputeETag(combinedData);

        var part = new UploadPart
        {
            PartNumber = partNumber,
            ETag = etag,
            Size = combinedData.Length,
            LastModified = DateTime.UtcNow,
            Data = combinedData
        };

        // Calculate checksums if requested
        if (checksumRequest != null)
        {
            using var calculator = new StreamingChecksumCalculator(checksumRequest.Algorithm, checksumRequest.ProvidedChecksums);
            if (calculator.HasChecksums)
            {
                calculator.Append(combinedData);
                var result = calculator.Finish();

                if (!result.IsValid)
                {
                    // Validation failed - return error
                    return StorageResult<UploadPart>.Error("InvalidChecksum", result.ErrorMessage ?? "Checksum validation failed");
                }

                // Populate checksum fields from calculated values
                if (result.CalculatedChecksums.TryGetValue("CRC32", out var crc32))
                    part.ChecksumCRC32 = crc32;
                if (result.CalculatedChecksums.TryGetValue("CRC32C", out var crc32c))
                    part.ChecksumCRC32C = crc32c;
                if (result.CalculatedChecksums.TryGetValue("CRC64NVME", out var crc64))
                    part.ChecksumCRC64NVME = crc64;
                if (result.CalculatedChecksums.TryGetValue("SHA1", out var sha1))
                    part.ChecksumSHA1 = sha1;
                if (result.CalculatedChecksums.TryGetValue("SHA256", out var sha256))
                    part.ChecksumSHA256 = sha256;
            }
        }

        parts[partNumber] = part;
        return StorageResult<UploadPart>.Success(part);
    }

    public Task<IEnumerable<PipeReader>> GetPartReadersAsync(string bucketName, string key, string uploadId, List<CompletedPart> parts, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        if (!_uploadParts.TryGetValue(uploadKey, out var storedParts))
        {
            return Task.FromResult(Enumerable.Empty<PipeReader>());
        }

        var orderedParts = parts.OrderBy(p => p.PartNumber).ToList();
        var readers = new List<PipeReader>();

        foreach (var completePart in orderedParts)
        {
            if (!storedParts.TryGetValue(completePart.PartNumber, out var part) || part.Data == null)
            {
                // Clean up any readers we've already created
                foreach (var r in readers)
                {
                    _ = r.CompleteAsync();
                }
                return Task.FromResult(Enumerable.Empty<PipeReader>());
            }

            // Create a PipeReader from the in-memory data
            var stream = new MemoryStream(part.Data);
            var partReader = PipeReader.Create(stream);
            readers.Add(partReader);
        }

        return Task.FromResult<IEnumerable<PipeReader>>(readers);
    }

    public Task<bool> DeleteAllPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        return Task.FromResult(_uploadParts.TryRemove(uploadKey, out _));
    }

    public Task<List<UploadPart>> GetStoredPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var uploadKey = $"{bucketName}/{key}/{uploadId}";
        if (_uploadParts.TryGetValue(uploadKey, out var parts))
        {
            return Task.FromResult(parts.Values.OrderBy(p => p.PartNumber).ToList());
        }
        return Task.FromResult(new List<UploadPart>());
    }

}