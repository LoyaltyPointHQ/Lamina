using System.IO.Pipelines;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Microsoft.Extensions.Logging;

namespace Lamina.Storage.Core;

public class MultipartUploadStorageFacade : IMultipartUploadStorageFacade
{
    private readonly IMultipartUploadDataStorage _dataStorage;
    private readonly IMultipartUploadMetadataStorage _metadataStorage;
    private readonly IObjectDataStorage _objectDataStorage;
    private readonly IObjectMetadataStorage _objectMetadataStorage;
    private readonly ILogger<MultipartUploadStorageFacade> _logger;
    private readonly IChunkedDataParser _chunkedDataParser;

    public MultipartUploadStorageFacade(
        IMultipartUploadDataStorage dataStorage,
        IMultipartUploadMetadataStorage metadataStorage,
        IObjectDataStorage objectDataStorage,
        IObjectMetadataStorage objectMetadataStorage,
        ILogger<MultipartUploadStorageFacade> logger,
        IChunkedDataParser chunkedDataParser)
    {
        _dataStorage = dataStorage;
        _metadataStorage = metadataStorage;
        _objectDataStorage = objectDataStorage;
        _objectMetadataStorage = objectMetadataStorage;
        _logger = logger;
        _chunkedDataParser = chunkedDataParser;
    }

    public async Task<MultipartUpload> InitiateMultipartUploadAsync(string bucketName, string key, InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.InitiateUploadAsync(bucketName, key, request, cancellationToken);
    }

    public async Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        // Data-first approach: Store the part data without checking metadata
        // This allows parts to be uploaded even if metadata was lost or corrupted
        return await _dataStorage.StorePartDataAsync(bucketName, key, uploadId, partNumber, dataReader, null, cancellationToken);
    }

    public async Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, CancellationToken cancellationToken = default)
    {
        // If no chunk validator, use the non-validating version
        if (chunkValidator == null)
        {
            return await UploadPartAsync(bucketName, key, uploadId, partNumber, dataReader, cancellationToken);
        }

        // Data-first approach: Don't check metadata, just validate and store the part
        // Use the specialized overload that handles validation and storage in a single pass
        return await _dataStorage.StorePartDataAsync(bucketName, key, uploadId, partNumber, dataReader, _chunkedDataParser, chunkValidator, null, cancellationToken);
    }

    public async Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, ChecksumRequest? checksumRequest, CancellationToken cancellationToken = default)
    {
        // Data-first approach: Store the part data without checking metadata
        // This allows parts to be uploaded even if metadata was lost or corrupted
        var result = await _dataStorage.StorePartDataAsync(bucketName, key, uploadId, partNumber, dataReader, checksumRequest, cancellationToken);

        // Store part checksums in upload metadata if upload succeeded and has checksums
        if (result.IsSuccess && result.Value != null && HasChecksums(result.Value))
        {
            var upload = await _metadataStorage.GetUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);
            if (upload != null)
            {
                upload.Parts[partNumber] = new PartMetadata
                {
                    ChecksumCRC32 = result.Value.ChecksumCRC32,
                    ChecksumCRC32C = result.Value.ChecksumCRC32C,
                    ChecksumCRC64NVME = result.Value.ChecksumCRC64NVME,
                    ChecksumSHA1 = result.Value.ChecksumSHA1,
                    ChecksumSHA256 = result.Value.ChecksumSHA256
                };
                await _metadataStorage.UpdateUploadMetadataAsync(bucketName, key, uploadId, upload, cancellationToken);
            }
        }

        return result;
    }

    private static bool HasChecksums(UploadPart part)
    {
        return !string.IsNullOrEmpty(part.ChecksumCRC32) ||
               !string.IsNullOrEmpty(part.ChecksumCRC32C) ||
               !string.IsNullOrEmpty(part.ChecksumCRC64NVME) ||
               !string.IsNullOrEmpty(part.ChecksumSHA1) ||
               !string.IsNullOrEmpty(part.ChecksumSHA256);
    }

    public async Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, ChecksumRequest? checksumRequest, CancellationToken cancellationToken = default)
    {
        // If no chunk validator, use the version with just checksum request
        if (chunkValidator == null)
        {
            return await UploadPartAsync(bucketName, key, uploadId, partNumber, dataReader, checksumRequest, cancellationToken);
        }

        // Data-first approach: Don't check metadata, just validate and store the part
        // Use the specialized overload that handles validation and storage in a single pass
        var result = await _dataStorage.StorePartDataAsync(bucketName, key, uploadId, partNumber, dataReader, _chunkedDataParser, chunkValidator, checksumRequest, cancellationToken);

        // Store part checksums in upload metadata if upload succeeded and has checksums
        if (result.IsSuccess && result.Value != null && HasChecksums(result.Value))
        {
            var upload = await _metadataStorage.GetUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);
            if (upload != null)
            {
                upload.Parts[partNumber] = new PartMetadata
                {
                    ChecksumCRC32 = result.Value.ChecksumCRC32,
                    ChecksumCRC32C = result.Value.ChecksumCRC32C,
                    ChecksumCRC64NVME = result.Value.ChecksumCRC64NVME,
                    ChecksumSHA1 = result.Value.ChecksumSHA1,
                    ChecksumSHA256 = result.Value.ChecksumSHA256
                };
                await _metadataStorage.UpdateUploadMetadataAsync(bucketName, key, uploadId, upload, cancellationToken);
            }
        }

        return result;
    }

    public async Task<StorageResult<CompleteMultipartUploadResponse>> CompleteMultipartUploadAsync(string bucketName, string key, CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        // Data-first approach: Check for data existence instead of metadata
        // Phase 3 & 4 Validation: Part size and ETag validation
        var storedParts = await _dataStorage.GetStoredPartsAsync(bucketName, key, request.UploadId, cancellationToken);

        // Check if any parts exist - this is the real validation
        if (!storedParts.Any())
        {
            return StorageResult<CompleteMultipartUploadResponse>.Error("NoSuchUpload", $"Upload '{request.UploadId}' not found");
        }

        // Merge checksums from metadata storage (similar to ListPartsAsync pattern)
        var uploadMetadata = await _metadataStorage.GetUploadMetadataAsync(bucketName, key, request.UploadId, cancellationToken);
        if (uploadMetadata != null && uploadMetadata.Parts.Count > 0)
        {
            foreach (var part in storedParts)
            {
                if (uploadMetadata.Parts.TryGetValue(part.PartNumber, out var partMetadata))
                {
                    part.ChecksumCRC32 = partMetadata.ChecksumCRC32;
                    part.ChecksumCRC32C = partMetadata.ChecksumCRC32C;
                    part.ChecksumCRC64NVME = partMetadata.ChecksumCRC64NVME;
                    part.ChecksumSHA1 = partMetadata.ChecksumSHA1;
                    part.ChecksumSHA256 = partMetadata.ChecksumSHA256;
                }
            }
        }

        var storedPartsDict = storedParts.ToDictionary(p => p.PartNumber, p => p);

        for (int i = 0; i < request.Parts.Count; i++)
        {
            var requestedPart = request.Parts[i];

            // Check if part exists in storage
            if (!storedPartsDict.TryGetValue(requestedPart.PartNumber, out var storedPart))
            {
                return StorageResult<CompleteMultipartUploadResponse>.Error("InvalidPart", $"Part number {requestedPart.PartNumber} does not exist");
            }

            // ETag validation
            if (!string.Equals(requestedPart.ETag.Trim('"'), storedPart.ETag.Trim('"'), StringComparison.OrdinalIgnoreCase))
            {
                return StorageResult<CompleteMultipartUploadResponse>.Error("InvalidPart", $"Part number {requestedPart.PartNumber} ETag does not match. Expected: {storedPart.ETag}, Got: {requestedPart.ETag}");
            }

            // Note: Part size validation (5MB minimum) should be enforced during upload, not here.
            // S3 allows completing multipart uploads with parts smaller than 5MB as long as they were accepted during upload.
        }

        // Get readers for all parts
        var partReaders = await _dataStorage.GetPartReadersAsync(bucketName, key, request.UploadId, request.Parts, cancellationToken);
        var readersList = partReaders.ToList();
        if (!readersList.Any())
        {
            throw new InvalidOperationException("Failed to get part readers");
        }

        // Use already retrieved metadata for S3 compliance, but fall back to defaults if missing (data-first resilience)
        var putRequest = new PutObjectRequest
        {
            Key = key,
            ContentType = uploadMetadata?.ContentType ?? "application/octet-stream",
            Metadata = uploadMetadata?.Metadata ?? new Dictionary<string, string>()
        };

        // Phase 5: Compute proper multipart ETag from individual part ETags
        var partETags = request.Parts.Select(p => storedPartsDict[p.PartNumber].ETag).ToList();
        var multipartETag = ETagHelper.ComputeMultipartETag(partETags);

        // Aggregate checksums from stored parts (checksum-of-checksums per S3 spec)
        // Use checksums from stored parts, not from the client's request
        var orderedStoredParts = request.Parts
            .OrderBy(p => p.PartNumber)
            .Select(p => storedPartsDict[p.PartNumber])
            .ToList();

        var aggregatedCRC32 = MultipartChecksumAggregator.AggregateCrc32(orderedStoredParts.Select(p => p.ChecksumCRC32));
        var aggregatedCRC32C = MultipartChecksumAggregator.AggregateCrc32C(orderedStoredParts.Select(p => p.ChecksumCRC32C));
        var aggregatedSHA1 = MultipartChecksumAggregator.AggregateSha1(orderedStoredParts.Select(p => p.ChecksumSHA1));
        var aggregatedSHA256 = MultipartChecksumAggregator.AggregateSha256(orderedStoredParts.Select(p => p.ChecksumSHA256));
        var aggregatedCRC64NVME = MultipartChecksumAggregator.AggregateCrc64Nvme(orderedStoredParts.Select(p => p.ChecksumCRC64NVME));

        // Build checksums dictionary for storage
        var aggregatedChecksums = new Dictionary<string, string>();
        if (!string.IsNullOrEmpty(aggregatedCRC32))
            aggregatedChecksums["CRC32"] = aggregatedCRC32;
        if (!string.IsNullOrEmpty(aggregatedCRC32C))
            aggregatedChecksums["CRC32C"] = aggregatedCRC32C;
        if (!string.IsNullOrEmpty(aggregatedSHA1))
            aggregatedChecksums["SHA1"] = aggregatedSHA1;
        if (!string.IsNullOrEmpty(aggregatedSHA256))
            aggregatedChecksums["SHA256"] = aggregatedSHA256;
        if (!string.IsNullOrEmpty(aggregatedCRC64NVME))
            aggregatedChecksums["CRC64NVME"] = aggregatedCRC64NVME;

        // Store multipart data directly using the streaming method
        var (size, _) = await _objectDataStorage.StoreMultipartDataAsync(bucketName, key, readersList, cancellationToken);

        // Store metadata with the correct multipart ETag and aggregated checksums
        await _objectMetadataStorage.StoreMetadataAsync(bucketName, key, multipartETag, size, putRequest, aggregatedChecksums.Count > 0 ? aggregatedChecksums : null, cancellationToken);

        // Clean up multipart upload
        await _dataStorage.DeleteAllPartsAsync(bucketName, key, request.UploadId, cancellationToken);
        await _metadataStorage.DeleteUploadMetadataAsync(bucketName, key, request.UploadId, cancellationToken);

        return StorageResult<CompleteMultipartUploadResponse>.Success(new CompleteMultipartUploadResponse
        {
            BucketName = bucketName,
            Key = key,
            ETag = multipartETag,  // Use the proper multipart ETag
            ChecksumCRC32 = aggregatedCRC32,
            ChecksumCRC32C = aggregatedCRC32C,
            ChecksumSHA1 = aggregatedSHA1,
            ChecksumSHA256 = aggregatedSHA256,
            ChecksumCRC64NVME = aggregatedCRC64NVME
        });
    }

    public async Task<bool> AbortMultipartUploadAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var dataDeleted = await _dataStorage.DeleteAllPartsAsync(bucketName, key, uploadId, cancellationToken);
        var metadataDeleted = await _metadataStorage.DeleteUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);

        return dataDeleted || metadataDeleted;
    }

    public async Task<List<UploadPart>> ListPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        // Get parts from data storage (includes ETag, Size, LastModified)
        var parts = await _dataStorage.GetStoredPartsAsync(bucketName, key, uploadId, cancellationToken);

        // Get upload metadata which contains part checksums
        var upload = await _metadataStorage.GetUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);

        // Merge checksums from metadata into parts
        if (upload != null && upload.Parts.Count > 0)
        {
            foreach (var part in parts)
            {
                if (upload.Parts.TryGetValue(part.PartNumber, out var partMetadata))
                {
                    part.ChecksumCRC32 = partMetadata.ChecksumCRC32;
                    part.ChecksumCRC32C = partMetadata.ChecksumCRC32C;
                    part.ChecksumCRC64NVME = partMetadata.ChecksumCRC64NVME;
                    part.ChecksumSHA1 = partMetadata.ChecksumSHA1;
                    part.ChecksumSHA256 = partMetadata.ChecksumSHA256;
                }
            }
        }

        return parts;
    }

    public async Task<List<MultipartUpload>> ListMultipartUploadsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.ListUploadsAsync(bucketName, cancellationToken);
    }
}