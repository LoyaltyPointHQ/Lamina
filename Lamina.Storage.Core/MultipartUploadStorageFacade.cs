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

    public async Task<UploadPart> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        // Data-first approach: Store the part data without checking metadata
        // This allows parts to be uploaded even if metadata was lost or corrupted
        return await _dataStorage.StorePartDataAsync(bucketName, key, uploadId, partNumber, dataReader, cancellationToken);
    }

    public async Task<UploadPart?> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, CancellationToken cancellationToken = default)
    {
        // If no chunk validator, use the non-validating version
        if (chunkValidator == null)
        {
            return await UploadPartAsync(bucketName, key, uploadId, partNumber, dataReader, cancellationToken);
        }

        // Data-first approach: Don't check metadata, just validate and store the part
        // Use the specialized overload that handles validation and storage in a single pass
        return await _dataStorage.StorePartDataAsync(bucketName, key, uploadId, partNumber, dataReader, _chunkedDataParser, chunkValidator, cancellationToken);
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

        // Try to retrieve metadata for S3 compliance, but fall back to defaults if missing (data-first resilience)
        var upload = await _metadataStorage.GetUploadMetadataAsync(bucketName, key, request.UploadId, cancellationToken);
        var putRequest = new PutObjectRequest
        {
            Key = key,
            ContentType = upload?.ContentType ?? "application/octet-stream",
            Metadata = upload?.Metadata ?? new Dictionary<string, string>()
        };

        // Phase 5: Compute proper multipart ETag from individual part ETags
        var partETags = request.Parts.Select(p => storedPartsDict[p.PartNumber].ETag).ToList();
        var multipartETag = ETagHelper.ComputeMultipartETag(partETags);

        // Store multipart data directly using the streaming method
        var (size, _) = await _objectDataStorage.StoreMultipartDataAsync(bucketName, key, readersList, cancellationToken);

        // Store metadata with the correct multipart ETag
        await _objectMetadataStorage.StoreMetadataAsync(bucketName, key, multipartETag, size, putRequest, cancellationToken);

        // Clean up multipart upload
        await _dataStorage.DeleteAllPartsAsync(bucketName, key, request.UploadId, cancellationToken);
        await _metadataStorage.DeleteUploadMetadataAsync(bucketName, key, request.UploadId, cancellationToken);

        return StorageResult<CompleteMultipartUploadResponse>.Success(new CompleteMultipartUploadResponse
        {
            BucketName = bucketName,
            Key = key,
            ETag = multipartETag  // Use the proper multipart ETag
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
        return await _dataStorage.GetStoredPartsAsync(bucketName, key, uploadId, cancellationToken);
    }

    public async Task<List<MultipartUpload>> ListMultipartUploadsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.ListUploadsAsync(bucketName, cancellationToken);
    }
}