using System.Collections.Concurrent;
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

    // Per-upload serialisation for metadata mutations. MultipartUpload.Parts is a plain
    // Dictionary<int, PartMetadata> shared across concurrent UploadPart requests for the same
    // uploadId (aws s3 cp fires ~10 parallel parts). Without this lock, Get→Mutate→Update racing
    // produces IndexOutOfRangeException / NullReferenceException during dictionary resize and can
    // silently drop entries under last-writer-wins.
    //
    // MUST be static: the facade is registered as Scoped, so concurrent requests for the same
    // upload get different instances. A non-static lock dictionary would give each request its
    // own SemaphoreSlim and defeat the whole point. Making the dictionary static makes the lock
    // pool process-wide, which is what we actually need.
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> _uploadLocks = new();

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

    private async Task<IDisposable> AcquireUploadLockAsync(string uploadId, CancellationToken cancellationToken)
    {
        var sem = _uploadLocks.GetOrAdd(uploadId, static _ => new SemaphoreSlim(1, 1));
        await sem.WaitAsync(cancellationToken);
        return new SemaphoreReleaser(sem);
    }

    private void ReleaseUploadLockResources(string uploadId)
    {
        if (_uploadLocks.TryRemove(uploadId, out var sem))
        {
            sem.Dispose();
        }
    }

    private readonly struct SemaphoreReleaser(SemaphoreSlim sem) : IDisposable
    {
        public void Dispose() => sem.Release();
    }

    public async Task<MultipartUpload> InitiateMultipartUploadAsync(string bucketName, string key, InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.InitiateUploadAsync(bucketName, key, request, cancellationToken);
    }

    public async Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, byte[]? expectedMd5 = null, CancellationToken cancellationToken = default)
    {
        var result = await _dataStorage.StorePartDataAsync(bucketName, key, uploadId, partNumber, dataReader, null, expectedMd5, cancellationToken);
        await PersistPartMetadataAsync(bucketName, key, uploadId, partNumber, result, cancellationToken);
        return result;
    }

    public async Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, byte[]? expectedMd5 = null, CancellationToken cancellationToken = default)
    {
        if (chunkValidator == null)
        {
            return await UploadPartAsync(bucketName, key, uploadId, partNumber, dataReader, expectedMd5, cancellationToken);
        }

        var result = await _dataStorage.StorePartDataAsync(bucketName, key, uploadId, partNumber, dataReader, _chunkedDataParser, chunkValidator, null, expectedMd5, cancellationToken);
        await PersistPartMetadataAsync(bucketName, key, uploadId, partNumber, result, cancellationToken);
        return result;
    }

    public async Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, ChecksumRequest? checksumRequest, byte[]? expectedMd5 = null, CancellationToken cancellationToken = default)
    {
        var result = await _dataStorage.StorePartDataAsync(bucketName, key, uploadId, partNumber, dataReader, checksumRequest, expectedMd5, cancellationToken);
        await PersistPartMetadataAsync(bucketName, key, uploadId, partNumber, result, cancellationToken);
        return result;
    }

    public async Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, ChecksumRequest? checksumRequest, byte[]? expectedMd5 = null, CancellationToken cancellationToken = default)
    {
        if (chunkValidator == null)
        {
            return await UploadPartAsync(bucketName, key, uploadId, partNumber, dataReader, checksumRequest, expectedMd5, cancellationToken);
        }

        var result = await _dataStorage.StorePartDataAsync(bucketName, key, uploadId, partNumber, dataReader, _chunkedDataParser, chunkValidator, checksumRequest, expectedMd5, cancellationToken);
        await PersistPartMetadataAsync(bucketName, key, uploadId, partNumber, result, cancellationToken);
        return result;
    }

    // Persists the server-computed ETag (and any checksums) for a successfully-stored part.
    // Data-first is preserved: if the upload metadata doesn't exist (e.g. user bypassed Initiate
    // or metadata was wiped), the part file is still on disk and Complete will fall back to
    // recomputing ETag from the file. This is best-effort bookkeeping to speed up Complete.
    private async Task PersistPartMetadataAsync(string bucketName, string key, string uploadId, int partNumber, StorageResult<UploadPart> result, CancellationToken cancellationToken)
    {
        if (!result.IsSuccess || result.Value == null)
        {
            return;
        }

        // Serialise the Get→Mutate→Update sequence against other concurrent UploadPart callers
        // for the same uploadId so the shared MultipartUpload.Parts dictionary stays consistent.
        using var _ = await AcquireUploadLockAsync(uploadId, cancellationToken);

        var upload = await _metadataStorage.GetUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);
        if (upload == null)
        {
            return;
        }

        upload.Parts[partNumber] = new PartMetadata
        {
            ETag = result.Value.ETag,
            ChecksumCRC32 = result.Value.ChecksumCRC32,
            ChecksumCRC32C = result.Value.ChecksumCRC32C,
            ChecksumCRC64NVME = result.Value.ChecksumCRC64NVME,
            ChecksumSHA1 = result.Value.ChecksumSHA1,
            ChecksumSHA256 = result.Value.ChecksumSHA256
        };
        await _metadataStorage.UpdateUploadMetadataAsync(bucketName, key, uploadId, upload, cancellationToken);
    }

    public async Task<StorageResult<CompleteMultipartUploadResponse>> CompleteMultipartUploadAsync(string bucketName, string key, CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        // Data-first approach: cheap existence check before touching metadata.
        // If no part data exists, reject without ever loading metadata (preserves the invariant that
        // existence is decided by data, not metadata - an upload with stale metadata but no parts is
        // treated as non-existent).
        var hasAnyParts = await _dataStorage.HasAnyPartsAsync(bucketName, key, request.UploadId, cancellationToken);
        if (!hasAnyParts)
        {
            return StorageResult<CompleteMultipartUploadResponse>.Error("NoSuchUpload", $"Upload '{request.UploadId}' not found");
        }

        // Serialise against in-flight UploadPart metadata writers for the same upload: we're about
        // to read Upload.Parts and must see a consistent snapshot. Acquired and released manually
        // so the per-upload SemaphoreSlim can be freed (only) after a successful Complete; error
        // paths leave it intact so subsequent retries still see a live lock.
        var uploadLockReleaser = await AcquireUploadLockAsync(request.UploadId, cancellationToken);
        var uploadCompleted = false;
        try
        {

        // Upload exists - load metadata up-front so we can hint GetStoredPartsAsync with persisted
        // part ETags and checksums, letting filesystem backends skip a full MD5 pass over every part.
        var uploadMetadata = await _metadataStorage.GetUploadMetadataAsync(bucketName, key, request.UploadId, cancellationToken);

        var storedParts = await _dataStorage.GetStoredPartsAsync(bucketName, key, request.UploadId, uploadMetadata?.Parts, cancellationToken);

        // Defensive: HasAnyPartsAsync said yes but GetStoredPartsAsync returned empty. Shouldn't
        // happen in practice, but guards against a TOCTOU race (parts deleted between the two calls).
        if (!storedParts.Any())
        {
            return StorageResult<CompleteMultipartUploadResponse>.Error("NoSuchUpload", $"Upload '{request.UploadId}' not found");
        }

        // Merge per-part checksums from metadata (ETag is already resolved by GetStoredPartsAsync via the hint above).
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

        // Use already retrieved metadata for S3 compliance, but fall back to defaults if missing (data-first resilience)
        var putRequest = new PutObjectRequest
        {
            Key = key,
            ContentType = uploadMetadata?.ContentType ?? "application/octet-stream",
            Metadata = uploadMetadata?.Metadata ?? new Dictionary<string, string>(),
            Tags = uploadMetadata?.Tags ?? new Dictionary<string, string>()
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
        var aggregatedCRC64NVME = MultipartChecksumAggregator.AggregateCrc64NvmeFullObject(orderedStoredParts.Select(p => (p.ChecksumCRC64NVME, p.Size)));

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

        // Phase 1: Prepare multipart data (temp file, not yet visible).
        // Fast path: when both storages are file-backed (filesystem + filesystem), assemble the
        // tempfile directly from part file paths using kernel-side copy (copy_file_range -> CoW
        // reflink on XFS/Btrfs, or server-side SMB/NFS copy) so bytes never traverse userspace
        // or the client<->server network hop.
        PreparedData? prepared = null;
        if (_dataStorage is IFileBackedMultipartPartSource fileSrc
            && _objectDataStorage is IFileBackedObjectDataStorage fileDst
            && fileSrc.TryGetPartFilePaths(bucketName, key, request.UploadId, request.Parts, out var partPaths))
        {
            prepared = await fileDst.PrepareMultipartDataFromFilesAsync(bucketName, key, partPaths, cancellationToken);
        }

        if (prepared == null)
        {
            // Fallback: PipeReader path - required for non-file-backed backends (InMemory, future
            // SQL-data) and when the feature flag UseZeroCopyCompleteMultipart is disabled.
            var partReaders = await _dataStorage.GetPartReadersAsync(bucketName, key, request.UploadId, request.Parts, cancellationToken);
            var readersList = partReaders.ToList();
            if (!readersList.Any())
            {
                throw new InvalidOperationException("Failed to get part readers");
            }
            prepared = await _objectDataStorage.PrepareMultipartDataAsync(bucketName, key, readersList, cancellationToken);
        }

        using var preparedData = prepared;
        var size = preparedData.Size;

        // Phase 2: Commit data first so its mtime is real before Phase 3 reads it. The filesystem
        // metadata storage derives LastModified from FileInfo(dataPath).LastWriteTimeUtc; if the
        // data file doesn't exist yet, that returns Windows epoch (1601-01-01 UTC) and persists
        // DateTime.MinValue, which then triggers stale-metadata recompute on every GET/HEAD and
        // silently rewrites the multipart ETag to MD5-of-full-file. The data-first architecture
        // tolerates a crash between Phase 2 and Phase 3 (visible data without metadata is
        // auto-regenerated on first read).
        await _objectDataStorage.CommitPreparedDataAsync(preparedData, cancellationToken);

        // Phase 3: Store metadata, reading the now-real file mtime for LastModified.
        await _objectMetadataStorage.StoreMetadataAsync(bucketName, key, multipartETag, size, putRequest, aggregatedChecksums.Count > 0 ? aggregatedChecksums : null, cancellationToken);

        // Clean up multipart upload
        await _dataStorage.DeleteAllPartsAsync(bucketName, key, request.UploadId, cancellationToken);
        await _metadataStorage.DeleteUploadMetadataAsync(bucketName, key, request.UploadId, cancellationToken);

        var response = StorageResult<CompleteMultipartUploadResponse>.Success(new CompleteMultipartUploadResponse
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

        uploadCompleted = true;
        return response;
        }
        finally
        {
            uploadLockReleaser.Dispose();
            if (uploadCompleted)
            {
                // Upload is gone (DeleteAllParts + DeleteUploadMetadata ran) - free the
                // per-upload semaphore so it doesn't accumulate one entry per historical upload.
                ReleaseUploadLockResources(request.UploadId);
            }
        }
    }

    public async Task<bool> AbortMultipartUploadAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        bool dataDeleted;
        bool metadataDeleted;
        using (await AcquireUploadLockAsync(uploadId, cancellationToken))
        {
            dataDeleted = await _dataStorage.DeleteAllPartsAsync(bucketName, key, uploadId, cancellationToken);
            metadataDeleted = await _metadataStorage.DeleteUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);
        }

        // Upload is gone - free the per-upload semaphore.
        ReleaseUploadLockResources(uploadId);

        return dataDeleted || metadataDeleted;
    }

    public async Task<List<UploadPart>> ListPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        // Serialise against concurrent UploadPart writers - we read Upload.Parts and must not see
        // it mid-resize.
        using var _ = await AcquireUploadLockAsync(uploadId, cancellationToken);

        // Load metadata first so we can hint the data storage with persisted ETags (same perf reason
        // as Complete - filesystem backend can skip a full MD5 pass when metadata has the ETag).
        var upload = await _metadataStorage.GetUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);

        var parts = await _dataStorage.GetStoredPartsAsync(bucketName, key, uploadId, upload?.Parts, cancellationToken);

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