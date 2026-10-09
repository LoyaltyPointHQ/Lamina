using Lamina.Storage.Core.Integrity;
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
    private readonly UploadContentProcessor _processor;
    private readonly IObjectPublicationLock _publicationLock;

    private const long MinimumPartSizeBytes = 5 * 1024 * 1024; // 5 MiB per AWS S3 spec

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
    private static readonly Dictionary<string, UploadLockEntry> _uploadLocks = new();
    private sealed class UploadLockEntry
    {
        public readonly SemaphoreSlim Semaphore = new(1, 1);
        public int References;
    }

    public MultipartUploadStorageFacade(
        IMultipartUploadDataStorage dataStorage,
        IMultipartUploadMetadataStorage metadataStorage,
        IObjectDataStorage objectDataStorage,
        IObjectMetadataStorage objectMetadataStorage,
        ILogger<MultipartUploadStorageFacade> logger,
        IChunkedDataParser chunkedDataParser,
        IObjectPublicationLock? publicationLock = null)
    {
        _dataStorage = dataStorage;
        _metadataStorage = metadataStorage;
        _objectDataStorage = objectDataStorage;
        _objectMetadataStorage = objectMetadataStorage;
        _logger = logger;
        _processor = new UploadContentProcessor(chunkedDataParser);
        _publicationLock = publicationLock ?? InMemoryObjectPublicationLock.Shared;
    }

    private static async Task<IDisposable> AcquireUploadLockAsync(string uploadId, CancellationToken cancellationToken)
    {
        UploadLockEntry entry;
        lock (_uploadLocks)
        {
            if (!_uploadLocks.TryGetValue(uploadId, out entry!))
                _uploadLocks.Add(uploadId, entry = new UploadLockEntry());
            entry.References++;
        }
        try { await entry.Semaphore.WaitAsync(cancellationToken); }
        catch { ReleaseReference(uploadId, entry); throw; }
        return new UploadLockLease(uploadId, entry);
    }

    private static void ReleaseReference(string uploadId, UploadLockEntry entry)
    {
        lock (_uploadLocks)
        {
            if (--entry.References == 0)
            {
                _uploadLocks.Remove(uploadId);
                entry.Semaphore.Dispose();
            }
        }
    }

    private sealed class UploadLockLease(string uploadId, UploadLockEntry entry) : IDisposable
    {
        private int _disposed;
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            entry.Semaphore.Release();
            ReleaseReference(uploadId, entry);
        }
    }

    public async Task<MultipartUpload> InitiateMultipartUploadAsync(string bucketName, string key, InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.InitiateUploadAsync(bucketName, key, request, cancellationToken);
    }

    public Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, byte[]? expectedMd5 = null, CancellationToken cancellationToken = default) =>
        UploadPartAsync(bucketName, key, uploadId, partNumber, dataReader, null, null, expectedMd5, cancellationToken);

    public Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, byte[]? expectedMd5 = null, CancellationToken cancellationToken = default) =>
        UploadPartAsync(bucketName, key, uploadId, partNumber, dataReader, chunkValidator, null, expectedMd5, cancellationToken);

    public Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, ChecksumRequest? checksumRequest, byte[]? expectedMd5 = null, CancellationToken cancellationToken = default) =>
        UploadPartAsync(bucketName, key, uploadId, partNumber, dataReader, null, checksumRequest, expectedMd5, cancellationToken);

    public async Task<StorageResult<UploadPart>> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, ChecksumRequest? checksumRequest, byte[]? expectedMd5 = null, CancellationToken cancellationToken = default)
    {
        if (chunkValidator?.ExpectsTrailers == true)
            checksumRequest = TrailerChecksumMerger.RegisterExpectedTrailers(chunkValidator.ExpectedTrailerNames, checksumRequest);
        await using var staging = await _dataStorage.BeginPartWriteAsync(bucketName, key, uploadId, partNumber, cancellationToken);
        var processed = await _processor.ProcessAsync(dataReader, staging.Stream, chunkValidator, checksumRequest, expectedMd5, cancellationToken);
        if (!processed.IsSuccess)
            return StorageResult<UploadPart>.Error(processed.ErrorCode!, processed.ErrorMessage!);
        using var prepared = await staging.SealAsync(cancellationToken);
        var integrity = processed.Value!;
        if (prepared.Size != integrity.Size)
            throw new IOException("Prepared part size does not match the validated payload.");
        // Publish and its metadata are serialized together so parallel replacements cannot
        // associate one part's bytes with another part's checksum.
        using var uploadLock = await AcquireUploadLockAsync(uploadId, cancellationToken);
        await _dataStorage.CommitPreparedPartAsync(bucketName, key, uploadId, partNumber, prepared, cancellationToken);
        var part = new UploadPart
        {
            PartNumber = partNumber,
            Size = integrity.Size,
            ETag = integrity.ETag,
            LastModified = DateTime.UtcNow,
            ChecksumCRC32 = integrity.Checksums.GetValueOrDefault("CRC32"),
            ChecksumCRC32C = integrity.Checksums.GetValueOrDefault("CRC32C"),
            ChecksumCRC64NVME = integrity.Checksums.GetValueOrDefault("CRC64NVME"),
            ChecksumSHA1 = integrity.Checksums.GetValueOrDefault("SHA1"),
            ChecksumSHA256 = integrity.Checksums.GetValueOrDefault("SHA256")
        };
        var result = StorageResult<UploadPart>.Success(part);
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
        try
        {

            // Load persisted integrity in the facade; raw storage only lists part data.
            var uploadMetadata = await _metadataStorage.GetUploadMetadataAsync(bucketName, key, request.UploadId, cancellationToken);

            var storedParts = await _dataStorage.GetStoredPartsAsync(bucketName, key, request.UploadId, cancellationToken);

            // Defensive: HasAnyPartsAsync said yes but GetStoredPartsAsync returned empty. Shouldn't
            // happen in practice, but guards against a TOCTOU race (parts deleted between the two calls).
            if (!storedParts.Any())
            {
                return StorageResult<CompleteMultipartUploadResponse>.Error("NoSuchUpload", $"Upload '{request.UploadId}' not found");
            }

            await ResolvePartIntegrityAsync(bucketName, key, request.UploadId, storedParts, uploadMetadata?.Parts, cancellationToken);

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
            }

            // Validate minimum part size: all parts except the last must be >= 5 MiB
            var maxPartNumber = request.Parts.Max(p => p.PartNumber);
            foreach (var requestedPart in request.Parts)
            {
                var storedPart = storedPartsDict[requestedPart.PartNumber];

                if (requestedPart.PartNumber == maxPartNumber)
                    continue;

                if (storedPart.Size < MinimumPartSizeBytes)
                {
                    return StorageResult<CompleteMultipartUploadResponse>.Error(
                        "EntityTooSmall",
                        $"Your proposed upload is smaller than the minimum allowed size. Part {requestedPart.PartNumber} is {storedPart.Size} bytes, minimum is {MinimumPartSizeBytes}.");
                }
            }

            // Use already retrieved metadata for S3 compliance, but fall back to defaults if missing (data-first resilience)
            var putRequest = new PutObjectRequest
            {
                Key = key,
                ContentType = string.IsNullOrEmpty(uploadMetadata?.ContentType) ? "application/octet-stream" : uploadMetadata.ContentType,
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

            // Default: write metadata first with an explicit LastModified (so a GET during the tiny
            // in-between window returns 404 rather than 200 with an auto-generated full-file MD5 ETag).
            // Xattr-style backends cannot persist metadata before the data file exists, so fall back
            // to data-first for them; the resulting <1 ms window where a concurrent GET might see an
            // auto-generated ETag is acceptable for that opt-in mode.
            var storeMetadataFirst = _objectMetadataStorage is not IRequiresDataFileForMetadata;
            var aggregatedChecksumsToStore = aggregatedChecksums.Count > 0 ? aggregatedChecksums : null;

            await using var publication = await _publicationLock.AcquireAsync(bucketName, key, cancellationToken);
            if (storeMetadataFirst)
            {
                var storedMetadata = await _objectMetadataStorage.StoreMetadataAsync(bucketName, key, multipartETag, size, putRequest, aggregatedChecksumsToStore, DateTime.UtcNow, cancellationToken);
                if (storedMetadata == null)
                    return StorageResult<CompleteMultipartUploadResponse>.Error("InternalError", "Unable to persist completed object metadata.");
                await _objectDataStorage.CommitPreparedDataAsync(preparedData, cancellationToken);
            }
            else
            {
                await _objectDataStorage.CommitPreparedDataAsync(preparedData, cancellationToken);
                var storedMetadata = await _objectMetadataStorage.StoreMetadataAsync(bucketName, key, multipartETag, size, putRequest, aggregatedChecksumsToStore, lastModified: null, cancellationToken);
                if (storedMetadata == null)
                {
                    await _objectDataStorage.DeleteDataAsync(bucketName, key, cancellationToken);
                    return StorageResult<CompleteMultipartUploadResponse>.Error("InternalError", "Unable to persist completed object metadata.");
                }
            }

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

            return response;
        }
        finally
        {
            uploadLockReleaser.Dispose();

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

        return dataDeleted || metadataDeleted;
    }

    public async Task<StorageResult<List<UploadPart>>> ListPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        // Serialise against concurrent UploadPart writers - we read Upload.Parts and must not see
        // it mid-resize.
        using var _ = await AcquireUploadLockAsync(uploadId, cancellationToken);

        // Reuse persisted integrity before reading any part payload.
        var upload = await _metadataStorage.GetUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);

        var parts = await _dataStorage.GetStoredPartsAsync(bucketName, key, uploadId, cancellationToken);

        // An initiated upload may have no parts yet; stored parts remain readable without
        // metadata (data-first). Only the absence of both means the upload is gone.
        if (upload == null && parts.Count == 0)
        {
            return StorageResult<List<UploadPart>>.Error("NoSuchUpload", $"Upload '{uploadId}' not found");
        }

        await ResolvePartIntegrityAsync(bucketName, key, uploadId, parts, upload?.Parts, cancellationToken);

        return StorageResult<List<UploadPart>>.Success(parts);
    }

    private async Task ResolvePartIntegrityAsync(string bucketName, string key, string uploadId, List<UploadPart> parts,
        IReadOnlyDictionary<int, PartMetadata>? metadata, CancellationToken cancellationToken)
    {
        foreach (var part in parts)
        {
            if (metadata != null && metadata.TryGetValue(part.PartNumber, out var stored))
            {
                if (string.IsNullOrEmpty(part.ETag)) part.ETag = stored.ETag;
                part.ChecksumCRC32 = stored.ChecksumCRC32;
                part.ChecksumCRC32C = stored.ChecksumCRC32C;
                part.ChecksumCRC64NVME = stored.ChecksumCRC64NVME;
                part.ChecksumSHA1 = stored.ChecksumSHA1;
                part.ChecksumSHA256 = stored.ChecksumSHA256;
            }
            if (!string.IsNullOrEmpty(part.ETag)) continue;
            var readers = (await _dataStorage.GetPartReadersAsync(bucketName, key, uploadId,
                [new CompletedPart { PartNumber = part.PartNumber }], cancellationToken)).ToList();
            try
            {
                if (readers.Count != 1) throw new IOException("Part disappeared while resolving its ETag.");
                await using var stream = readers[0].AsStream(leaveOpen: true);
                (part.ETag, _) = await ChecksumHelper.ComputeETagAndChecksumsFromStreamAsync(stream, [], cancellationToken);
            }
            finally
            {
                foreach (var reader in readers) await reader.CompleteAsync();
            }
        }
    }

    public IAsyncEnumerable<string> EnumerateUploadKeysAsync(string bucketName, CancellationToken cancellationToken = default) => _metadataStorage.EnumerateUploadKeysAsync(bucketName, cancellationToken);

    public Task<List<MultipartUpload>> ListAllMultipartUploadsAsync(CancellationToken cancellationToken = default) =>
        _metadataStorage.ListAllUploadsAsync(cancellationToken);

    public async Task<List<MultipartUpload>> ListMultipartUploadsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.ListUploadsAsync(bucketName, cancellationToken);
    }
}
