using System.IO.Pipelines;
using Microsoft.Extensions.Logging;
using Lamina.Models;
using Lamina.Services;

namespace Lamina.Storage.Abstract;

public class MultipartUploadStorageFacade : IMultipartUploadStorageFacade
{
    private readonly IMultipartUploadDataStorage _dataStorage;
    private readonly IMultipartUploadMetadataStorage _metadataStorage;
    private readonly IObjectDataStorage _objectDataStorage;
    private readonly IObjectMetadataStorage _objectMetadataStorage;
    private readonly ILogger<MultipartUploadStorageFacade> _logger;

    public MultipartUploadStorageFacade(
        IMultipartUploadDataStorage dataStorage,
        IMultipartUploadMetadataStorage metadataStorage,
        IObjectDataStorage objectDataStorage,
        IObjectMetadataStorage objectMetadataStorage,
        ILogger<MultipartUploadStorageFacade> logger)
    {
        _dataStorage = dataStorage;
        _metadataStorage = metadataStorage;
        _objectDataStorage = objectDataStorage;
        _objectMetadataStorage = objectMetadataStorage;
        _logger = logger;
    }

    public async Task<MultipartUpload> InitiateMultipartUploadAsync(string bucketName, string key, InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        return await _metadataStorage.InitiateUploadAsync(bucketName, key, request, cancellationToken);
    }

    public async Task<UploadPart> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        var upload = await _metadataStorage.GetUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);
        if (upload == null)
        {
            throw new InvalidOperationException($"Upload '{uploadId}' not found");
        }

        return await _dataStorage.StorePartDataAsync(bucketName, key, uploadId, partNumber, dataReader, cancellationToken);
    }

    public async Task<UploadPart?> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, IChunkSignatureValidator? chunkValidator, CancellationToken cancellationToken = default)
    {
        // If no chunk validator, use the non-validating version
        if (chunkValidator == null)
        {
            return await UploadPartAsync(bucketName, key, uploadId, partNumber, dataReader, cancellationToken);
        }

        // Verify upload exists
        var upload = await _metadataStorage.GetUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);
        if (upload == null)
        {
            throw new InvalidOperationException($"Upload '{uploadId}' not found");
        }

        // For streaming validation, parse and validate chunks while streaming to storage
        using var tempStream = new MemoryStream();
        try
        {
            await Helpers.AwsChunkedEncodingHelper.ParseChunkedDataToStreamAsync(dataReader, tempStream, chunkValidator, cancellationToken);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("Invalid") && ex.Message.Contains("signature"))
        {
            // Invalid chunk signature
            _logger.LogWarning("Chunk signature validation failed for part {PartNumber} of upload {UploadId}", partNumber, uploadId);
            return null;
        }

        // Create a pipe reader from the validated decoded data
        tempStream.Position = 0;
        var pipe = new System.IO.Pipelines.Pipe();
        var writeTask = Task.Run(async () =>
        {
            await tempStream.CopyToAsync(pipe.Writer.AsStream(), cancellationToken);
            await pipe.Writer.CompleteAsync();
        });

        var result = await _dataStorage.StorePartDataAsync(bucketName, key, uploadId, partNumber, pipe.Reader, cancellationToken);
        await writeTask;

        return result;
    }

    public async Task<CompleteMultipartUploadResponse> CompleteMultipartUploadAsync(string bucketName, string key, CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        var upload = await _metadataStorage.GetUploadMetadataAsync(bucketName, key, request.UploadId, cancellationToken);
        if (upload == null)
        {
            throw new InvalidOperationException($"Upload '{request.UploadId}' not found");
        }

        // Get readers for all parts
        var partReaders = await _dataStorage.GetPartReadersAsync(bucketName, key, request.UploadId, request.Parts, cancellationToken);
        var readersList = partReaders.ToList();
        if (!readersList.Any())
        {
            throw new InvalidOperationException("Failed to get part readers");
        }

        var putRequest = new PutObjectRequest
        {
            Key = key,
            ContentType = upload.ContentType,
            Metadata = upload.Metadata
        };

        // Store multipart data directly using the new streaming method
        var (size, etag) = await _objectDataStorage.StoreMultipartDataAsync(bucketName, key, readersList, cancellationToken);

        // Store metadata
        await _objectMetadataStorage.StoreMetadataAsync(bucketName, key, etag, size, putRequest, cancellationToken);

        // Clean up multipart upload
        await _dataStorage.DeleteAllPartsAsync(bucketName, key, request.UploadId, cancellationToken);
        await _metadataStorage.DeleteUploadMetadataAsync(bucketName, key, request.UploadId, cancellationToken);

        return new CompleteMultipartUploadResponse
        {
            BucketName = bucketName,
            Key = key,
            ETag = etag  // Use the ETag computed by StoreMultipartDataAsync
        };
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