using System.IO.Pipelines;
using Lamina.Models;

namespace Lamina.Services;

public class MultipartUploadServiceFacade : IMultipartUploadServiceFacade
{
    private readonly IMultipartUploadDataService _dataService;
    private readonly IMultipartUploadMetadataService _metadataService;
    private readonly IObjectDataService _objectDataService;
    private readonly IObjectMetadataService _objectMetadataService;
    private readonly ILogger<MultipartUploadServiceFacade> _logger;

    public MultipartUploadServiceFacade(
        IMultipartUploadDataService dataService,
        IMultipartUploadMetadataService metadataService,
        IObjectDataService objectDataService,
        IObjectMetadataService objectMetadataService,
        ILogger<MultipartUploadServiceFacade> logger)
    {
        _dataService = dataService;
        _metadataService = metadataService;
        _objectDataService = objectDataService;
        _objectMetadataService = objectMetadataService;
        _logger = logger;
    }

    public async Task<MultipartUpload> InitiateMultipartUploadAsync(string bucketName, string key, InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        return await _metadataService.InitiateUploadAsync(bucketName, key, request, cancellationToken);
    }

    public async Task<UploadPart> UploadPartAsync(string bucketName, string key, string uploadId, int partNumber, PipeReader dataReader, CancellationToken cancellationToken = default)
    {
        var upload = await _metadataService.GetUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);
        if (upload == null)
        {
            throw new InvalidOperationException($"Upload '{uploadId}' not found");
        }

        return await _dataService.StorePartDataAsync(bucketName, key, uploadId, partNumber, dataReader, cancellationToken);
    }

    public async Task<CompleteMultipartUploadResponse> CompleteMultipartUploadAsync(string bucketName, string key, CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        var upload = await _metadataService.GetUploadMetadataAsync(bucketName, key, request.UploadId, cancellationToken);
        if (upload == null)
        {
            throw new InvalidOperationException($"Upload '{request.UploadId}' not found");
        }

        // Get readers for all parts
        var partReaders = await _dataService.GetPartReadersAsync(bucketName, key, request.UploadId, request.Parts, cancellationToken);
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
        var (size, etag) = await _objectDataService.StoreMultipartDataAsync(bucketName, key, readersList, cancellationToken);

        // Store metadata
        await _objectMetadataService.StoreMetadataAsync(bucketName, key, etag, size, putRequest, cancellationToken);

        // Clean up multipart upload
        await _dataService.DeleteAllPartsAsync(bucketName, key, request.UploadId, cancellationToken);
        await _metadataService.DeleteUploadMetadataAsync(bucketName, key, request.UploadId, cancellationToken);

        return new CompleteMultipartUploadResponse
        {
            BucketName = bucketName,
            Key = key,
            ETag = etag  // Use the ETag computed by StoreMultipartDataAsync
        };
    }

    public async Task<bool> AbortMultipartUploadAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        var dataDeleted = await _dataService.DeleteAllPartsAsync(bucketName, key, uploadId, cancellationToken);
        var metadataDeleted = await _metadataService.DeleteUploadMetadataAsync(bucketName, key, uploadId, cancellationToken);

        return dataDeleted || metadataDeleted;
    }

    public async Task<List<UploadPart>> ListPartsAsync(string bucketName, string key, string uploadId, CancellationToken cancellationToken = default)
    {
        return await _dataService.GetStoredPartsAsync(bucketName, key, uploadId, cancellationToken);
    }

    public async Task<List<MultipartUpload>> ListMultipartUploadsAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await _metadataService.ListUploadsAsync(bucketName, cancellationToken);
    }
}