using Microsoft.AspNetCore.Mvc;
using Lamina.Models;
using Lamina.Services;
using Lamina.Streaming.Validation;
using System.Xml.Serialization;
using System.IO;
using Lamina.Storage.Abstract;
using Lamina.Helpers;
using Lamina.Controllers.Base;
using Lamina.Controllers.Attributes;

namespace Lamina.Controllers;

[Route("{bucketName}")]
public class S3MultipartController : S3ControllerBase
{
    private readonly IMultipartUploadStorageFacade _multipartStorage;
    private readonly IBucketStorageFacade _bucketStorage;
    private readonly ILogger<S3MultipartController> _logger;

    public S3MultipartController(
        IMultipartUploadStorageFacade multipartStorage,
        IBucketStorageFacade bucketStorage,
        ILogger<S3MultipartController> logger
    )
    {
        _multipartStorage = multipartStorage;
        _bucketStorage = bucketStorage;
        _logger = logger;
    }

    private static XmlDeserializationResult TryDeserializeCompleteRequest(string xmlContent)
    {
        // Try without namespace first (for compatibility with most clients)
        if (TryDeserializeWithoutNamespace(xmlContent, out var partsNoNs))
        {
            return XmlDeserializationResult.Success(partsNoNs);
        }

        // Try with S3 namespace
        if (TryDeserializeWithNamespace(xmlContent, out var partsWithNs))
        {
            return XmlDeserializationResult.Success(partsWithNs);
        }

        return XmlDeserializationResult.Error("The XML you provided was not well-formed or did not validate against our published schema.");
    }

    private static bool TryDeserializeWithoutNamespace(string xmlContent, out List<CompletedPart> parts)
    {
        parts = new List<CompletedPart>();
        try
        {
            var serializer = new XmlSerializer(typeof(CompleteMultipartUploadXmlNoNamespace));
            using var stringReader = new StringReader(xmlContent);
            var completeRequest = serializer.Deserialize(stringReader) as CompleteMultipartUploadXmlNoNamespace;

            if (completeRequest?.Parts != null)
            {
                parts = completeRequest.Parts.Select(p => new CompletedPart
                {
                    PartNumber = p.PartNumber,
                    ETag = p.ETag,
                    ChecksumCRC32 = p.ChecksumCRC32,
                    ChecksumCRC32C = p.ChecksumCRC32C,
                    ChecksumCRC64NVME = p.ChecksumCRC64NVME,
                    ChecksumSHA1 = p.ChecksumSHA1,
                    ChecksumSHA256 = p.ChecksumSHA256
                }).ToList();
                return parts.Count > 0;
            }
            return false;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryDeserializeWithNamespace(string xmlContent, out List<CompletedPart> parts)
    {
        parts = new List<CompletedPart>();
        try
        {
            var serializer = new XmlSerializer(typeof(CompleteMultipartUploadXml));
            using var stringReader = new StringReader(xmlContent);
            var completeRequest = serializer.Deserialize(stringReader) as CompleteMultipartUploadXml;

            if (completeRequest?.Parts != null)
            {
                parts = completeRequest.Parts.Select(p => new CompletedPart
                {
                    PartNumber = p.PartNumber,
                    ETag = p.ETag,
                    ChecksumCRC32 = p.ChecksumCRC32,
                    ChecksumCRC32C = p.ChecksumCRC32C,
                    ChecksumCRC64NVME = p.ChecksumCRC64NVME,
                    ChecksumSHA1 = p.ChecksumSHA1,
                    ChecksumSHA256 = p.ChecksumSHA256
                }).ToList();
                return parts.Count > 0;
            }
            return false;
        }
        catch
        {
            return false;
        }
    }

    [HttpGet("")]
    [RequireQueryParameter("uploads")]
    public async Task<IActionResult> ListMultipartUploads(
        string bucketName,
        [FromQuery(Name = "key-marker")] string? keyMarker,
        [FromQuery(Name = "upload-id-marker")] string? uploadIdMarker,
        [FromQuery(Name = "max-uploads")] int? maxUploads,
        CancellationToken cancellationToken = default
    )
    {
        var uploads = await _multipartStorage.ListMultipartUploadsAsync(bucketName, cancellationToken);

        var result = new ListMultipartUploadsResult
        {
            Bucket = bucketName,
            KeyMarker = keyMarker,
            UploadIdMarker = uploadIdMarker,
            MaxUploads = maxUploads ?? 1000,
            IsTruncated = false,
            Uploads = uploads.Select(u => new Upload
            {
                Key = u.Key,
                UploadId = u.UploadId,
                StorageClass = "STANDARD",
                Initiated = u.Initiated.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'"),
                Owner = new Owner(),
                Initiator = new Owner()
            }).ToList()
        };

        Response.ContentType = "application/xml";
        return Ok(result);
    }

    [HttpPost("{*key}")]
    [RequireQueryParameter("uploads")]
    public async Task<IActionResult> InitiateMultipartUpload(
        string bucketName,
        string key,
        CancellationToken cancellationToken = default
    )
    {
        // Validate the object key
        var objectStorage = HttpContext.RequestServices.GetRequiredService<IObjectStorageFacade>();
        if (!objectStorage.IsValidObjectKey(key))
        {
            return S3Error("InvalidObjectName", "Object key is forbidden", $"/{bucketName}/{key}", 400);
        }

        var request = new InitiateMultipartUploadRequest
        {
            Key = key,
            ContentType = Request.ContentType,
            Metadata = Request.Headers
                .Where(h => h.Key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
                .ToDictionary(h => h.Key.Substring(11), h => h.Value.ToString())
        };

        try
        {
            if (!await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken))
            {
                throw new InvalidOperationException($"Bucket '{bucketName}' does not exist");
            }

            var upload = await _multipartStorage.InitiateMultipartUploadAsync(bucketName, key, request, cancellationToken);

            var result = new InitiateMultipartUploadResult
            {
                Bucket = bucketName,
                Key = key,
                UploadId = upload.UploadId
            };

            Response.ContentType = "application/xml";
            return Ok(result);
        }
        catch (InvalidOperationException)
        {
            return S3Error("NoSuchBucket", "The specified bucket does not exist", bucketName, 404);
        }
    }

    [HttpPut("{*key}")]
    [RequireQueryParameter("partNumber", "uploadId")]
    [DisableRequestSizeLimit]
    public async Task<IActionResult> UploadPart(
        string bucketName,
        string key,
        [FromQuery] int partNumber,
        [FromQuery] string uploadId,
        CancellationToken cancellationToken = default
    )
    {
        _logger.LogInformation("Uploading part {PartNumber} for upload {UploadId} in bucket {BucketName}, key {Key}", partNumber, uploadId, bucketName, key);

        try
        {
            // Use PipeReader from the request body
            var reader = Request.BodyReader;

            // Check if there's a chunk validator from the authentication middleware (for streaming uploads)
            var chunkValidator = HttpContext.Items["ChunkValidator"] as IChunkSignatureValidator;

            var part = chunkValidator != null
                ? await _multipartStorage.UploadPartAsync(bucketName, key, uploadId, partNumber, reader, chunkValidator, cancellationToken)
                : await _multipartStorage.UploadPartAsync(bucketName, key, uploadId, partNumber, reader, cancellationToken);

            if (part == null)
            {
                // If we had a chunk validator and the operation failed, it's likely due to invalid signature
                if (chunkValidator != null)
                {
                    _logger.LogWarning("Chunk signature validation failed for part {PartNumber} of upload {UploadId}", partNumber, uploadId);
                    return S3Error("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", $"/{bucketName}/{key}", 403);
                }

                // Otherwise, generic error
                return StatusCode(500);
            }

            Response.Headers.Append("ETag", $"\"{part.ETag}\"");
            return Ok();
        }
        catch (InvalidOperationException ex)
        {
            return S3Error("NoSuchUpload", ex.Message, $"{bucketName}/{key}", 404);
        }
    }

    [HttpPost("{*key}")]
    [RequireQueryParameter("uploadId")]
    [RequireNoQueryParameters("uploads")]
    public async Task<IActionResult> CompleteMultipartUpload(
        string bucketName,
        string key,
        [FromQuery] string uploadId,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            // Read the XML request body using PipeReader
            var xmlContent = await PipeReaderHelper.ReadAllTextAsync(Request.BodyReader, false, cancellationToken);

            // Deserialize XML without exception-driven control flow
            var deserializationResult = TryDeserializeCompleteRequest(xmlContent);
            if (!deserializationResult.IsSuccess)
            {
                return S3Error("MalformedXML", deserializationResult.ErrorMessage!, $"{bucketName}/{key}", 400);
            }

            var parts = deserializationResult.Parts!;

            // Phase 2 Validation: Part number ordering (parts must be consecutive starting from 1)
            // Check if the provided parts are already in ascending order
            bool isInOrder = true;
            for (int i = 0; i < parts.Count - 1; i++)
            {
                if (parts[i].PartNumber >= parts[i + 1].PartNumber)
                {
                    isInOrder = false;
                    break;
                }
            }

            if (!isInOrder)
            {
                return S3Error("InvalidPartOrder", "The list of parts was not in ascending order. The parts list must be specified in order by part number.", $"{bucketName}/{key}", 400);
            }

            // Additional validation: Part numbers must be between 1 and 10000
            foreach (var part in parts)
            {
                if (part.PartNumber < 1 || part.PartNumber > 10000)
                {
                    return S3Error("InvalidPartOrder", "Part numbers must be between 1 and 10000.", $"{bucketName}/{key}", 400);
                }
            }

            var completeRequest = new CompleteMultipartUploadRequest
            {
                UploadId = uploadId,
                Parts = parts
            };

            var storageResult = await _multipartStorage.CompleteMultipartUploadAsync(bucketName, key, completeRequest, cancellationToken);

            if (!storageResult.IsSuccess)
            {
                return storageResult.ErrorCode switch
                {
                    "NoSuchUpload" => S3Error("NoSuchUpload", storageResult.ErrorMessage!, $"{bucketName}/{key}", 404),
                    "InvalidPart" => S3Error("InvalidPart", storageResult.ErrorMessage!, $"{bucketName}/{key}", 400),
                    "EntityTooSmall" => S3Error("EntityTooSmall", storageResult.ErrorMessage!, $"{bucketName}/{key}", 400),
                    _ => S3Error("InternalError", storageResult.ErrorMessage!, $"{bucketName}/{key}", 500)
                };
            }

            var completeResponse = storageResult.Value!;

            var result = new CompleteMultipartUploadResult
            {
                Location = $"http://{Request.Host}/{bucketName}/{key}",
                Bucket = bucketName,
                Key = key,
                ETag = $"\"{completeResponse.ETag}\""
            };

            // Add S3-compliant response headers
            Response.Headers.Append("x-amz-version-id", "null");

            // Add checksum headers if any were provided in the request parts
            var firstPartWithChecksum = parts.FirstOrDefault(p =>
                !string.IsNullOrEmpty(p.ChecksumCRC32) ||
                !string.IsNullOrEmpty(p.ChecksumCRC32C) ||
                !string.IsNullOrEmpty(p.ChecksumSHA1) ||
                !string.IsNullOrEmpty(p.ChecksumSHA256));

            if (firstPartWithChecksum != null)
            {
                if (!string.IsNullOrEmpty(firstPartWithChecksum.ChecksumCRC32))
                    Response.Headers.Append("x-amz-checksum-crc32", firstPartWithChecksum.ChecksumCRC32);
                if (!string.IsNullOrEmpty(firstPartWithChecksum.ChecksumCRC32C))
                    Response.Headers.Append("x-amz-checksum-crc32c", firstPartWithChecksum.ChecksumCRC32C);
                if (!string.IsNullOrEmpty(firstPartWithChecksum.ChecksumSHA1))
                    Response.Headers.Append("x-amz-checksum-sha1", firstPartWithChecksum.ChecksumSHA1);
                if (!string.IsNullOrEmpty(firstPartWithChecksum.ChecksumSHA256))
                    Response.Headers.Append("x-amz-checksum-sha256", firstPartWithChecksum.ChecksumSHA256);
            }

            Response.ContentType = "application/xml";
            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error completing multipart upload {UploadId} for key {Key} in bucket {BucketName}", uploadId, key, bucketName);
            return S3Error("InternalError", "We encountered an internal error. Please try again.", $"{bucketName}/{key}", 500);
        }
    }

    [HttpDelete("{*key}")]
    [RequireQueryParameter("uploadId")]
    public async Task<IActionResult> AbortMultipartUpload(
        string bucketName,
        string key,
        [FromQuery] string uploadId,
        CancellationToken cancellationToken = default
    )
    {
        _logger.LogInformation("Aborting multipart upload {UploadId} for key {Key} in bucket {BucketName}", uploadId, key, bucketName);
        await _multipartStorage.AbortMultipartUploadAsync(bucketName, key, uploadId, cancellationToken);
        return NoContent();
    }

    [HttpGet("{*key}")]
    [RequireQueryParameter("uploadId")]
    public async Task<IActionResult> ListParts(
        string bucketName,
        string key,
        [FromQuery] string uploadId,
        [FromQuery(Name = "part-number-marker")]
        int? partNumberMarker,
        [FromQuery(Name = "max-parts")] int? maxParts,
        CancellationToken cancellationToken = default
    )
    {
        var parts = await _multipartStorage.ListPartsAsync(bucketName, key, uploadId, cancellationToken);

        var result = new ListPartsResult
        {
            Bucket = bucketName,
            Key = key,
            UploadId = uploadId,
            StorageClass = "STANDARD",
            PartNumberMarker = partNumberMarker ?? 0,
            MaxParts = maxParts ?? 1000,
            IsTruncated = false,
            Owner = new Owner(),
            Initiator = new Owner(),
            Parts = parts.Select(p => new Part
            {
                PartNumber = p.PartNumber,
                LastModified = p.LastModified.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'"),
                ETag = $"\"{p.ETag}\"",
                Size = p.Size
            }).ToList()
        };

        Response.ContentType = "application/xml";
        return Ok(result);
    }
}