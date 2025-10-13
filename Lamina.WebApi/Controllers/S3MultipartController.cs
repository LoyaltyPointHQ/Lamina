using System.Xml.Serialization;
using Lamina.Core.Models;
using Lamina.Core.Streaming;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Helpers;
using Lamina.WebApi.Authorization;
using Lamina.WebApi.Controllers.Attributes;
using Lamina.WebApi.Controllers.Base;
using Microsoft.AspNetCore.Mvc;

namespace Lamina.WebApi.Controllers;

[Route("{bucketName}")]
public class S3MultipartController : S3ControllerBase
{
    private readonly IMultipartUploadStorageFacade _multipartStorage;
    private readonly IBucketStorageFacade _bucketStorage;
    private readonly IObjectStorageFacade _objectStorage;
    private readonly ILogger<S3MultipartController> _logger;

    public S3MultipartController(
        IMultipartUploadStorageFacade multipartStorage,
        IBucketStorageFacade bucketStorage,
        IObjectStorageFacade objectStorage,
        ILogger<S3MultipartController> logger
    )
    {
        _multipartStorage = multipartStorage;
        _bucketStorage = bucketStorage;
        _objectStorage = objectStorage;
        _logger = logger;
    }

    private static XmlDeserializationResult<List<CompletedPart>> TryDeserializeCompleteRequest(string xmlContent)
    {
        // Try without namespace first (for compatibility with most clients)
        if (TryDeserializeWithoutNamespace(xmlContent, out var partsNoNs))
        {
            return XmlDeserializationResult<List<CompletedPart>>.Success(partsNoNs);
        }

        // Try with S3 namespace
        if (TryDeserializeWithNamespace(xmlContent, out var partsWithNs))
        {
            return XmlDeserializationResult<List<CompletedPart>>.Success(partsWithNs);
        }

        return XmlDeserializationResult<List<CompletedPart>>.Error("The XML you provided was not well-formed or did not validate against our published schema.");
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
    [S3Authorize(S3Operations.List, S3ResourceType.Object)]
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
    [S3Authorize(S3Operations.Write, S3ResourceType.Object)]
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

        // Parse checksum algorithm header
        Request.Headers.TryGetValue("x-amz-checksum-algorithm", out var checksumAlgorithm);
        var checksumAlgorithmValue = checksumAlgorithm.ToString();
        
        // Validate checksum algorithm if provided
        if (!string.IsNullOrEmpty(checksumAlgorithmValue) && !StreamingChecksumCalculator.IsValidAlgorithm(checksumAlgorithmValue))
        {
            return S3Error("InvalidArgument", $"Invalid checksum algorithm: {checksumAlgorithmValue}. Valid values are: CRC32, CRC32C, SHA1, SHA256, CRC64NVME", $"/{bucketName}/{key}", 400);
        }

        var request = new InitiateMultipartUploadRequest
        {
            Key = key,
            ContentType = Request.ContentType,
            Metadata = Request.Headers
                .Where(h => h.Key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
                .ToDictionary(h => h.Key.Substring(11), h => h.Value.ToString()),
            ChecksumAlgorithm = string.IsNullOrEmpty(checksumAlgorithmValue) ? null : checksumAlgorithmValue
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
    [S3Authorize(S3Operations.Write, S3ResourceType.Object)]
    public async Task<IActionResult> UploadPart(
        string bucketName,
        string key,
        [FromQuery] int partNumber,
        [FromQuery] string uploadId,
        CancellationToken cancellationToken = default
    )
    {
        // Log comprehensive request headers for diagnostics
        LogUploadRequestHeaders(_logger, "UploadPart", bucketName, key, ("Part", partNumber), ("UploadId", uploadId));

        // Check if this is an UploadPartCopy operation
        if (Request.Headers.ContainsKey("x-amz-copy-source"))
        {
            return await HandleUploadPartCopy(bucketName, key, partNumber, uploadId, cancellationToken);
        }

        // Validate Content-Length header (required by S3 API for regular uploads)
        var contentLengthError = ValidateContentLengthHeader($"/{bucketName}/{key}");
        if (contentLengthError != null)
        {
            _logger.LogWarning("UploadPart request missing Content-Length header for part {PartNumber} of upload {UploadId}", partNumber, uploadId);
            return contentLengthError;
        }

        // Validate x-amz-content-sha256 header when using AWS Signature V4
        var contentSha256Error = ValidateContentSha256Header($"/{bucketName}/{key}");
        if (contentSha256Error != null)
        {
            _logger.LogWarning("UploadPart request has invalid or missing x-amz-content-sha256 header for part {PartNumber} of upload {UploadId}", partNumber, uploadId);
            return contentSha256Error;
        }

        // Parse checksum headers
        Request.Headers.TryGetValue("x-amz-checksum-algorithm", out var checksumAlgorithm);
        Request.Headers.TryGetValue("x-amz-checksum-crc32", out var checksumCrc32);
        Request.Headers.TryGetValue("x-amz-checksum-crc32c", out var checksumCrc32c);
        Request.Headers.TryGetValue("x-amz-checksum-sha1", out var checksumSha1);
        Request.Headers.TryGetValue("x-amz-checksum-sha256", out var checksumSha256);
        Request.Headers.TryGetValue("x-amz-checksum-crc64nvme", out var checksumCrc64nvme);
        
        var checksumAlgorithmValue = checksumAlgorithm.ToString();
        
        // Validate checksum algorithm if provided
        if (!string.IsNullOrEmpty(checksumAlgorithmValue) && !StreamingChecksumCalculator.IsValidAlgorithm(checksumAlgorithmValue))
        {
            return S3Error("InvalidArgument", $"Invalid checksum algorithm: {checksumAlgorithmValue}. Valid values are: CRC32, CRC32C, SHA1, SHA256, CRC64NVME", $"/{bucketName}/{key}", 400);
        }

        // Build checksum request if any checksums were provided
        ChecksumRequest? checksumRequest = null;
        if (!string.IsNullOrEmpty(checksumAlgorithmValue) || 
            !string.IsNullOrEmpty(checksumCrc32.ToString()) ||
            !string.IsNullOrEmpty(checksumCrc32c.ToString()) ||
            !string.IsNullOrEmpty(checksumSha1.ToString()) ||
            !string.IsNullOrEmpty(checksumSha256.ToString()) ||
            !string.IsNullOrEmpty(checksumCrc64nvme.ToString()))
        {
            checksumRequest = new ChecksumRequest
            {
                Algorithm = string.IsNullOrEmpty(checksumAlgorithmValue) ? null : checksumAlgorithmValue,
                ProvidedChecksums = new Dictionary<string, string>()
            };
            
            if (!string.IsNullOrEmpty(checksumCrc32.ToString()))
                checksumRequest.ProvidedChecksums["CRC32"] = checksumCrc32.ToString();
            if (!string.IsNullOrEmpty(checksumCrc32c.ToString()))
                checksumRequest.ProvidedChecksums["CRC32C"] = checksumCrc32c.ToString();
            if (!string.IsNullOrEmpty(checksumSha1.ToString()))
                checksumRequest.ProvidedChecksums["SHA1"] = checksumSha1.ToString();
            if (!string.IsNullOrEmpty(checksumSha256.ToString()))
                checksumRequest.ProvidedChecksums["SHA256"] = checksumSha256.ToString();
            if (!string.IsNullOrEmpty(checksumCrc64nvme.ToString()))
                checksumRequest.ProvidedChecksums["CRC64NVME"] = checksumCrc64nvme.ToString();
        }

        try
        {
            // Use PipeReader from the request body
            var reader = Request.BodyReader;

            // Check if there's a chunk validator from the authentication middleware (for streaming uploads)
            var chunkValidator = HttpContext.Items["ChunkValidator"] as IChunkSignatureValidator;

            var partResult = chunkValidator != null
                ? await _multipartStorage.UploadPartAsync(bucketName, key, uploadId, partNumber, reader, chunkValidator, checksumRequest, cancellationToken)
                : await _multipartStorage.UploadPartAsync(bucketName, key, uploadId, partNumber, reader, checksumRequest, cancellationToken);

            if (!partResult.IsSuccess)
            {
                // Handle specific error cases
                if (partResult.ErrorCode == "InvalidChecksum")
                {
                    _logger.LogWarning("Checksum validation failed for part {PartNumber} of upload {UploadId}: {ErrorMessage}",
                        partNumber, uploadId, partResult.ErrorMessage);
                    return S3Error("InvalidChecksum", partResult.ErrorMessage!, $"/{bucketName}/{key}", 400);
                }

                // If we had a chunk validator and the operation failed, it's likely due to invalid signature
                if (chunkValidator != null || partResult.ErrorCode == "SignatureDoesNotMatch")
                {
                    _logger.LogWarning("Chunk signature validation failed for part {PartNumber} of upload {UploadId}", partNumber, uploadId);
                    return S3Error("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", $"/{bucketName}/{key}", 403);
                }

                // Otherwise, generic error
                _logger.LogError("Failed to upload part {PartNumber} of upload {UploadId}: {ErrorCode} - {ErrorMessage}",
                    partNumber, uploadId, partResult.ErrorCode, partResult.ErrorMessage);
                return StatusCode(500);
            }

            var part = partResult.Value!;
            Response.Headers.Append("ETag", $"\"{part.ETag}\"");
            
            // Add checksum headers if present
            if (!string.IsNullOrEmpty(part.ChecksumCRC32))
                Response.Headers.Append("x-amz-checksum-crc32", part.ChecksumCRC32);
            if (!string.IsNullOrEmpty(part.ChecksumCRC32C))
                Response.Headers.Append("x-amz-checksum-crc32c", part.ChecksumCRC32C);
            if (!string.IsNullOrEmpty(part.ChecksumSHA1))
                Response.Headers.Append("x-amz-checksum-sha1", part.ChecksumSHA1);
            if (!string.IsNullOrEmpty(part.ChecksumSHA256))
                Response.Headers.Append("x-amz-checksum-sha256", part.ChecksumSHA256);
            if (!string.IsNullOrEmpty(part.ChecksumCRC64NVME))
                Response.Headers.Append("x-amz-checksum-crc64nvme", part.ChecksumCRC64NVME);
            
            return Ok();
        }
        catch (InvalidOperationException ex)
        {
            return S3Error("NoSuchUpload", ex.Message, $"{bucketName}/{key}", 404);
        }
    }

    private async Task<IActionResult> HandleUploadPartCopy(
        string bucketName,
        string key,
        int partNumber,
        string uploadId,
        CancellationToken cancellationToken)
    {
        try
        {
            // Parse x-amz-copy-source header
            if (!Request.Headers.TryGetValue("x-amz-copy-source", out var copySourceValue))
            {
                return S3Error("InvalidArgument", "x-amz-copy-source header is required for copy operations", $"/{bucketName}/{key}", 400);
            }

            var copySource = copySourceValue.ToString();

            // Copy source format: /sourceBucket/sourceKey or sourceBucket/sourceKey
            var copySourceParts = copySource.TrimStart('/').Split('/', 2);
            if (copySourceParts.Length != 2)
            {
                return S3Error("InvalidArgument", "Invalid x-amz-copy-source format. Expected: /sourceBucket/sourceKey", $"/{bucketName}/{key}", 400);
            }

            var sourceBucketName = Uri.UnescapeDataString(copySourceParts[0]);
            var sourceKey = Uri.UnescapeDataString(copySourceParts[1]);

            // Parse optional byte range header (x-amz-copy-source-range: bytes=start-end)
            long? byteRangeStart = null;
            long? byteRangeEnd = null;

            if (Request.Headers.TryGetValue("x-amz-copy-source-range", out var rangeValue))
            {
                var rangeStr = rangeValue.ToString();
                if (rangeStr.StartsWith("bytes=", StringComparison.OrdinalIgnoreCase))
                {
                    var bytesRange = rangeStr.Substring(6); // Remove "bytes=" prefix
                    var rangeParts = bytesRange.Split('-');

                    if (rangeParts.Length == 2 &&
                        long.TryParse(rangeParts[0], out var start) &&
                        long.TryParse(rangeParts[1], out var end))
                    {
                        byteRangeStart = start;
                        byteRangeEnd = end;
                    }
                    else
                    {
                        return S3Error("InvalidArgument", "Invalid x-amz-copy-source-range format. Expected: bytes=start-end", $"/{bucketName}/{key}", 400);
                    }
                }
            }

            _logger.LogInformation(
                "UploadPartCopy: source={SourceBucket}/{SourceKey}, dest={DestBucket}/{DestKey}, part={PartNumber}, uploadId={UploadId}, range={RangeStart}-{RangeEnd}",
                sourceBucketName, sourceKey, bucketName, key, partNumber, uploadId, byteRangeStart, byteRangeEnd);

            // Call the facade to copy the object part
            var uploadPart = await _objectStorage.CopyObjectPartAsync(
                sourceBucketName,
                sourceKey,
                bucketName,
                key,
                uploadId,
                partNumber,
                byteRangeStart,
                byteRangeEnd,
                cancellationToken);

            if (uploadPart == null)
            {
                // Check if source object exists
                var sourceExists = await _objectStorage.ObjectExistsAsync(sourceBucketName, sourceKey, cancellationToken);
                if (!sourceExists)
                {
                    return S3Error("NoSuchKey", "The specified key does not exist.", $"/{sourceBucketName}/{sourceKey}", 404);
                }

                // Check if the byte range is invalid
                if (byteRangeStart.HasValue || byteRangeEnd.HasValue)
                {
                    return S3Error("InvalidRange", "The requested range is not satisfiable", $"/{sourceBucketName}/{sourceKey}", 416);
                }

                // Generic error
                return StatusCode(500);
            }

            // Return success with ETag and copy metadata
            Response.Headers.Append("ETag", $"\"{uploadPart.ETag}\"");
            Response.Headers.Append("x-amz-copy-source-version-id", "null"); // No versioning support yet

            // Return CopyPartResult XML response
            var copyPartResult = new CopyPartResult
            {
                LastModified = uploadPart.LastModified.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'"),
                ETag = $"\"{uploadPart.ETag}\"",
                ChecksumCRC32 = uploadPart.ChecksumCRC32,
                ChecksumCRC32C = uploadPart.ChecksumCRC32C,
                ChecksumCRC64NVME = uploadPart.ChecksumCRC64NVME,
                ChecksumSHA1 = uploadPart.ChecksumSHA1,
                ChecksumSHA256 = uploadPart.ChecksumSHA256
            };

            // Serialize to XML and return - Content() method handles ContentType
            var xmlSerializer = new XmlSerializer(typeof(CopyPartResult));
            await using var stringWriter = new StringWriter();
            xmlSerializer.Serialize(stringWriter, copyPartResult);

            return Content(stringWriter.ToString(), "application/xml");
        }
        catch (InvalidOperationException ex)
        {
            return S3Error("NoSuchUpload", ex.Message, $"{bucketName}/{key}", 404);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in UploadPartCopy for {Bucket}/{Key} part {PartNumber}", bucketName, key, partNumber);
            return StatusCode(500);
        }
    }

    [HttpPost("{*key}")]
    [RequireQueryParameter("uploadId")]
    [RequireNoQueryParameters("uploads")]
    [S3Authorize(S3Operations.Write, S3ResourceType.Object)]
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

            var parts = deserializationResult.Value!;

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

            // Add checksum fields if any were provided in the request parts
            var firstPartWithChecksum = parts.FirstOrDefault(p =>
                !string.IsNullOrEmpty(p.ChecksumCRC32) ||
                !string.IsNullOrEmpty(p.ChecksumCRC32C) ||
                !string.IsNullOrEmpty(p.ChecksumSHA1) ||
                !string.IsNullOrEmpty(p.ChecksumSHA256) ||
                !string.IsNullOrEmpty(p.ChecksumCRC64NVME));

            var result = new CompleteMultipartUploadResult
            {
                Location = $"http://{Request.Host}/{bucketName}/{key}",
                Bucket = bucketName,
                Key = key,
                ETag = $"\"{completeResponse.ETag}\"",
                ChecksumCRC32 = firstPartWithChecksum?.ChecksumCRC32,
                ChecksumCRC32C = firstPartWithChecksum?.ChecksumCRC32C,
                ChecksumCRC64NVME = firstPartWithChecksum?.ChecksumCRC64NVME,
                ChecksumSHA1 = firstPartWithChecksum?.ChecksumSHA1,
                ChecksumSHA256 = firstPartWithChecksum?.ChecksumSHA256
            };

            // Add S3-compliant response headers
            Response.Headers.Append("x-amz-version-id", "null");

            // Add checksum headers if any were provided
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
                if (!string.IsNullOrEmpty(firstPartWithChecksum.ChecksumCRC64NVME))
                    Response.Headers.Append("x-amz-checksum-crc64nvme", firstPartWithChecksum.ChecksumCRC64NVME);
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
    [S3Authorize(S3Operations.Delete, S3ResourceType.Object)]
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
    [S3Authorize(S3Operations.Read, S3ResourceType.Object)]
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
                Size = p.Size,
                ChecksumCRC32 = p.ChecksumCRC32,
                ChecksumCRC32C = p.ChecksumCRC32C,
                ChecksumCRC64NVME = p.ChecksumCRC64NVME,
                ChecksumSHA1 = p.ChecksumSHA1,
                ChecksumSHA256 = p.ChecksumSHA256
            }).ToList()
        };

        Response.ContentType = "application/xml";
        return Ok(result);
    }

    [HttpHead("{*key}")]
    [RequireQueryParameter("uploadId")]
    [S3Authorize(S3Operations.Read, S3ResourceType.Object)]
    public async Task<IActionResult> HeadParts(
        string bucketName,
        string key,
        [FromQuery] string uploadId,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            var parts = await _multipartStorage.ListPartsAsync(bucketName, key, uploadId, cancellationToken);

            // If no parts and upload doesn't exist, return 404
            // We can check if the upload exists by attempting to list parts
            // An empty list could mean either no parts uploaded yet or upload doesn't exist
            // To be safe, we'll check if the upload metadata exists
            var uploads = await _multipartStorage.ListMultipartUploadsAsync(bucketName, cancellationToken);
            var uploadExists = uploads.Any(u => u.UploadId == uploadId && u.Key == key);

            if (!uploadExists)
            {
                Response.StatusCode = 404;
                Response.ContentType = "application/xml";
                return new EmptyResult();
            }

            // Add metadata to response headers for HEAD request
            Response.Headers.Append("x-amz-parts-count", parts.Count.ToString());
            if (parts.Any())
            {
                Response.Headers.Append("x-amz-last-part-number", parts.Max(p => p.PartNumber).ToString());
                Response.Headers.Append("x-amz-total-size", parts.Sum(p => p.Size).ToString());
            }

            return Ok();
        }
        catch (InvalidOperationException)
        {
            Response.StatusCode = 404;
            Response.ContentType = "application/xml";
            return new EmptyResult();
        }
    }
}