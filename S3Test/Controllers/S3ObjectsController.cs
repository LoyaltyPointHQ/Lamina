using Microsoft.AspNetCore.Mvc;
using S3Test.Models;
using S3Test.Services;
using System.Xml.Serialization;
using System.IO;
using System.IO.Pipelines;

namespace S3Test.Controllers;

[ApiController]
[Route("{bucketName}")]
[Produces("application/xml")]
public class S3ObjectsController : ControllerBase
{
    private readonly IObjectService _objectService;
    private readonly IBucketService _bucketService;
    private readonly IMultipartUploadService _multipartUploadService;
    private readonly ILogger<S3ObjectsController> _logger;

    public S3ObjectsController(
        IObjectService objectService,
        IBucketService bucketService,
        IMultipartUploadService multipartUploadService,
        ILogger<S3ObjectsController> logger)
    {
        _objectService = objectService;
        _bucketService = bucketService;
        _multipartUploadService = multipartUploadService;
        _logger = logger;
    }

    [HttpPut("{*key}")]
    [DisableRequestSizeLimit]
    public async Task<IActionResult> PutObject(
        string bucketName,
        string key,
        [FromQuery] int? partNumber,
        [FromQuery] string? uploadId,
        CancellationToken cancellationToken = default)
    {
        // If uploadId and partNumber are present, this is a part upload
        if (!string.IsNullOrEmpty(uploadId) && partNumber.HasValue)
        {
            _logger.LogInformation("Uploading part {PartNumber} for upload {UploadId} in bucket {BucketName}, key {Key}", partNumber.Value, uploadId, bucketName, key);
            return await UploadPartInternal(bucketName, key, partNumber.Value, uploadId, cancellationToken);
        }

        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            var error = new S3Error
            {
                Code = "NoSuchBucket",
                Message = "The specified bucket does not exist",
                Resource = bucketName
            };
            Response.StatusCode = 404;
            Response.ContentType = "application/xml";
            return new ObjectResult(error);
        }

        var contentType = Request.ContentType ?? "application/octet-stream";

        var putRequest = new PutObjectRequest
        {
            Key = key,
            ContentType = contentType,
            Metadata = Request.Headers
                .Where(h => h.Key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
                .ToDictionary(h => h.Key.Substring(11), h => h.Value.ToString())
        };

        // Use PipeReader from the request body
        var reader = Request.BodyReader;

        _logger.LogInformation("Putting object {Key} in bucket {BucketName}", key, bucketName);
        var s3Object = await _objectService.PutObjectAsync(bucketName, key, reader, putRequest, cancellationToken);
        if (s3Object == null)
        {
            _logger.LogError("Failed to put object {Key} in bucket {BucketName}", key, bucketName);
            return StatusCode(500);
        }

        _logger.LogInformation("Object {Key} stored successfully in bucket {BucketName}, ETag: {ETag}, Size: {Size}", key, bucketName, s3Object.ETag, s3Object.Size);
        Response.Headers.Append("ETag", $"\"{s3Object.ETag}\"");
        Response.Headers.Append("x-amz-version-id", "null");
        return Ok();
    }

    [HttpGet("{*key}")]
    public async Task<IActionResult> GetObject(
        string bucketName,
        string key,
        [FromQuery] string? uploadId,
        [FromQuery(Name = "part-number-marker")] int? partNumberMarker,
        [FromQuery(Name = "max-parts")] int? maxParts,
        CancellationToken cancellationToken = default)
    {
        // List parts for a multipart upload
        if (!string.IsNullOrEmpty(uploadId))
        {
            return await ListPartsInternal(bucketName, key, uploadId, partNumberMarker, maxParts, cancellationToken);
        }
        _logger.LogInformation("Getting object {Key} from bucket {BucketName}", key, bucketName);
        var response = await _objectService.GetObjectAsync(bucketName, key, cancellationToken);
        if (response == null)
        {
            _logger.LogWarning("Object not found: {Key} in bucket {BucketName}", key, bucketName);
            var error = new S3Error
            {
                Code = "NoSuchKey",
                Message = "The specified key does not exist.",
                Resource = $"{bucketName}/{key}"
            };
            Response.StatusCode = 404;
            Response.ContentType = "application/xml";
            return new ObjectResult(error);
        }

        Response.Headers.Append("ETag", $"\"{response.ETag}\"");
        Response.Headers.Append("Last-Modified", response.LastModified.ToString("R"));
        Response.Headers.Append("Content-Length", response.ContentLength.ToString());

        foreach (var metadata in response.Metadata)
        {
            Response.Headers.Append($"x-amz-meta-{metadata.Key}", metadata.Value);
        }

        // Create a pipe to stream data back
        var pipe = new Pipe();
        var writer = pipe.Writer;

        // Start writing to pipe in background
        _ = Task.Run(async () =>
        {
            try
            {
                await _objectService.WriteObjectToPipeAsync(bucketName, key, writer, cancellationToken);
            }
            catch
            {
                await writer.CompleteAsync();
            }
        });

        return new FileStreamResult(pipe.Reader.AsStream(), response.ContentType);
    }

    [HttpDelete("{*key}")]
    public async Task<IActionResult> DeleteObject(
        string bucketName,
        string key,
        [FromQuery] string? uploadId,
        CancellationToken cancellationToken = default)
    {
        // Abort multipart upload
        if (!string.IsNullOrEmpty(uploadId))
        {
            _logger.LogInformation("Aborting multipart upload {UploadId} for key {Key} in bucket {BucketName}", uploadId, key, bucketName);
            return await AbortMultipartUploadInternal(bucketName, key, uploadId, cancellationToken);
        }
        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            // S3 returns 204 even if bucket doesn't exist for delete operations
            return NoContent();
        }

        _logger.LogInformation("Deleting object {Key} from bucket {BucketName}", key, bucketName);
        await _objectService.DeleteObjectAsync(bucketName, key, cancellationToken);
        return NoContent();
    }

    [HttpHead("{*key}")]
    public async Task<IActionResult> HeadObject(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var objectInfo = await _objectService.GetObjectInfoAsync(bucketName, key, cancellationToken);
        if (objectInfo == null)
        {
            return NotFound();
        }

        Response.Headers.Append("ETag", $"\"{objectInfo.ETag}\"");
        Response.Headers.Append("Content-Length", objectInfo.Size.ToString());
        Response.Headers.Append("Last-Modified", objectInfo.LastModified.ToString("R"));

        return Ok();
    }

    [HttpPost("{*key}")]
    public async Task<IActionResult> PostObject(
        string bucketName,
        string key,
        [FromQuery(Name = "uploads")] string? uploads,
        [FromQuery(Name = "uploadId")] string? uploadId,
        CancellationToken cancellationToken = default)
    {
        // Initiate multipart upload - check if 'uploads' query parameter is present
        if (Request.Query.ContainsKey("uploads"))
        {
            return await InitiateMultipartUploadInternal(bucketName, key, cancellationToken);
        }

        // Complete multipart upload
        if (!string.IsNullOrEmpty(uploadId))
        {
            return await CompleteMultipartUploadInternal(bucketName, key, uploadId, cancellationToken);
        }

        return NotFound();
    }

    private async Task<IActionResult> InitiateMultipartUploadInternal(
        string bucketName,
        string key,
        CancellationToken cancellationToken)
    {

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
            var response = await _multipartUploadService.InitiateMultipartUploadAsync(bucketName, request, cancellationToken);

            var result = new InitiateMultipartUploadResult
            {
                Bucket = response.BucketName,
                Key = response.Key,
                UploadId = response.UploadId
            };

            Response.ContentType = "application/xml";
            return Ok(result);
        }
        catch (InvalidOperationException)
        {
            var error = new S3Error
            {
                Code = "NoSuchBucket",
                Message = "The specified bucket does not exist",
                Resource = bucketName
            };
            Response.StatusCode = 404;
            Response.ContentType = "application/xml";
            return new ObjectResult(error);
        }
    }

    private async Task<IActionResult> UploadPartInternal(
        string bucketName,
        string key,
        int partNumber,
        string uploadId,
        CancellationToken cancellationToken)
    {
        try
        {
            // Use PipeReader from the request body
            var reader = Request.BodyReader;
            var response = await _multipartUploadService.UploadPartAsync(bucketName, key, uploadId, partNumber, reader, cancellationToken);
            Response.Headers.Append("ETag", $"\"{response.ETag}\"");
            return Ok();
        }
        catch (InvalidOperationException ex)
        {
            var error = new S3Error
            {
                Code = "NoSuchUpload",
                Message = ex.Message,
                Resource = $"{bucketName}/{key}"
            };
            Response.StatusCode = 404;
            Response.ContentType = "application/xml";
            return new ObjectResult(error);
        }
    }

    private async Task<IActionResult> CompleteMultipartUploadInternal(
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken)
    {
        try
        {
            // Read the XML request body
            List<CompletedPart> parts = new();

            using (var reader = new StreamReader(Request.Body))
            {
                var xmlContent = await reader.ReadToEndAsync();

                // Try to deserialize - first without namespace (most common), then with namespace
                try
                {
                    // Try without namespace first (for compatibility with most clients)
                    var serializerNoNamespace = new XmlSerializer(typeof(CompleteMultipartUploadXmlNoNamespace));
                    using var stringReader = new StringReader(xmlContent);
                    var completeRequestNoNs = serializerNoNamespace.Deserialize(stringReader) as CompleteMultipartUploadXmlNoNamespace;

                    if (completeRequestNoNs != null && completeRequestNoNs.Parts != null)
                    {
                        parts = completeRequestNoNs.Parts.Select(p => new CompletedPart
                        {
                            PartNumber = p.PartNumber,
                            ETag = p.ETag
                        }).ToList();
                    }
                }
                catch
                {
                    // If that fails, try with S3 namespace
                    var serializerWithNamespace = new XmlSerializer(typeof(CompleteMultipartUploadXml));
                    using var stringReader = new StringReader(xmlContent);
                    var completeRequestWithNs = serializerWithNamespace.Deserialize(stringReader) as CompleteMultipartUploadXml;

                    if (completeRequestWithNs != null && completeRequestWithNs.Parts != null)
                    {
                        parts = completeRequestWithNs.Parts.Select(p => new CompletedPart
                        {
                            PartNumber = p.PartNumber,
                            ETag = p.ETag
                        }).ToList();
                    }
                }
            }

            if (parts.Count == 0)
            {
                return BadRequest();
            }


            var request = new CompleteMultipartUploadRequest
            {
                UploadId = uploadId,
                Parts = parts
            };

            var response = await _multipartUploadService.CompleteMultipartUploadAsync(bucketName, key, request, cancellationToken);
            if (response == null)
            {
                var error = new S3Error
                {
                    Code = "InvalidPart",
                    Message = "One or more of the specified parts could not be found. Check ETags and part numbers.",
                    Resource = $"{bucketName}/{key}"
                };
                Response.StatusCode = 400;
                Response.ContentType = "application/xml";
                return new ObjectResult(error);
            }

            var result = new CompleteMultipartUploadResult
            {
                Location = $"http://{Request.Host}/{bucketName}/{key}",
                Bucket = response.BucketName,
                Key = response.Key,
                ETag = $"\"{response.ETag}\""
            };

            Response.ContentType = "application/xml";
            return Ok(result);
        }
        catch (InvalidOperationException ex)
        {
            var error = new S3Error
            {
                Code = "NoSuchUpload",
                Message = ex.Message,
                Resource = $"{bucketName}/{key}"
            };
            Response.StatusCode = 404;
            Response.ContentType = "application/xml";
            return new ObjectResult(error);
        }
    }

    private async Task<IActionResult> AbortMultipartUploadInternal(
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken)
    {
        await _multipartUploadService.AbortMultipartUploadAsync(bucketName, key, uploadId, cancellationToken);
        return NoContent();
    }

    private async Task<IActionResult> ListPartsInternal(
        string bucketName,
        string key,
        string uploadId,
        int? partNumberMarker,
        int? maxParts,
        CancellationToken cancellationToken)
    {
        var parts = await _multipartUploadService.ListPartsAsync(bucketName, key, uploadId, cancellationToken);

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