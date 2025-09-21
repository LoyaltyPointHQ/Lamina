using Microsoft.AspNetCore.Mvc;
using Lamina.Models;
using Lamina.Services;
using System.Xml.Serialization;
using System.IO;
using System.IO.Pipelines;
using Lamina.Storage.Abstract;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Helpers;

namespace Lamina.Controllers;

[ApiController]
[Route("{bucketName}")]
[Produces("application/xml")]
public class S3ObjectsController : ControllerBase
{
    private readonly IObjectStorageFacade _objectStorage;
    private readonly IMultipartUploadStorageFacade _multipartStorage;
    private readonly IBucketStorageFacade _bucketStorage;
    private readonly FilesystemStorageSettings? _filesystemSettings;
    private readonly ILogger<S3ObjectsController> _logger;
    private readonly MetadataStorageMode _metadataMode;
    private readonly string _inlineMetadataDirectoryName;

    public S3ObjectsController(
        IObjectStorageFacade objectStorage,
        IMultipartUploadStorageFacade multipartStorage,
        IBucketStorageFacade bucketStorage,
        IServiceProvider serviceProvider,
        ILogger<S3ObjectsController> logger)
    {
        _objectStorage = objectStorage;
        _multipartStorage = multipartStorage;
        _bucketStorage = bucketStorage;
        _logger = logger;

        // Try to get FilesystemStorageSettings if we're using filesystem storage
        _filesystemSettings = serviceProvider.GetService<FilesystemStorageSettings>();

        if (_filesystemSettings != null)
        {
            _metadataMode = _filesystemSettings.MetadataMode;
            _inlineMetadataDirectoryName = _filesystemSettings.InlineMetadataDirectoryName;
        }
        else
        {
            // Default values for non-filesystem storage
            _metadataMode = MetadataStorageMode.SeparateDirectory;
            _inlineMetadataDirectoryName = ".lamina-meta";
        }
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
        // Validate the object key
        if (!ObjectKeyValidator.IsValidObjectKey(key, _metadataMode, _inlineMetadataDirectoryName))
        {
            return new ObjectResult(new S3Error
            {
                Code = "InvalidObjectName",
                Message = $"Object key contains forbidden metadata directory name '{_inlineMetadataDirectoryName}'",
                Resource = $"/{bucketName}/{key}",
            })
            { StatusCode = 400 };
        }
        // If uploadId and partNumber are present, this is a part upload
        if (!string.IsNullOrEmpty(uploadId) && partNumber.HasValue)
        {
            _logger.LogInformation("Uploading part {PartNumber} for upload {UploadId} in bucket {BucketName}, key {Key}", partNumber.Value, uploadId, bucketName, key);
            return await UploadPartInternal(bucketName, key, partNumber.Value, uploadId, cancellationToken);
        }

        if (!await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken))
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

        var s3Object = await _objectStorage.PutObjectAsync(bucketName, key, reader, putRequest, cancellationToken);
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

        var metadata = await _objectStorage.GetObjectInfoAsync(bucketName, key, cancellationToken);
        if (metadata == null)
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

        Response.Headers.Append("ETag", $"\"{metadata.ETag}\"");
        Response.Headers.Append("Last-Modified", metadata.LastModified.ToString("R"));
        Response.Headers.Append("Content-Length", metadata.Size.ToString());

        foreach (var metadataItem in metadata.Metadata)
        {
            Response.Headers.Append($"x-amz-meta-{metadataItem.Key}", metadataItem.Value);
        }

        // Create a pipe to stream data back
        var pipe = new Pipe();
        var writer = pipe.Writer;

        // Start writing to pipe in background
        _ = Task.Run(async () =>
        {
            try
            {
                await _objectStorage.WriteObjectToStreamAsync(bucketName, key, writer, cancellationToken);
                await writer.CompleteAsync();
            }
            catch (Exception ex)
            {
                await writer.CompleteAsync(ex);
            }
        });

        return new FileStreamResult(pipe.Reader.AsStream(), metadata.ContentType);
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
        if (!await _bucketStorage.BucketExistsAsync(bucketName, cancellationToken))
        {
            // S3 returns 204 even if bucket doesn't exist for delete operations
            return NoContent();
        }

        _logger.LogInformation("Deleting object {Key} from bucket {BucketName}", key, bucketName);
        await _objectStorage.DeleteObjectAsync(bucketName, key, cancellationToken);
        return NoContent();
    }

    [HttpHead("{*key}")]
    public async Task<IActionResult> HeadObject(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var objectInfo = await _objectStorage.GetObjectInfoAsync(bucketName, key, cancellationToken);
        if (objectInfo == null)
        {
            return NotFound();
        }

        Response.Headers.Append("ETag", $"\"{objectInfo.ETag}\"");
        Response.Headers.Append("Content-Length", objectInfo.Size.ToString());
        Response.Headers.Append("Content-Type", objectInfo.ContentType);
        Response.Headers.Append("Last-Modified", objectInfo.LastModified.ToString("R"));

        // Add custom metadata headers with x-amz-meta- prefix
        foreach (var metadata in objectInfo.Metadata)
        {
            Response.Headers.Append($"x-amz-meta-{metadata.Key}", metadata.Value);
        }

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
        // Validate the object key
        if (!ObjectKeyValidator.IsValidObjectKey(key, _metadataMode, _inlineMetadataDirectoryName))
        {
            return new ObjectResult(new S3Error
            {
                Code = "InvalidObjectName",
                Message = $"Object key contains forbidden metadata directory name '{_inlineMetadataDirectoryName}'",
                Resource = $"/{bucketName}/{key}",
            })
            { StatusCode = 400 };
        }
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
            var part = await _multipartStorage.UploadPartAsync(bucketName, key, uploadId, partNumber, reader, cancellationToken);
            Response.Headers.Append("ETag", $"\"{part.ETag}\"");
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

            var completeRequest = new CompleteMultipartUploadRequest
            {
                UploadId = uploadId,
                Parts = parts
            };

            var completeResponse = await _multipartStorage.CompleteMultipartUploadAsync(bucketName, key, completeRequest, cancellationToken);

            var result = new CompleteMultipartUploadResult
            {
                Location = $"http://{Request.Host}/{bucketName}/{key}",
                Bucket = bucketName,
                Key = key,
                ETag = $"\"{completeResponse.ETag}\""
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
        await _multipartStorage.AbortMultipartUploadAsync(bucketName, key, uploadId, cancellationToken);
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