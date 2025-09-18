using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Security.Cryptography;
using S3Test.Models;

namespace S3Test.Services;

public class InMemoryMultipartUploadService : IMultipartUploadService
{
    private readonly ConcurrentDictionary<string, MultipartUpload> _uploads = new();
    private readonly IBucketService _bucketService;
    private readonly IObjectService _objectService;

    public InMemoryMultipartUploadService(IBucketService bucketService, IObjectService objectService)
    {
        _bucketService = bucketService;
        _objectService = objectService;
    }

    public async Task<InitiateMultipartUploadResponse> InitiateMultipartUploadAsync(
        string bucketName,
        InitiateMultipartUploadRequest request,
        CancellationToken cancellationToken = default)
    {
        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            throw new InvalidOperationException($"Bucket '{bucketName}' does not exist");
        }

        var uploadId = Guid.NewGuid().ToString();
        var upload = new MultipartUpload
        {
            UploadId = uploadId,
            BucketName = bucketName,
            Key = request.Key,
            Initiated = DateTime.UtcNow,
            Parts = new List<UploadPart>(),
            Metadata = request.Metadata ?? new Dictionary<string, string>(),
            ContentType = request.ContentType ?? "application/octet-stream"
        };

        _uploads[uploadId] = upload;

        return new InitiateMultipartUploadResponse
        {
            UploadId = uploadId,
            BucketName = bucketName,
            Key = request.Key
        };
    }

    public async Task<UploadPartResponse> UploadPartAsync(
        string bucketName,
        string key,
        string uploadId,
        int partNumber,
        PipeReader dataReader,
        CancellationToken cancellationToken = default)
    {
        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            throw new InvalidOperationException($"Bucket '{bucketName}' does not exist");
        }

        if (!_uploads.TryGetValue(uploadId, out var upload))
        {
            throw new InvalidOperationException($"Upload '{uploadId}' not found");
        }

        if (upload.BucketName != bucketName || upload.Key != key)
        {
            throw new InvalidOperationException("Upload ID does not match bucket and key");
        }

        // Read data from PipeReader
        var dataSegments = new List<byte[]>();
        long totalSize = 0;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await dataReader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (buffer.Length > 0)
                {
                    // Copy data from buffer
                    var data = buffer.ToArray();
                    dataSegments.Add(data);
                    totalSize += data.Length;
                }

                dataReader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }

            // Combine all segments
            var partData = new byte[totalSize];
            int offset = 0;
            foreach (var segment in dataSegments)
            {
                Buffer.BlockCopy(segment, 0, partData, offset, segment.Length);
                offset += segment.Length;
            }

            var etag = ComputeETag(partData);
            var part = new UploadPart
            {
                PartNumber = partNumber,
                ETag = etag,
                Size = partData.Length,
                LastModified = DateTime.UtcNow,
                Data = partData
            };

            lock (upload.Parts)
            {
                upload.Parts.RemoveAll(p => p.PartNumber == partNumber);
                upload.Parts.Add(part);
                upload.Parts.Sort((a, b) => a.PartNumber.CompareTo(b.PartNumber));
            }

            return new UploadPartResponse
            {
                ETag = etag,
                PartNumber = partNumber
            };
        }
        finally
        {
            await dataReader.CompleteAsync();
        }
    }

    public async Task<CompleteMultipartUploadResponse?> CompleteMultipartUploadAsync(
        string bucketName,
        string key,
        CompleteMultipartUploadRequest request,
        CancellationToken cancellationToken = default)
    {
        if (!await _bucketService.BucketExistsAsync(bucketName, cancellationToken))
        {
            return null;
        }

        if (!_uploads.TryGetValue(request.UploadId, out var upload))
        {
            return null;
        }

        if (upload.BucketName != bucketName || upload.Key != key)
        {
            return null;
        }

        var completedPartNumbers = request.Parts.Select(p => p.PartNumber).ToHashSet();
        List<UploadPart> uploadedParts;
        lock (upload.Parts)
        {
            uploadedParts = upload.Parts.Where(p => completedPartNumbers.Contains(p.PartNumber)).OrderBy(p => p.PartNumber).ToList();
        }

        if (uploadedParts.Count != request.Parts.Count)
        {
            return null;
        }

        for (int i = 0; i < request.Parts.Count; i++)
        {
            var requestedPart = request.Parts.FirstOrDefault(p => p.PartNumber == uploadedParts[i].PartNumber);
            if (requestedPart == null)
            {
                return null;
            }

            // Normalize ETags for comparison - remove quotes if present
            var requestedETag = requestedPart.ETag.Trim('"');
            var uploadedETag = uploadedParts[i].ETag.Trim('"');

            if (requestedETag != uploadedETag)
            {
                return null;
            }
        }

        // Create a pipe to transfer combined data
        var pipe = new Pipe();
        var writer = pipe.Writer;
        var reader = pipe.Reader;

        // Start writing combined data in background
        _ = Task.Run(async () =>
        {
            try
            {
                foreach (var part in uploadedParts)
                {
                    var memory = writer.GetMemory(part.Data.Length);
                    part.Data.CopyTo(memory);
                    writer.Advance(part.Data.Length);
                    await writer.FlushAsync();
                }
            }
            finally
            {
                await writer.CompleteAsync();
            }
        });

        // Compute final ETag from combined data (we still need this for the response)
        var combinedData = uploadedParts.SelectMany(p => p.Data).ToArray();
        var finalETag = ComputeETag(combinedData);

        var putRequest = new PutObjectRequest
        {
            Key = key,
            ContentType = upload.ContentType,
            Metadata = upload.Metadata
        };

        await _objectService.PutObjectAsync(bucketName, key, reader, putRequest, cancellationToken);

        _uploads.TryRemove(request.UploadId, out _);

        return new CompleteMultipartUploadResponse
        {
            Location = $"/{bucketName}/{key}",
            BucketName = bucketName,
            Key = key,
            ETag = finalETag
        };
    }

    public Task<bool> AbortMultipartUploadAsync(
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default)
    {
        if (_uploads.TryRemove(uploadId, out var upload))
        {
            return Task.FromResult(upload.BucketName == bucketName && upload.Key == key);
        }

        return Task.FromResult(false);
    }

    public Task<List<UploadPart>> ListPartsAsync(
        string bucketName,
        string key,
        string uploadId,
        CancellationToken cancellationToken = default)
    {
        if (_uploads.TryGetValue(uploadId, out var upload))
        {
            if (upload.BucketName == bucketName && upload.Key == key)
            {
                lock (upload.Parts)
                {
                    return Task.FromResult(upload.Parts.Select(p => new UploadPart
                    {
                        PartNumber = p.PartNumber,
                        ETag = p.ETag,
                        Size = p.Size,
                        LastModified = p.LastModified
                    }).ToList());
                }
            }
        }

        return Task.FromResult(new List<UploadPart>());
    }

    public Task<List<MultipartUpload>> ListMultipartUploadsAsync(
        string bucketName,
        CancellationToken cancellationToken = default)
    {
        var uploads = _uploads.Values
            .Where(u => u.BucketName == bucketName)
            .OrderBy(u => u.Initiated)
            .ToList();

        return Task.FromResult(uploads);
    }

    private static string ComputeETag(byte[] data)
    {
        using var md5 = MD5.Create();
        var hash = md5.ComputeHash(data);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}