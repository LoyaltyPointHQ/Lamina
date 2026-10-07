using System.IO.Pipelines;
using System.Security.Cryptography;
using Lamina.Core.Models;
using Lamina.Core.Streaming;

namespace Lamina.Storage.Core.Helpers;

public sealed record UploadIntegrityResult(long Size, string ETag, Dictionary<string, string> Checksums);

/// <summary>Processes request bytes independently of storage. The destination stream is borrowed.</summary>
public sealed class UploadContentProcessor(IChunkedDataParser? parser)
{
    public async Task<StorageResult<UploadIntegrityResult>> ProcessAsync(
        PipeReader source, Stream destination, IChunkSignatureValidator? validator,
        ChecksumRequest? checksums, byte[]? expectedMd5, CancellationToken cancellationToken = default)
    {
        using var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
        using var calculator = new StreamingChecksumCalculator(checksums?.Algorithm, checksums?.ProvidedChecksums);
        long size = 0;
        void Append(ReadOnlySpan<byte> bytes)
        {
            md5.AppendData(bytes);
            calculator.Append(bytes);
            size += bytes.Length;
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (validator != null)
            {
                ArgumentNullException.ThrowIfNull(parser);
                var parsed = validator.ExpectsTrailers
                    ? await parser.ParseChunkedDataWithTrailersToStreamAsync(source, destination, validator, Append, cancellationToken)
                    : await parser.ParseChunkedDataToStreamAsync(source, destination, validator, Append, cancellationToken);
                if (!parsed.Success)
                    return StorageResult<UploadIntegrityResult>.Error("SignatureDoesNotMatch", "Chunk signature validation failed");
                TrailerChecksumMerger.MergeIntoCalculator(parsed.Trailers, calculator);
            }
            else
            {
                while (true)
                {
                    var read = await source.ReadAsync(cancellationToken);
                    if (read.IsCanceled)
                        throw new OperationCanceledException(cancellationToken);
                    try
                    {
                        foreach (var segment in read.Buffer)
                        {
                            await destination.WriteAsync(segment, cancellationToken);
                            Append(segment.Span);
                        }
                    }
                    finally { source.AdvanceTo(read.Buffer.End); }
                    if (read.IsCompleted) break;
                }
            }

            cancellationToken.ThrowIfCancellationRequested();
            var calculated = calculator.Finish();
            if (!calculated.IsValid)
                return StorageResult<UploadIntegrityResult>.Error("InvalidChecksum", calculated.ErrorMessage ?? "Checksum validation failed");
            var digest = md5.GetHashAndReset();
            if (expectedMd5 != null && !CryptographicOperations.FixedTimeEquals(digest, expectedMd5))
                return StorageResult<UploadIntegrityResult>.Error("BadDigest", "The Content-MD5 you specified did not match what we received.");
            return StorageResult<UploadIntegrityResult>.Success(new(size, Convert.ToHexString(digest).ToLowerInvariant(), calculated.CalculatedChecksums));
        }
        finally
        {
            await source.CompleteAsync();
        }
    }
}
