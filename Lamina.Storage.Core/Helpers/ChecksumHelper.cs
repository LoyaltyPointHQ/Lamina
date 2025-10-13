using System.Buffers.Binary;
using System.IO.Hashing;
using System.Security.Cryptography;
using Force.Crc32;

namespace Lamina.Storage.Core.Helpers;

/// <summary>
/// Helper class for computing checksums selectively based on which algorithms are needed.
/// Supports single-pass computation of multiple checksums for efficiency.
/// </summary>
public static class ChecksumHelper
{
    /// <summary>
    /// Computes only the specified checksums from a file in a single pass.
    /// </summary>
    /// <param name="filePath">Path to the file to checksum</param>
    /// <param name="algorithms">List of algorithm names to compute (e.g., "CRC32", "SHA256")</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Dictionary with algorithm names as keys and base64-encoded checksums as values</returns>
    public static async Task<Dictionary<string, string>> ComputeSelectiveChecksumsFromFileAsync(
        string filePath,
        IEnumerable<string> algorithms,
        CancellationToken cancellationToken = default)
    {
        var algorithmList = algorithms.ToList();
        if (algorithmList.Count == 0)
        {
            return new Dictionary<string, string>();
        }

        // Initialize hash algorithms based on requested list
        var crc32 = algorithmList.Contains("CRC32") ? new Crc32Algorithm() : null;
        var crc32c = algorithmList.Contains("CRC32C") ? new Crc32CAlgorithm() : null;
        var crc64nvme = algorithmList.Contains("CRC64NVME") ? new Crc64Algorithm() : null;
        var sha1 = algorithmList.Contains("SHA1") ? SHA1.Create() : null;
        var sha256 = algorithmList.Contains("SHA256") ? SHA256.Create() : null;

        try
        {
            await using var fileStream = new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 81920,
                useAsync: true);

            var buffer = new byte[81920];
            int bytesRead;

            // Single pass through the file, computing all requested checksums
            while ((bytesRead = await fileStream.ReadAsync(buffer, cancellationToken)) > 0)
            {
                var span = buffer.AsSpan(0, bytesRead);

                crc32?.Append(span);
                crc32c?.Append(span);
                crc64nvme?.Append(span);
                sha1?.TransformBlock(buffer, 0, bytesRead, null, 0);
                sha256?.TransformBlock(buffer, 0, bytesRead, null, 0);
            }

            // Finalize and collect results
            var results = new Dictionary<string, string>();

            if (crc32 != null)
            {
                var hash = crc32.GetCurrentHash();
                results["CRC32"] = Convert.ToBase64String(hash);
            }

            if (crc32c != null)
            {
                var hash = crc32c.GetCurrentHash();
                results["CRC32C"] = Convert.ToBase64String(hash);
            }

            if (crc64nvme != null)
            {
                var hash = crc64nvme.GetCurrentHash();
                results["CRC64NVME"] = Convert.ToBase64String(hash);
            }

            if (sha1 != null)
            {
                sha1.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                results["SHA1"] = Convert.ToBase64String(sha1.Hash!);
            }

            if (sha256 != null)
            {
                sha256.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                results["SHA256"] = Convert.ToBase64String(sha256.Hash!);
            }

            return results;
        }
        finally
        {
            crc32?.Dispose();
            crc32c?.Dispose();
            crc64nvme?.Dispose();
            sha1?.Dispose();
            sha256?.Dispose();
        }
    }

    /// <summary>
    /// Helper classes for CRC algorithms that need to accumulate data across multiple calls.
    /// </summary>
    private class Crc32Algorithm : IDisposable
    {
        private uint _crc = 0;

        public void Append(ReadOnlySpan<byte> data)
        {
            _crc = Force.Crc32.Crc32Algorithm.Append(_crc, data.ToArray());
        }

        public byte[] GetCurrentHash()
        {
            Span<byte> hash = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(hash, _crc);
            return hash.ToArray();
        }

        public void Dispose() { }
    }

    private class Crc32CAlgorithm : IDisposable
    {
        private uint _crc = 0;

        public void Append(ReadOnlySpan<byte> data)
        {
            _crc = Force.Crc32.Crc32CAlgorithm.Append(_crc, data.ToArray());
        }

        public byte[] GetCurrentHash()
        {
            Span<byte> hash = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(hash, _crc);
            return hash.ToArray();
        }

        public void Dispose() { }
    }

    private class Crc64Algorithm : IDisposable
    {
        private readonly Crc64 _crc64 = new Crc64();

        public void Append(ReadOnlySpan<byte> data)
        {
            _crc64.Append(data);
        }

        public byte[] GetCurrentHash()
        {
            var hash = _crc64.GetCurrentHash();
            Array.Reverse(hash);
            return hash;
        }

        public void Dispose() { }
    }
}
