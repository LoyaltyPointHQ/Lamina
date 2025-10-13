using System.Buffers.Binary;
using System.IO.Hashing;
using System.Security.Cryptography;
using Force.Crc32;

namespace Lamina.Storage.Core.Helpers;

/// <summary>
/// Calculates multiple checksums simultaneously while streaming data.
/// Supports CRC32, CRC32C, SHA1, SHA256, and CRC64NVME algorithms.
/// </summary>
public class StreamingChecksumCalculator : IDisposable
{
    private readonly HashSet<string> _requestedAlgorithms;
    private readonly Dictionary<string, string> _providedChecksums;

    // Incremental hash calculators for SHA algorithms
    private IncrementalHash? _sha1;
    private IncrementalHash? _sha256;
    private uint? _crc32Hash;
    private uint? _crc32CHash;
    private Crc64? _crc64;

    // Buffer for CRC algorithms (CRC32, CRC32C, CRC64NVME require buffering with Force.Crc32 library)

    private bool _disposed;

    /// <summary>
    /// Creates a new streaming checksum calculator.
    /// </summary>
    /// <param name="algorithm">Primary algorithm from x-amz-checksum-algorithm header</param>
    /// <param name="providedChecksums">Dictionary of algorithm names to provided checksum values for validation</param>
    public StreamingChecksumCalculator(string? algorithm, Dictionary<string, string>? providedChecksums = null)
    {
        _requestedAlgorithms = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        _providedChecksums = providedChecksums ?? new Dictionary<string, string>();

        // Add primary algorithm if specified
        if (!string.IsNullOrEmpty(algorithm) && IsValidAlgorithm(algorithm))
        {
            _requestedAlgorithms.Add(algorithm.ToUpperInvariant());
        }

        // Add any algorithms that have provided checksums (for validation)
        foreach (var key in _providedChecksums.Keys)
        {
            if (IsValidAlgorithm(key))
            {
                _requestedAlgorithms.Add(key.ToUpperInvariant());
            }
        }
        
        if (_requestedAlgorithms.Contains("CRC32"))
        {
            _crc32Hash = 0;
        }
        
        if (_requestedAlgorithms.Contains("CRC32C"))
        {
            _crc32CHash = 0;
        }
        
        if (_requestedAlgorithms.Contains("CRC64NVME"))
        {
            _crc64 = new Crc64();
        }

        if (_requestedAlgorithms.Contains("SHA1"))
        {
            _sha1 = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);
        }

        if (_requestedAlgorithms.Contains("SHA256"))
        {
            _sha256 = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        }
    }

    /// <summary>
    /// Appends data to all active hash calculators.
    /// Call this method for each chunk of data as it's being written.
    /// </summary>
    public void Append(ReadOnlySpan<byte> data)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(StreamingChecksumCalculator));

        if (_crc32Hash != null)
            _crc32Hash = Crc32Algorithm.Append(_crc32Hash.Value, data.ToArray());
        if (_crc32CHash != null)
            _crc32CHash = Crc32CAlgorithm.Append(_crc32CHash.Value, data.ToArray());
        _crc64?.Append(data);

        // SHA algorithms support incremental hashing
        _sha1?.AppendData(data);
        _sha256?.AppendData(data);
    }

    /// <summary>
    /// Finalizes all hash calculations and returns the results with validation.
    /// </summary>
    public ChecksumResult Finish()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(StreamingChecksumCalculator));

        var calculated = new Dictionary<string, string>();
        if (_crc32Hash != null)
        {
            Span<byte> hash = stackalloc  byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(hash, _crc32Hash.Value);
            calculated["CRC32"] = Convert.ToBase64String(hash);
        }
        
        if (_crc32CHash != null)
        {
            Span<byte> hash = stackalloc  byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(hash, _crc32CHash.Value);
            calculated["CRC32C"] = Convert.ToBase64String(hash);
        }

        if (_crc64 != null)
        {
            var hash = _crc64.GetCurrentHash();
            Array.Reverse(hash);
            calculated["CRC64NVME"] = Convert.ToBase64String(hash);
        }

        // Get calculated checksums from incremental hash calculators
        if (_sha1 != null)
        {
            var hash = _sha1.GetHashAndReset();
            calculated["SHA1"] = Convert.ToBase64String(hash);
        }

        if (_sha256 != null)
        {
            var hash = _sha256.GetHashAndReset();
            calculated["SHA256"] = Convert.ToBase64String(hash);
        }

        // Validate against provided checksums
        foreach (var (algorithm, providedValue) in _providedChecksums)
        {
            var normalizedAlgorithm = algorithm.ToUpperInvariant();
            if (calculated.TryGetValue(normalizedAlgorithm, out var calculatedValue))
            {
                if (!string.Equals(providedValue, calculatedValue, StringComparison.Ordinal))
                {
                    return new ChecksumResult
                    {
                        IsValid = false,
                        ErrorMessage = $"Checksum mismatch for {normalizedAlgorithm}. Expected: {providedValue}, Got: {calculatedValue}",
                        CalculatedChecksums = calculated
                    };
                }
            }
        }

        return new ChecksumResult
        {
            IsValid = true,
            CalculatedChecksums = calculated
        };
    }

    /// <summary>
    /// Returns true if any checksums are being calculated.
    /// </summary>
    public bool HasChecksums => _requestedAlgorithms.Count > 0;

    public void Dispose()
    {
        if (_disposed)
            return;

        _sha1?.Dispose();
        _sha256?.Dispose();

        _disposed = true;
    }
    
    /// <summary>
    /// Validates algorithm name is supported.
    /// </summary>
    public static bool IsValidAlgorithm(string? algorithm)
    {
        if (string.IsNullOrEmpty(algorithm))
            return false;

        return algorithm.ToUpperInvariant() is "CRC32" or "CRC32C" or "SHA1" or "SHA256" or "CRC64NVME";
    }
}

/// <summary>
/// Represents a checksum request with algorithm and optional provided values.
/// </summary>
public class ChecksumRequest
{
    /// <summary>
    /// Primary algorithm from x-amz-checksum-algorithm header.
    /// </summary>
    public string? Algorithm { get; set; }

    /// <summary>
    /// Provided checksum values from headers (algorithm name -> base64 value).
    /// Used for validation.
    /// </summary>
    public Dictionary<string, string> ProvidedChecksums { get; set; } = new();
}

/// <summary>
/// Result of checksum calculation and validation.
/// </summary>
public class ChecksumResult
{
    /// <summary>
    /// Calculated checksums (algorithm name -> base64 value).
    /// </summary>
    public Dictionary<string, string> CalculatedChecksums { get; set; } = new();

    /// <summary>
    /// True if all provided checksums match calculated ones.
    /// </summary>
    public bool IsValid { get; set; }

    /// <summary>
    /// Error message if validation failed.
    /// </summary>
    public string? ErrorMessage { get; set; }
}
