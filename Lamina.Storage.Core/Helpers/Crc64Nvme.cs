using System.Buffers.Binary;

namespace Lamina.Storage.Core.Helpers;

/// <summary>
/// CRC-64/NVME (a.k.a. CRC-64/Rocksoft) implementation matching the algorithm used by
/// AWS S3 for the x-amz-checksum-crc64nvme header. This is a different algorithm than
/// CRC-64/ECMA-182 (which is what System.IO.Hashing.Crc64 implements).
///
/// Parameters per NVMe NVM Command Set Specification 1.0d / AWS CRT:
///   Polynomial (non-reflected) : 0xAD93D23594C93659
///   Initial value              : 0xFFFFFFFFFFFFFFFF
///   ReflectIn / ReflectOut     : true
///   XorOut                     : 0xFFFFFFFFFFFFFFFF
///
/// Reference vector: CRC-64/NVME of ASCII "123456789" == 0xAE8B14860A799888.
/// Track dotnet/runtime#123164 for native System.IO.Hashing.Crc64.Nvme support.
/// </summary>
public sealed class Crc64Nvme
{
    private const ulong InitialValue = 0xFFFFFFFFFFFFFFFFUL;
    private const ulong XorOut = 0xFFFFFFFFFFFFFFFFUL;

    // Bit-reflection of polynomial 0xAD93D23594C93659.
    private const ulong ReflectedPolynomial = 0x9A6C9329AC4BC9B5UL;

    // Slice-by-8 lookup tables (8 * 256 entries = 16 KB). Table8[0] is the standard
    // single-byte reflected CRC table; Table8[k] is "CRC of byte i followed by k zero bytes".
    // This breaks the per-byte serial dependency on `crc` and lets the CPU process 8 bytes
    // per iteration with instruction-level parallelism (~4-8x throughput vs byte-by-byte).
    private static readonly ulong[][] Table8 = BuildSliceBy8Table();

    private ulong _crc = InitialValue;

    public void Append(ReadOnlySpan<byte> data)
    {
        ulong crc = _crc;
        int i = 0;

        // Slice-by-8: 8 bytes per iteration, parallel table lookups XOR'd together.
        while (i + 8 <= data.Length)
        {
            ulong word = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(i, 8));
            crc ^= word;
            crc = Table8[7][(byte) crc       ] ^
                  Table8[6][(byte)(crc >>  8)] ^
                  Table8[5][(byte)(crc >> 16)] ^
                  Table8[4][(byte)(crc >> 24)] ^
                  Table8[3][(byte)(crc >> 32)] ^
                  Table8[2][(byte)(crc >> 40)] ^
                  Table8[1][(byte)(crc >> 48)] ^
                  Table8[0][(byte)(crc >> 56)];
            i += 8;
        }

        // Tail (≤7 bytes) using single-byte path.
        var table0 = Table8[0];
        while (i < data.Length)
        {
            crc = table0[(byte)(crc ^ data[i])] ^ (crc >> 8);
            i++;
        }

        _crc = crc;
    }

    public ulong GetCurrentHash() => _crc ^ XorOut;

    /// <summary>
    /// Writes the finalized CRC as 8 bytes in big-endian order (S3 wire format)
    /// into the provided destination. Allocation-free overload — pair with stackalloc.
    /// </summary>
    public void GetCurrentHash(Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt64BigEndian(destination, GetCurrentHash());
    }

    /// <summary>
    /// Returns the finalized CRC as a newly-allocated 8-byte array in big-endian order.
    /// Convenience overload; prefer <see cref="GetCurrentHash(Span{byte})"/> on hot paths.
    /// </summary>
    public byte[] GetCurrentHashBytes()
    {
        var bytes = new byte[8];
        GetCurrentHash(bytes);
        return bytes;
    }

    public void Reset() => _crc = InitialValue;

    /// <summary>
    /// Combines two CRCs to give the CRC of their concatenation: CRC(A || B) given
    /// CRC(A), CRC(B) and the byte length of B. crcA and crcB are *finalized* values
    /// (already XOR'd with XorOut). Used for multipart full-object checksum aggregation.
    /// Algorithm adapted from zlib crc32_combine64 (Mark Adler), GF(2) matrix
    /// multiplication, scaled to 64 bits.
    /// </summary>
    public static ulong Combine(ulong crcA, ulong crcB, long lengthB)
    {
        if (lengthB <= 0) return crcA;

        Span<ulong> even = stackalloc ulong[64];
        Span<ulong> odd = stackalloc ulong[64];

        // odd[] = operator for one zero bit (multiply by x mod p(x), reflected form).
        odd[0] = ReflectedPolynomial;
        ulong row = 1;
        for (int n = 1; n < 64; n++)
        {
            odd[n] = row;
            row <<= 1;
        }

        // even[] = operator for two zero bits.
        Gf2MatrixSquare(even, odd);
        // odd[] = operator for four zero bits.
        Gf2MatrixSquare(odd, even);

        // Walk lengthB (in bytes) bit by bit; on each iteration the operator squares,
        // doubling the number of zero bits it shifts. Applying the operator when the
        // current bit of len is set composes "shift crcA by 2^k zero bytes".
        ulong len = (ulong)lengthB;
        do
        {
            Gf2MatrixSquare(even, odd);
            if ((len & 1) != 0) crcA = Gf2MatrixTimes(even, crcA);
            len >>= 1;
            if (len == 0) break;

            Gf2MatrixSquare(odd, even);
            if ((len & 1) != 0) crcA = Gf2MatrixTimes(odd, crcA);
            len >>= 1;
        } while (len != 0);

        return crcA ^ crcB;
    }

    private static ulong Gf2MatrixTimes(ReadOnlySpan<ulong> mat, ulong vec)
    {
        ulong sum = 0;
        int idx = 0;
        while (vec != 0)
        {
            if ((vec & 1) != 0) sum ^= mat[idx];
            vec >>= 1;
            idx++;
        }
        return sum;
    }

    private static void Gf2MatrixSquare(Span<ulong> square, ReadOnlySpan<ulong> mat)
    {
        for (int n = 0; n < 64; n++)
            square[n] = Gf2MatrixTimes(mat, mat[n]);
    }

    private static ulong[][] BuildSliceBy8Table()
    {
        var t = new ulong[8][];
        for (int k = 0; k < 8; k++) t[k] = new ulong[256];

        // T[0] = standard reflected CRC byte table.
        for (int i = 0; i < 256; i++)
        {
            ulong crc = (ulong)i;
            for (int j = 0; j < 8; j++)
            {
                if ((crc & 1) != 0)
                    crc = (crc >> 1) ^ ReflectedPolynomial;
                else
                    crc >>= 1;
            }
            t[0][i] = crc;
        }

        // T[k][i] = T[0][T[k-1][i] & 0xFF] ^ (T[k-1][i] >> 8)
        // i.e. CRC of byte i followed by k zero bytes.
        for (int i = 0; i < 256; i++)
        {
            for (int k = 1; k < 8; k++)
            {
                ulong prev = t[k - 1][i];
                t[k][i] = t[0][prev & 0xFF] ^ (prev >> 8);
            }
        }

        return t;
    }
}
