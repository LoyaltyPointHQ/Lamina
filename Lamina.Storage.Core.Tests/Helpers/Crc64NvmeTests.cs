using System.Text;
using Lamina.Storage.Core.Helpers;

namespace Lamina.Storage.Core.Tests.Helpers;

public class Crc64NvmeTests
{
    [Fact]
    public void Compute_123456789_MatchesNvmeReferenceVector()
    {
        // Canonical CRC-64/NVME check value per NVMe spec / AWS CRT.
        const ulong ExpectedCheckValue = 0xAE8B14860A799888UL;

        var crc = new Crc64Nvme();
        crc.Append(Encoding.ASCII.GetBytes("123456789"));

        Assert.Equal(ExpectedCheckValue, crc.GetCurrentHash());
    }

    [Fact]
    public void Combine_TwoArbitraryByteSequences_EqualsCrcOfConcatenation()
    {
        // Property-based: for any A and B, Combine(CRC(A), CRC(B), len(B)) == CRC(A || B).
        var rng = new Random(42);
        for (int trial = 0; trial < 20; trial++)
        {
            var a = new byte[rng.Next(1, 1000)];
            var b = new byte[rng.Next(1, 1000)];
            rng.NextBytes(a);
            rng.NextBytes(b);

            var crcA = ComputeFinal(a);
            var crcB = ComputeFinal(b);

            var concat = new byte[a.Length + b.Length];
            a.CopyTo(concat, 0);
            b.CopyTo(concat, a.Length);
            var crcConcat = ComputeFinal(concat);

            var combined = Crc64Nvme.Combine(crcA, crcB, b.Length);
            Assert.Equal(crcConcat, combined);
        }
    }

    [Fact]
    public void Append_TailOnly_LessThan8Bytes_MatchesByteByByte()
    {
        // Verifies that the tail loop (≤7 bytes, after the slice-by-8 fast path
        // exits) produces the same result as feeding the bytes one-by-one.
        var rng = new Random(123);
        for (int len = 0; len < 8; len++)
        {
            var data = new byte[len];
            rng.NextBytes(data);

            var bulk = new Crc64Nvme();
            bulk.Append(data);

            var oneByOne = new Crc64Nvme();
            for (int i = 0; i < data.Length; i++)
                oneByOne.Append(data.AsSpan(i, 1));

            Assert.Equal(oneByOne.GetCurrentHash(), bulk.GetCurrentHash());
        }
    }

    [Fact]
    public void Append_LongInput_BulkAndChunkedYieldSameResult()
    {
        // Verifies slice-by-8 path: feed 1024 bytes at once vs in 13-byte chunks
        // (forces tail handling at every chunk boundary, exercising both paths).
        var rng = new Random(456);
        var data = new byte[1024];
        rng.NextBytes(data);

        var bulk = new Crc64Nvme();
        bulk.Append(data);

        var chunked = new Crc64Nvme();
        for (int i = 0; i < data.Length; i += 13)
            chunked.Append(data.AsSpan(i, Math.Min(13, data.Length - i)));

        Assert.Equal(bulk.GetCurrentHash(), chunked.GetCurrentHash());
    }

    [Fact]
    public void Combine_EmptyB_ReturnsCrcA()
    {
        var crcA = ComputeFinal(Encoding.ASCII.GetBytes("foo"));
        Assert.Equal(crcA, Crc64Nvme.Combine(crcA, 0UL, 0));
    }

    private static ulong ComputeFinal(byte[] data)
    {
        var crc = new Crc64Nvme();
        crc.Append(data);
        return crc.GetCurrentHash();
    }

    [Fact]
    public void GetCurrentHashBytes_123456789_IsBigEndianEncoding()
    {
        // 0xAE8B14860A799888 in big-endian byte order = AE 8B 14 86 0A 79 98 88
        // (S3 wire format: most significant byte first).
        var expected = new byte[] { 0xAE, 0x8B, 0x14, 0x86, 0x0A, 0x79, 0x98, 0x88 };

        var crc = new Crc64Nvme();
        crc.Append(Encoding.ASCII.GetBytes("123456789"));

        Assert.Equal(expected, crc.GetCurrentHashBytes());
    }
}
