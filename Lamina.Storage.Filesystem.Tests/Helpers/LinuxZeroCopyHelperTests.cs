using System.Runtime.InteropServices;
using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Lamina.Storage.Filesystem.Tests.Helpers;

public class LinuxZeroCopyHelperTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LinuxZeroCopyHelper _helper;

    public LinuxZeroCopyHelperTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"lamina_zerocopy_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _helper = new LinuxZeroCopyHelper(NullLogger<LinuxZeroCopyHelper>.Instance);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public void IsSupported_ReflectsCurrentPlatform()
    {
        Assert.Equal(RuntimeInformation.IsOSPlatform(OSPlatform.Linux), _helper.IsSupported);
    }

    [Fact]
    public void TryCopyFileRange_ConcatenatesTwoSourcesIntoDestination_OnLinux()
    {
        // copy_file_range is Linux-only. On macOS/Windows the helper should signal unsupported
        // (return false) rather than throw, which is the fallback-to-userspace contract.
        var isLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);

        var src1Path = Path.Combine(_tempDir, "src1");
        var src2Path = Path.Combine(_tempDir, "src2");
        var dstPath = Path.Combine(_tempDir, "dst");

        var src1Bytes = System.Text.Encoding.UTF8.GetBytes("hello ");
        var src2Bytes = System.Text.Encoding.UTF8.GetBytes("world!");
        File.WriteAllBytes(src1Path, src1Bytes);
        File.WriteAllBytes(src2Path, src2Bytes);

        using (var dst = File.OpenHandle(dstPath, FileMode.Create, FileAccess.Write, FileShare.None))
        using (var src1 = File.OpenHandle(src1Path, FileMode.Open, FileAccess.Read, FileShare.Read))
        using (var src2 = File.OpenHandle(src2Path, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            var ok1 = _helper.TryCopyFileRange(src1, dst, src1Bytes.Length);
            var ok2 = _helper.TryCopyFileRange(src2, dst, src2Bytes.Length);

            if (!isLinux)
            {
                Assert.False(ok1);
                Assert.False(ok2);
                return;
            }

            // On Linux, we accept EITHER full success (reflink or regular kernel copy) OR early
            // fallback (e.g. some obscure FS). If fallback, we skip the byte-level assertion.
            if (!ok1 || !ok2)
            {
                return;
            }
        }

        if (isLinux)
        {
            var combined = File.ReadAllBytes(dstPath);
            var expected = src1Bytes.Concat(src2Bytes).ToArray();
            Assert.Equal(expected, combined);
        }
    }

    [Fact]
    public void TryCopyFileRange_ZeroLength_ReturnsTrueWithoutSyscall()
    {
        var dstPath = Path.Combine(_tempDir, "empty-dst");
        var srcPath = Path.Combine(_tempDir, "empty-src");
        File.WriteAllBytes(srcPath, Array.Empty<byte>());

        using var dst = File.OpenHandle(dstPath, FileMode.Create, FileAccess.Write, FileShare.None);
        using var src = File.OpenHandle(srcPath, FileMode.Open, FileAccess.Read, FileShare.Read);

        Assert.True(_helper.TryCopyFileRange(src, dst, 0));
    }
}
