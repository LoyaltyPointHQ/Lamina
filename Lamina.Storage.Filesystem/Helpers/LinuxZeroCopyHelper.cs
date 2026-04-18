using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Microsoft.Win32.SafeHandles;

namespace Lamina.Storage.Filesystem.Helpers;

/// <summary>
/// Thin wrapper around Linux <c>copy_file_range(2)</c> for zero-copy / server-side file concatenation.
/// On XFS/Btrfs/ZFS with reflinks enabled the kernel performs a CoW copy (O(1), no I/O).
/// On NFSv4.2 and SMB2+ the kernel issues a server-side COPY / FSCTL_SRV_COPYCHUNK, keeping bytes
/// entirely on the remote server. On unsupported setups (Linux &lt; 4.5, cross-device on &lt; 5.3)
/// the syscall returns errno and the caller must fall back to userspace read/write.
/// </summary>
public class LinuxZeroCopyHelper
{
    private readonly ILogger<LinuxZeroCopyHelper> _logger;
    private static readonly bool IsLinuxPlatform = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
    private int _fallbackLogged;

    // Linux errno values for "fall back" responses
    private const int ENOSYS = 38;
    private const int EOPNOTSUPP = 95;
    private const int EXDEV = 18;

    // Kernel hard limit per copy_file_range call (2 GiB - 4 KiB). Larger requests must be looped.
    private const long MaxChunkBytes = 0x7fff_f000L;

    [DllImport("libc", EntryPoint = "copy_file_range", SetLastError = true)]
    private static extern long copy_file_range(int fd_in, IntPtr off_in, int fd_out, IntPtr off_out, UIntPtr len, uint flags);

    public LinuxZeroCopyHelper(ILogger<LinuxZeroCopyHelper> logger)
    {
        _logger = logger;
    }

    public bool IsSupported => IsLinuxPlatform;

    /// <summary>
    /// Copies exactly <paramref name="length"/> bytes from <paramref name="src"/> (starting at its
    /// current file offset) into <paramref name="dst"/> (at its current offset), advancing both.
    /// Returns <c>true</c> on full success, <c>false</c> if the kernel signalled "not supported for
    /// this pair" (ENOSYS/EOPNOTSUPP/EXDEV) so the caller should fall back to userspace copy. Any
    /// other syscall error raises <see cref="IOException"/>.
    /// </summary>
    public bool TryCopyFileRange(SafeFileHandle src, SafeFileHandle dst, long length, CancellationToken cancellationToken = default)
    {
        if (!IsLinuxPlatform)
        {
            return false;
        }
        if (length == 0)
        {
            return true;
        }
        if (length < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(length));
        }

        var srcFd = (int)src.DangerousGetHandle();
        var dstFd = (int)dst.DangerousGetHandle();

        var remaining = length;
        while (remaining > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var requested = (UIntPtr)(ulong)Math.Min(remaining, MaxChunkBytes);
            var ret = copy_file_range(srcFd, IntPtr.Zero, dstFd, IntPtr.Zero, requested, 0u);

            if (ret < 0)
            {
                var errno = Marshal.GetLastPInvokeError();
                if (errno is ENOSYS or EOPNOTSUPP or EXDEV)
                {
                    LogFallbackOnce(errno);
                    return false;
                }
                throw new IOException($"copy_file_range failed with errno {errno}");
            }

            if (ret == 0)
            {
                // Kernel reports EOF on source before we expected it - part file shorter than claimed length.
                throw new IOException($"copy_file_range returned EOF with {remaining} bytes remaining");
            }

            remaining -= ret;
        }

        return true;
    }

    private void LogFallbackOnce(int errno)
    {
        // Log the first fallback on each process lifetime so production can see whether reflinks /
        // server-side copy are actually being used, without spamming logs when they're not.
        if (Interlocked.CompareExchange(ref _fallbackLogged, 1, 0) == 0)
        {
            _logger.LogInformation(
                "copy_file_range unsupported on this filesystem (errno={Errno}); multipart assembly will use userspace read/write fallback",
                errno);
        }
    }
}
