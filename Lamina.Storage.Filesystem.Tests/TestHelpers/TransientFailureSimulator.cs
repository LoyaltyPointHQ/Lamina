using System.IO;
using System.Runtime.InteropServices;

namespace Lamina.Storage.Filesystem.Tests.TestHelpers;

/// <summary>
/// Helper class for creating simulated transient filesystem exceptions
/// that mimic real CIFS and NFS error conditions.
/// </summary>
public static class TransientFailureSimulator
{
    /// <summary>
    /// Creates an IOException with a CIFS "process cannot access" error message.
    /// </summary>
    public static IOException CreateCIFSProcessInUseException()
    {
        return new IOException("The process cannot access the file because it is being used by another process.");
    }

    /// <summary>
    /// Creates an IOException with a CIFS "network path not found" error message.
    /// </summary>
    public static IOException CreateCIFSNetworkPathNotFoundException()
    {
        return new IOException("The network path was not found.");
    }

    /// <summary>
    /// Creates an IOException with a CIFS "access is denied" error message.
    /// </summary>
    public static IOException CreateCIFSAccessDeniedException()
    {
        return new IOException("Access is denied.");
    }

    /// <summary>
    /// Creates an IOException with a CIFS "sharing violation" error message.
    /// </summary>
    public static IOException CreateCIFSSharingViolationException()
    {
        return new IOException("The process cannot access the file because it is being used by another process. (Sharing violation)");
    }

    /// <summary>
    /// Creates an IOException with a CIFS "network name no longer available" error message.
    /// </summary>
    public static IOException CreateCIFSNetworkNameUnavailableException()
    {
        return new IOException("The specified network name is no longer available.");
    }

    /// <summary>
    /// Creates an IOException with a CIFS "directory not empty" error message.
    /// </summary>
    public static IOException CreateCIFSDirectoryNotEmptyException()
    {
        return new IOException("The directory is not empty.");
    }

    /// <summary>
    /// Creates an IOException with an NFS "stale file handle" error message.
    /// </summary>
    public static IOException CreateNFSStaleFileHandleException()
    {
        return new IOException("Stale NFS file handle");
    }

    /// <summary>
    /// Creates an IOException with an NFS "input/output error" message.
    /// </summary>
    public static IOException CreateNFSInputOutputErrorException()
    {
        return new IOException("Input/output error");
    }

    /// <summary>
    /// Creates an IOException with an NFS "no such file or directory" message (can happen with stale handles).
    /// </summary>
    public static IOException CreateNFSNoSuchFileException()
    {
        return new IOException("No such file or directory");
    }

    /// <summary>
    /// Creates an IOException with ESTALE HResult (errno 116 on Linux).
    /// This simulates the Linux-specific ESTALE error code for stale NFS file handles.
    /// </summary>
    public static IOException CreateNFSESTALEException()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            // ESTALE is errno 116 on Linux
            // HResult format for errno: 0x8007 followed by errno in hex
            // 116 decimal = 0x74 hex, so HResult = 0x80070074
            var exception = new IOException("Stale file handle");

            // Use reflection to set HResult (it's a protected property)
            var hresultProperty = typeof(IOException).GetProperty("HResult");
            if (hresultProperty != null)
            {
                hresultProperty.SetValue(exception, unchecked((int)0x80070074));
            }

            return exception;
        }

        // On non-Linux platforms, just use the message
        return new IOException("Stale file handle");
    }

    /// <summary>
    /// Creates a non-transient IOException that should NOT be retried.
    /// </summary>
    public static IOException CreateNonTransientException()
    {
        return new IOException("Disk quota exceeded");
    }

    /// <summary>
    /// Creates an UnauthorizedAccessException (transient for CIFS).
    /// </summary>
    public static UnauthorizedAccessException CreateCIFSUnauthorizedAccessException()
    {
        return new UnauthorizedAccessException("Access to the path is denied.");
    }
}
