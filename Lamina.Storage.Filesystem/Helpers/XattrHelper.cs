using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Lamina.Storage.Filesystem.Helpers;

public class XattrHelper
{
    private readonly ILogger<XattrHelper> _logger;
    private readonly string _prefix;
    private static readonly bool IsLinuxOrMacOs = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                                                  RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

    // P/Invoke declarations for Linux xattr functions
    [DllImport("libc", SetLastError = true)]
    private static extern int setxattr(string path, string name, byte[] value, int size, int flags);

    [DllImport("libc", SetLastError = true)]
    private static extern int getxattr(string path, string name, byte[]? value, int size);

    [DllImport("libc", SetLastError = true)]
    private static extern int removexattr(string path, string name);

    [DllImport("libc", SetLastError = true)]
    private static extern int listxattr(string path, byte[]? list, int size);

    public XattrHelper(string prefix, ILogger<XattrHelper> logger)
    {
        _prefix = prefix;
        _logger = logger;
    }

    public bool IsSupported => IsLinuxOrMacOs;

    public bool SetAttribute(string filePath, string name, string value)
    {
        if (!IsSupported)
        {
            _logger.LogError("Extended attributes are not supported on this platform");
            return false;
        }

        try
        {
            var attrName = GetAttributeName(name);
            var valueBytes = Encoding.UTF8.GetBytes(value);

            // Truncate if too large (typically 64KB limit on ext4)
            const int maxSize = 65536;
            if (valueBytes.Length > maxSize)
            {
                _logger.LogWarning("Attribute {AttributeName} value is too large ({Size} bytes), truncating to {MaxSize} bytes",
                    attrName, valueBytes.Length, maxSize);
                valueBytes = valueBytes[..maxSize];
            }

            var result = setxattr(filePath, attrName, valueBytes, valueBytes.Length, 0);
            if (result != 0)
            {
                var error = Marshal.GetLastPInvokeError();
                _logger.LogError("Failed to set extended attribute {AttributeName} on {FilePath}: errno {Error}",
                    attrName, filePath, error);
                return false;
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Exception setting extended attribute {AttributeName} on {FilePath}", name, filePath);
            return false;
        }
    }

    public string? GetAttribute(string filePath, string name)
    {
        if (!IsSupported)
        {
            return null;
        }

        try
        {
            var attrName = GetAttributeName(name);

            // First, get the size of the attribute
            var size = getxattr(filePath, attrName, null, 0);
            if (size < 0)
            {
                // Attribute doesn't exist or error occurred
                return null;
            }

            if (size == 0)
            {
                return string.Empty;
            }

            // Allocate buffer and read the attribute
            var buffer = new byte[size];
            var actualSize = getxattr(filePath, attrName, buffer, size);
            if (actualSize < 0)
            {
                var error = Marshal.GetLastPInvokeError();
                _logger.LogError("Failed to get extended attribute {AttributeName} from {FilePath}: errno {Error}",
                    attrName, filePath, error);
                return null;
            }

            return Encoding.UTF8.GetString(buffer, 0, actualSize);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Exception getting extended attribute {AttributeName} from {FilePath}", name, filePath);
            return null;
        }
    }

    public bool RemoveAttribute(string filePath, string name)
    {
        if (!IsSupported)
        {
            return false;
        }

        try
        {
            var attrName = GetAttributeName(name);
            var result = removexattr(filePath, attrName);
            if (result != 0)
            {
                var error = Marshal.GetLastPInvokeError();
                // ENODATA (61) means attribute doesn't exist, which we consider success
                if (error != 61)
                {
                    _logger.LogError("Failed to remove extended attribute {AttributeName} from {FilePath}: errno {Error}",
                        attrName, filePath, error);
                    return false;
                }
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Exception removing extended attribute {AttributeName} from {FilePath}", name, filePath);
            return false;
        }
    }

    public List<string> ListAttributes(string filePath)
    {
        var attributes = new List<string>();

        if (!IsSupported)
        {
            return attributes;
        }

        try
        {
            // First, get the size needed for the list
            var size = listxattr(filePath, null, 0);
            if (size < 0)
            {
                return attributes;
            }

            if (size == 0)
            {
                return attributes;
            }

            // Allocate buffer and read the list
            var buffer = new byte[size];
            var actualSize = listxattr(filePath, buffer, size);
            if (actualSize < 0)
            {
                var error = Marshal.GetLastPInvokeError();
                _logger.LogError("Failed to list extended attributes from {FilePath}: errno {Error}", filePath, error);
                return attributes;
            }

            // Parse the null-separated list
            var attrList = Encoding.UTF8.GetString(buffer, 0, actualSize);
            var attrNames = attrList.Split('\0', StringSplitOptions.RemoveEmptyEntries);

            // Filter to only our prefix and remove the prefix
            var prefixDot = _prefix + ".";
            foreach (var attrName in attrNames)
            {
                if (attrName.StartsWith(prefixDot))
                {
                    attributes.Add(attrName[prefixDot.Length..]);
                }
            }

            return attributes;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Exception listing extended attributes from {FilePath}", filePath);
            return attributes;
        }
    }

    public bool RemoveAllAttributes(string filePath)
    {
        if (!IsSupported)
        {
            return false;
        }

        try
        {
            var attributes = ListAttributes(filePath);
            var success = true;

            foreach (var attr in attributes)
            {
                var removed = RemoveAttribute(filePath, attr);
                if (!removed)
                {
                    success = false;
                }
            }

            return success;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Exception removing all extended attributes from {FilePath}", filePath);
            return false;
        }
    }

    private string GetAttributeName(string name)
    {
        return $"{_prefix}.{name}";
    }
}