using Lamina.Storage.Filesystem.Helpers;
using Microsoft.Extensions.Logging.Abstractions;

namespace Lamina.Storage.Filesystem.Tests;

public class XattrStrictReadTests
{
    [Fact]
    public void StrictRead_DistinguishesIoFailureFromMissingAttribute()
    {
        if (!OperatingSystem.IsLinux()) return;
        var helper = new XattrHelper("user.lamina-test", NullLogger<XattrHelper>.Instance);
        var file = Path.GetTempFileName();
        try
        {
            Assert.Null(helper.GetAttribute(file, "absent", throwOnError: true));
            Assert.Throws<IOException>(() => helper.GetAttribute(Path.Combine(Path.GetTempPath(), new string('a', 300)),
                "etag", throwOnError: true));
        }
        finally { File.Delete(file); }
    }
}
