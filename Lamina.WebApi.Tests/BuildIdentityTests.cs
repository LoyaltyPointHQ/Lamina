using System.Reflection;

namespace Lamina.WebApi.Tests;

public class BuildIdentityTests
{
    [Fact]
    public void Build_EmbedsRevisionInAssemblyMetadataAndInformationalVersion()
    {
        var assembly = typeof(global::Program).Assembly;
        var revision = Assert.Single(assembly.GetCustomAttributes<AssemblyMetadataAttribute>(), a => a.Key == "GitCommit").Value;
        Assert.False(string.IsNullOrWhiteSpace(revision));
        var version = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()!.InformationalVersion;
        if (revision != "unknown") Assert.Contains(revision!, version);
    }
}
