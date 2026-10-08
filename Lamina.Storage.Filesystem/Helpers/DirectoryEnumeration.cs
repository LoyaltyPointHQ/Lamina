namespace Lamina.Storage.Filesystem.Helpers;

/// <summary>Shallow enumeration that tolerates directories removed by concurrent operations.</summary>
public static class DirectoryEnumeration
{
    public static IEnumerable<string> Files(string directory, string pattern) =>
        Enumerate(() => Directory.EnumerateFiles(directory, pattern));

    public static IEnumerable<string> Directories(string directory) =>
        Enumerate(() => Directory.EnumerateDirectories(directory));

    private static IEnumerable<string> Enumerate(Func<IEnumerable<string>> entries)
    {
        IEnumerator<string> iterator;
        try { iterator = entries().GetEnumerator(); }
        catch (DirectoryNotFoundException) { yield break; }

        using (iterator)
        {
            while (true)
            {
                bool hasNext;
                try { hasNext = iterator.MoveNext(); }
                catch (DirectoryNotFoundException) { yield break; }
                if (!hasNext) yield break;
                yield return iterator.Current;
            }
        }
    }
}
