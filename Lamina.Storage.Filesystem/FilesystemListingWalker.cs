using System.Buffers;
using System.IO.Enumeration;
using Lamina.Storage.Core.Listing;

namespace Lamina.Storage.Filesystem;

/// <summary>Depth-first enumeration with O(depth) handles and a reusable key buffer.</summary>
internal sealed class FilesystemListingWalker : IDisposable
{
    private readonly string _bucketPath;
    private readonly string _metadataDirectoryName;
    private readonly string _tempPrefix;
    private readonly ListingQuery _query;
    private readonly ListingPageSelector _selector;
    private readonly CancellationToken _cancellationToken;
    private readonly ArrayPool<char> _pool;
    private readonly bool _detectSymlinks;
    private char[] _keyBuffer;

    public FilesystemListingWalker(string bucketPath, string metadataDirectoryName, string tempPrefix,
        ListingQuery query, ListingPageSelector selector, CancellationToken cancellationToken, ArrayPool<char>? pool = null, bool detectSymlinks = false)
    {
        _bucketPath = Path.GetFullPath(bucketPath);
        _metadataDirectoryName = metadataDirectoryName;
        _tempPrefix = tempPrefix;
        _query = query;
        _selector = selector;
        _cancellationToken = cancellationToken;
        _pool = pool ?? ArrayPool<char>.Shared;
        _detectSymlinks = detectSymlinks;
        _keyBuffer = _pool.Rent(256);
    }

    public bool HasSymlinks { get; private set; }

    public void Walk()
    {
        _cancellationToken.ThrowIfCancellationRequested();
        if (_query.CandidateLimit == 0)
            return;

        var slash = _query.Prefix.LastIndexOf('/');
        var relativeStart = slash < 0 ? string.Empty : _query.Prefix[..(slash + 1)];
        // The last unfinished component is a StartsWith filter, not a directory name:
        // prefix=.lamina-meta must still match .lamina-meta-backup/visible.
        if (FilesystemStorageHelper.HasInternalListingSegment(relativeStart, _metadataDirectoryName, _tempPrefix))
            return;
        var start = Path.GetFullPath(Path.Combine(_bucketPath, relativeStart.Replace('/', Path.DirectorySeparatorChar)));
        var pathComparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        if (!start.Equals(_bucketPath, pathComparison)
            && !start.StartsWith(Path.TrimEndingDirectorySeparator(_bucketPath) + Path.DirectorySeparatorChar, pathComparison))
            throw new InvalidOperationException("Invalid prefix to bucket");

        // ResolveLinkTarget on the final directory does not reveal links in its
        // ancestors. A prefix may start below an aliased subtree.
        var ancestorPath = _bucketPath;
        foreach (var component in _detectSymlinks ? relativeStart.Split('/', StringSplitOptions.RemoveEmptyEntries) : [])
        {
            ancestorPath = Path.Combine(ancestorPath, component);
            HasSymlinks |= new DirectoryInfo(ancestorPath).LinkTarget != null;
        }

        var stack = new Stack<(EntryEnumerator Enumerator, string PhysicalPath)>();
        var ancestors = new HashSet<string>(OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
        try
        {
            Push(start, start);
            while (stack.TryPeek(out var frame))
            {
                _cancellationToken.ThrowIfCancellationRequested();
                bool moved;
                try
                {
                    moved = frame.Enumerator.MoveNext();
                }
                catch (DirectoryNotFoundException)
                {
                    // A directory may be removed while listing it.
                    moved = false;
                }
                if (!moved)
                {
                    stack.Pop().Enumerator.Dispose();
                    ancestors.Remove(frame.PhysicalPath);
                    continue;
                }

                if (frame.Enumerator.Current is { } child)
                    Push(child, Path.Combine(frame.PhysicalPath, Path.GetFileName(child)));
            }
        }
        finally
        {
            while (stack.TryPop(out var frame))
                frame.Enumerator.Dispose();
        }

        void Push(string logicalPath, string physicalPath)
        {
            try
            {
                var info = new DirectoryInfo(physicalPath);
                var target = info.ResolveLinkTarget(true);
                HasSymlinks |= target != null;
                var resolved = Path.TrimEndingDirectorySeparator(target?.FullName ?? info.FullName);
                if (!ancestors.Add(resolved))
                    throw new IOException("A directory symlink cycle was encountered while listing objects.");
                try
                {
                    var relative = Path.TrimEndingDirectorySeparator(Path.GetRelativePath(_bucketPath, logicalPath));
                    var keyPrefix = relative == "." ? string.Empty : relative.Replace(Path.DirectorySeparatorChar, '/') + "/";
                    stack.Push((new EntryEnumerator(logicalPath, keyPrefix, this), resolved));
                }
                catch
                {
                    ancestors.Remove(resolved);
                    throw;
                }
            }
            catch (Exception ex) when (ex is DirectoryNotFoundException or FileNotFoundException)
            {
                // Missing initial prefix, or a child removed since its parent was enumerated.
            }
        }
    }

    private void Consider(string directoryPrefix, ReadOnlySpan<char> fileName, bool directory)
    {
        var length = directoryPrefix.Length + fileName.Length + (directory ? 1 : 0);
        if (_keyBuffer.Length < length)
        {
            var next = _pool.Rent(length);
            var previous = _keyBuffer;
            _keyBuffer = next;
            _pool.Return(previous);
        }
        directoryPrefix.AsSpan().CopyTo(_keyBuffer);
        fileName.CopyTo(_keyBuffer.AsSpan(directoryPrefix.Length));
        if (directory)
            _keyBuffer[length - 1] = '/';
        _selector.ConsiderKey(_keyBuffer.AsSpan(0, length));
    }

    public void Dispose() => _pool.Return(_keyBuffer);

    private sealed class EntryEnumerator : FileSystemEnumerator<string?>
    {
        private readonly string _keyPrefix;
        private readonly FilesystemListingWalker _owner;

        public EntryEnumerator(string path, string keyPrefix, FilesystemListingWalker owner)
            : base(path, new EnumerationOptions
            {
                RecurseSubdirectories = false,
                AttributesToSkip = 0,
                IgnoreInaccessible = false,
                ReturnSpecialDirectories = false
            })
        {
            _keyPrefix = keyPrefix;
            _owner = owner;
        }

        protected override bool ShouldIncludeEntry(ref FileSystemEntry entry)
        {
            _owner._cancellationToken.ThrowIfCancellationRequested();
            _owner._selector.Statistics.ScannedEntries++;
            // Attributes stat the entry on Unix. File symlinks do not alias listing
            // subtrees, so only inspect directories (already resolved when traversed).
            _owner.HasSymlinks |= _owner._detectSymlinks && entry.IsDirectory && (entry.Attributes & FileAttributes.ReparsePoint) != 0;
            if (!FilesystemStorageHelper.IsInternalListingSegment(entry.FileName, _owner._metadataDirectoryName, _owner._tempPrefix))
                return true;
            _owner._selector.Statistics.ExcludedEntries++;
            if (entry.IsDirectory)
                _owner._selector.Statistics.ExcludedSubtrees++;
            return false;
        }

        protected override string? TransformEntry(ref FileSystemEntry entry)
        {
            if (!entry.IsDirectory)
                _owner.Consider(_keyPrefix, entry.FileName, false);
            else if (_owner._query.Delimiter == "/")
                _owner.Consider(_keyPrefix, entry.FileName, true);
            else
                return entry.ToFullPath(); // Allocate only for directories we actually descend into.
            return null;
        }
    }
}
