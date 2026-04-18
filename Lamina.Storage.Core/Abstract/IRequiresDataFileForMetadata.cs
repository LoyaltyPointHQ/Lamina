namespace Lamina.Storage.Core.Abstract;

/// <summary>
/// Marker: the metadata storage physically binds metadata to the data file (e.g. POSIX xattr
/// on the data file itself) and therefore cannot persist metadata before the data exists.
/// Callers that otherwise prefer metadata-first ordering (so a GET during the window sees
/// 404 instead of auto-generated metadata) must fall back to data-first for these backends.
/// </summary>
public interface IRequiresDataFileForMetadata
{
}
