namespace Lamina.Storage.Core.Abstract;

/// <summary>
/// Optional backend visibility rules, applied to auxiliary listing sources as well as data.
/// Filesystem-reserved names must not leak back into listings through multipart prefixes.
/// </summary>
public interface IObjectListingFilter
{
    bool IsVisibleInListing(string key);
}
