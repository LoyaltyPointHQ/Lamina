using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Listing;

namespace Lamina.Storage.Core.Tests;

internal static class ListingTestData
{
    public static ListingCandidates Candidates(ListDataResult page) => new(
        page.Keys.Select(k => new ListingEntry(k, false))
            .Concat(page.CommonPrefixes.Select(p => new ListingEntry(p, true))).ToArray(), new ListingStatistics());

    public static async IAsyncEnumerable<string> UploadKeys(IEnumerable<MultipartUpload> uploads)
    {
        await Task.CompletedTask;
        foreach (var upload in uploads)
            yield return upload.Key;
    }
}
