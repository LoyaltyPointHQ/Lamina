using Lamina.Core.Models;
using Lamina.Core.Streaming;

namespace Lamina.WebApi.Streaming.Validation;

public class AnonymousChunkValidator : IChunkSignatureValidator
{
    private int _chunkIndex;

    public AnonymousChunkValidator(long expectedDecodedLength, List<string> expectedTrailerNames)
    {
        ExpectedDecodedLength = expectedDecodedLength;
        ExpectedTrailerNames = expectedTrailerNames;
    }

    public long ExpectedDecodedLength { get; }
    public int ChunkIndex => _chunkIndex;
    public bool ExpectsTrailers => true;
    public List<string> ExpectedTrailerNames { get; }

    public bool ValidateChunk(ReadOnlyMemory<byte> chunkData, string chunkSignature, bool isLastChunk)
    {
        _chunkIndex++;
        return true;
    }

    public async Task<bool> ValidateChunkStreamAsync(Stream chunkStream, long chunkSize, string chunkSignature, bool isLastChunk)
    {
        _chunkIndex++;
        await Task.CompletedTask;
        return true;
    }

    public TrailerValidationResult ValidateTrailer(List<StreamingTrailer> trailers, string trailerSignature)
    {
        return new TrailerValidationResult
        {
            IsValid = true,
            Trailers = trailers
        };
    }
}
