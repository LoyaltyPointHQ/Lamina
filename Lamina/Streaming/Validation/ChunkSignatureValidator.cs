using Lamina.Models;
using Microsoft.Extensions.Logging;

namespace Lamina.Streaming.Validation
{
    /// <summary>
    /// Validates chunk signatures for AWS streaming uploads
    /// </summary>
    public class ChunkSignatureValidator : IChunkSignatureValidator
    {
        private readonly byte[] _signingKey;
        private readonly DateTime _requestDateTime;
        private readonly string _region;
        private readonly long _expectedDecodedLength;
        private readonly ILogger _logger;
        private readonly bool _expectsTrailers;
        private readonly List<string> _expectedTrailerNames;
        private int _chunkIndex;
        private string _previousSignature;

        public ChunkSignatureValidator(
            byte[] signingKey,
            DateTime requestDateTime,
            string region,
            long expectedDecodedLength,
            string seedSignature,
            ILogger logger,
            bool expectsTrailers = false,
            List<string>? expectedTrailerNames = null)
        {
            _signingKey = signingKey;
            _requestDateTime = requestDateTime;
            _region = region;
            _expectedDecodedLength = expectedDecodedLength;
            _logger = logger;
            _expectsTrailers = expectsTrailers;
            _expectedTrailerNames = expectedTrailerNames ?? new List<string>();
            _chunkIndex = 0;
            _previousSignature = seedSignature;
        }

        public long ExpectedDecodedLength => _expectedDecodedLength;
        public int ChunkIndex => _chunkIndex;
        public bool ExpectsTrailers => _expectsTrailers;
        public List<string> ExpectedTrailerNames => _expectedTrailerNames;

        // Internal properties for test access
        internal byte[] SigningKey => _signingKey;
        internal DateTime RequestDateTime => _requestDateTime;
        internal string Region => _region;
        internal string PreviousSignature => _previousSignature;

        public async Task<bool> ValidateChunkAsync(ReadOnlyMemory<byte> chunkData, string chunkSignature, bool isLastChunk)
        {
            try
            {
                var expectedSignature = SignatureCalculator.CalculateChunkSignature(
                    _signingKey,
                    _requestDateTime,
                    _region,
                    _previousSignature,
                    chunkData,
                    isLastChunk);

                var isValid = string.Equals(expectedSignature, chunkSignature, StringComparison.OrdinalIgnoreCase);

                if (isValid)
                {
                    _previousSignature = expectedSignature;
                    _chunkIndex++;
                    LogChunkValidationSuccess(chunkData, chunkSignature);
                }
                else
                {
                    LogChunkValidationFailure(expectedSignature, chunkSignature);
                }

                return isValid;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error validating chunk signature");
                return false;
            }
        }

        public async Task<bool> ValidateChunkStreamAsync(Stream chunkStream, long chunkSize, string chunkSignature, bool isLastChunk)
        {
            try
            {
                var expectedSignature = await SignatureCalculator.CalculateChunkSignatureStreamAsync(
                    _signingKey,
                    _requestDateTime,
                    _region,
                    _previousSignature,
                    chunkStream,
                    chunkSize,
                    isLastChunk);

                var isValid = string.Equals(expectedSignature, chunkSignature, StringComparison.OrdinalIgnoreCase);

                if (isValid)
                {
                    _previousSignature = expectedSignature;
                    _chunkIndex++;
                    LogChunkStreamValidationSuccess(chunkSize, chunkSignature, isLastChunk);
                }
                else
                {
                    LogChunkValidationFailure(expectedSignature, chunkSignature);
                }

                return isValid;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error validating chunk signature");
                return false;
            }
        }

        public async Task<TrailerValidationResult> ValidateTrailerAsync(List<StreamingTrailer> trailers, string trailerSignature)
        {
            var result = new TrailerValidationResult();

            try
            {
                if (!_expectsTrailers)
                {
                    result.ErrorMessage = "This validator does not expect trailers";
                    return result;
                }

                var validationError = ValidateExpectedTrailers(trailers);
                if (validationError != null)
                {
                    result.ErrorMessage = validationError;
                    return result;
                }

                var trailerHeaderString = SignatureCalculator.BuildTrailerHeaderString(trailers);
                var expectedSignature = SignatureCalculator.CalculateTrailerSignature(
                    _signingKey,
                    _requestDateTime,
                    _region,
                    _previousSignature,
                    trailerHeaderString);

                result.IsValid = string.Equals(expectedSignature, trailerSignature, StringComparison.OrdinalIgnoreCase);

                if (result.IsValid)
                {
                    result.Trailers = trailers;
                    _logger.LogDebug("Trailer signature validation successful: {TrailerSignature}", trailerSignature);
                }
                else
                {
                    result.ErrorMessage = $"Trailer signature validation failed. Expected: {expectedSignature}, Got: {trailerSignature}";
                    _logger.LogWarning("Trailer signature validation failed. Expected: {Expected}, Got: {Got}", expectedSignature, trailerSignature);
                }

                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error validating trailer signature");
                result.ErrorMessage = $"Error validating trailer: {ex.Message}";
                return result;
            }
        }

        private string? ValidateExpectedTrailers(List<StreamingTrailer> trailers)
        {
            var providedTrailerNames = trailers.Select(t => t.Name.ToLower()).ToHashSet();
            var missingTrailers = _expectedTrailerNames.Except(providedTrailerNames).ToList();

            return missingTrailers.Any()
                ? $"Missing expected trailers: {string.Join(", ", missingTrailers)}"
                : null;
        }

        private void LogChunkValidationSuccess(ReadOnlyMemory<byte> chunkData, string chunkSignature)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                var chunkBytes = chunkData.ToArray();
                var debugBytes = chunkBytes.Length > 10 ? chunkBytes.Take(10).ToArray() : chunkBytes;
                var hexString = BitConverter.ToString(debugBytes).Replace("-", " ");
                var chunkHash = SignatureCalculator.GetHash(chunkBytes);

                _logger.LogDebug("Chunk signature validation - Size: {Size}, Hash: {Hash}, IsLast: {IsLast}, FirstBytes: {Bytes}",
                    chunkData.Length, chunkHash, false, hexString);
            }
        }

        private void LogChunkStreamValidationSuccess(long chunkSize, string chunkSignature, bool isLastChunk)
        {
            _logger.LogDebug("Chunk signature validation (streaming) - Size: {Size}, Signature: {Signature}, IsLast: {IsLast}",
                chunkSize, chunkSignature, isLastChunk);
        }

        private void LogChunkValidationFailure(string expectedSignature, string actualSignature)
        {
            _logger.LogWarning("Chunk signature validation failed at chunk index {Index}. Expected: {Expected}, Got: {Got}. Previous signature: {Previous}",
                _chunkIndex, expectedSignature, actualSignature, _previousSignature);
        }
    }
}