using System.Security.Cryptography;
using System.Text;

namespace Lamina.Streaming.Validation
{
    /// <summary>
    /// Utility class for calculating AWS streaming signatures and hashes
    /// </summary>
    public static class SignatureCalculator
    {
        /// <summary>
        /// Calculates chunk signature for AWS streaming requests
        /// </summary>
        public static string CalculateChunkSignature(
            byte[] signingKey,
            DateTime requestDateTime,
            string region,
            string previousSignature,
            ReadOnlyMemory<byte> chunkData,
            bool isLastChunk)
        {
            var dateStamp = requestDateTime.ToString("yyyyMMdd");
            var amzDate = requestDateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            var algorithm = "AWS4-HMAC-SHA256-PAYLOAD";
            var credentialScope = $"{dateStamp}/{region}/s3/aws4_request";
            var emptyStringHash = GetHash(Array.Empty<byte>());
            var chunkSize = isLastChunk ? "0" : chunkData.Length.ToString("x");
            var chunkBytes = chunkData.ToArray();
            var chunkHash = GetHash(chunkBytes);

            var stringToSign = $"{algorithm}\n{amzDate}\n{credentialScope}\n{previousSignature}\n{emptyStringHash}\n{chunkHash}";

            return GetHmacSha256Hex(signingKey, stringToSign);
        }

        /// <summary>
        /// Calculates chunk signature for AWS streaming requests (stream-based)
        /// </summary>
        public static async Task<string> CalculateChunkSignatureStreamAsync(
            byte[] signingKey,
            DateTime requestDateTime,
            string region,
            string previousSignature,
            Stream chunkStream,
            long chunkSize,
            bool isLastChunk)
        {
            var dateStamp = requestDateTime.ToString("yyyyMMdd");
            var amzDate = requestDateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            var algorithm = "AWS4-HMAC-SHA256-PAYLOAD";
            var credentialScope = $"{dateStamp}/{region}/s3/aws4_request";
            var emptyStringHash = GetHash(Array.Empty<byte>());
            var chunkSizeHex = isLastChunk ? "0" : chunkSize.ToString("x");
            var chunkHash = await GetHashFromStreamAsync(chunkStream);

            var stringToSign = $"{algorithm}\n{amzDate}\n{credentialScope}\n{previousSignature}\n{emptyStringHash}\n{chunkHash}";

            return GetHmacSha256Hex(signingKey, stringToSign);
        }

        /// <summary>
        /// Calculates trailer signature for AWS streaming requests with trailers
        /// </summary>
        public static string CalculateTrailerSignature(
            byte[] signingKey,
            DateTime requestDateTime,
            string region,
            string previousSignature,
            string trailerHeaderString)
        {
            var dateStamp = requestDateTime.ToString("yyyyMMdd");
            var amzDate = requestDateTime.ToString("yyyyMMdd'T'HHmmss'Z'");

            var algorithm = "AWS4-HMAC-SHA256-TRAILER";
            var credentialScope = $"{dateStamp}/{region}/s3/aws4_request";
            var trailerHash = GetHash(Encoding.UTF8.GetBytes(trailerHeaderString));

            var stringToSign = $"{algorithm}\n{amzDate}\n{credentialScope}\n{previousSignature}\n{trailerHash}";

            return GetHmacSha256Hex(signingKey, stringToSign);
        }

        /// <summary>
        /// Computes SHA256 hash of byte array
        /// </summary>
        public static string GetHash(byte[] data)
        {
            using var sha256 = SHA256.Create();
            var hash = sha256.ComputeHash(data);
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }

        /// <summary>
        /// Computes SHA256 hash from stream (async)
        /// </summary>
        public static async Task<string> GetHashFromStreamAsync(Stream stream)
        {
            using var sha256 = SHA256.Create();
            var originalPosition = stream.Position;
            stream.Position = 0;
            var hash = await sha256.ComputeHashAsync(stream);
            stream.Position = originalPosition;
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }

        /// <summary>
        /// Computes HMAC-SHA256 and returns as hex string
        /// </summary>
        public static string GetHmacSha256Hex(byte[] key, string data)
        {
            using var hmac = new HMACSHA256(key);
            var hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(data));
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }

        /// <summary>
        /// Builds trailer header string for signature calculation
        /// </summary>
        public static string BuildTrailerHeaderString(List<Models.StreamingTrailer> trailers)
        {
            var sortedTrailers = trailers.OrderBy(t => t.Name.ToLower()).ToList();
            var builder = new StringBuilder();

            foreach (var trailer in sortedTrailers)
            {
                builder.Append($"{trailer.Name.ToLower()}:{trailer.Value}\n");
            }

            return builder.ToString();
        }
    }
}