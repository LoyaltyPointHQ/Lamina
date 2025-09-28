using Lamina.Core.Models;
using Xunit;

namespace Lamina.Tests.Streaming.Trailers
{
    public class StreamingTrailerModelTests
    {
        [Fact]
        public void StreamingTrailer_CanBeCreated()
        {
            // Arrange & Act
            var trailer = new StreamingTrailer
            {
                Name = "x-amz-checksum-crc32c",
                Value = "wdBDMA=="
            };

            // Assert
            Assert.Equal("x-amz-checksum-crc32c", trailer.Name);
            Assert.Equal("wdBDMA==", trailer.Value);
        }

        [Fact]
        public void TrailerValidationResult_DefaultsCorrectly()
        {
            // Arrange & Act
            var result = new TrailerValidationResult();

            // Assert
            Assert.False(result.IsValid);
            Assert.Empty(result.Trailers);
            Assert.Null(result.ErrorMessage);
        }

        [Fact]
        public void TrailerValidationResult_CanSetProperties()
        {
            // Arrange
            var trailers = new List<StreamingTrailer>
            {
                new() { Name = "x-amz-checksum-crc32c", Value = "wdBDMA==" }
            };

            // Act
            var result = new TrailerValidationResult
            {
                IsValid = true,
                Trailers = trailers,
                ErrorMessage = "Test error"
            };

            // Assert
            Assert.True(result.IsValid);
            Assert.Single(result.Trailers);
            Assert.Equal("x-amz-checksum-crc32c", result.Trailers[0].Name);
            Assert.Equal("Test error", result.ErrorMessage);
        }

        [Fact]
        public void ChunkedDataResult_DefaultsCorrectly()
        {
            // Arrange & Act
            var result = new ChunkedDataResult();

            // Assert
            Assert.Empty(result.Trailers);
            Assert.Null(result.TrailerValidationResult);
            Assert.Equal(0, result.TotalBytesWritten);
            Assert.Null(result.ErrorMessage);
        }

        [Fact]
        public void ChunkedDataResult_CanSetProperties()
        {
            // Arrange
            var trailers = new List<StreamingTrailer>
            {
                new() { Name = "x-amz-checksum-sha256", Value = "abc123" }
            };

            // Act
            var result = new ChunkedDataResult
            {
                Trailers = trailers,
                TrailerValidationResult = true,
                TotalBytesWritten = 1024,
                ErrorMessage = "Test error"
            };

            // Assert
            Assert.Single(result.Trailers);
            Assert.Equal("x-amz-checksum-sha256", result.Trailers[0].Name);
            Assert.True(result.TrailerValidationResult);
            Assert.Equal(1024, result.TotalBytesWritten);
            Assert.Equal("Test error", result.ErrorMessage);
        }

        [Theory]
        [InlineData("x-amz-checksum-crc32c", "wdBDMA==")]
        [InlineData("x-amz-checksum-sha256", "abc123def456")]
        [InlineData("x-amz-checksum-md5", "098f6bcd4621d373cade4e832627b4f6")]
        [InlineData("custom-header", "custom-value")]
        public void StreamingTrailer_HandlesVariousHeaderTypes(string name, string value)
        {
            // Arrange & Act
            var trailer = new StreamingTrailer
            {
                Name = name,
                Value = value
            };

            // Assert
            Assert.Equal(name, trailer.Name);
            Assert.Equal(value, trailer.Value);
        }

        [Fact]
        public void StreamingTrailer_RequiredPropertiesEnforced()
        {
            // This test verifies that the required properties are properly defined
            // The compiler will enforce this at compile time, but we test the runtime behavior

            // Arrange & Act
            var trailer = new StreamingTrailer
            {
                Name = "test-name",
                Value = "test-value"
            };

            // Assert
            Assert.NotNull(trailer.Name);
            Assert.NotNull(trailer.Value);
        }
    }
}