namespace Lamina.Core.Models;

/// <summary>
/// Generic result class for XML deserialization operations.
/// </summary>
/// <typeparam name="T">The type of the deserialized object</typeparam>
public class XmlDeserializationResult<T>
{
    public T? Value { get; set; }
    public bool IsSuccess { get; set; }
    public string? ErrorMessage { get; set; }

    public static XmlDeserializationResult<T> Success(T value) =>
        new() { Value = value, IsSuccess = true };

    public static XmlDeserializationResult<T> Error(string message) =>
        new() { IsSuccess = false, ErrorMessage = message };
}