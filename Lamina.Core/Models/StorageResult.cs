namespace Lamina.Core.Models;

/// <summary>
/// Represents the result of a storage operation that may succeed or fail with validation errors.
/// This avoids using exceptions for validation failures.
/// </summary>
/// <typeparam name="T">The type of the successful result value</typeparam>
public class StorageResult<T>
{
    /// <summary>
    /// The successful result value, if the operation succeeded.
    /// </summary>
    public T? Value { get; init; }

    /// <summary>
    /// The error code if the operation failed (e.g., "InvalidArgument").
    /// </summary>
    public string? ErrorCode { get; init; }

    /// <summary>
    /// The error message if the operation failed.
    /// </summary>
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// Indicates whether the operation was successful.
    /// </summary>
    public bool IsSuccess => ErrorCode == null;

    /// <summary>
    /// Creates a successful result with the given value.
    /// </summary>
    public static StorageResult<T> Success(T value) => new() { Value = value };

    /// <summary>
    /// Creates a failed result with the given error code and message.
    /// </summary>
    public static StorageResult<T> Error(string code, string message) =>
        new() { ErrorCode = code, ErrorMessage = message };
}