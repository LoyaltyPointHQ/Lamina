namespace Lamina.Storage.Core.Helpers;

public static class TagValidator
{
    public const int MaxTags = 10;
    public const int MaxKeyLength = 128;
    public const int MaxValueLength = 256;

    public static TagValidationResult Validate(Dictionary<string, string>? tags)
    {
        if (tags == null || tags.Count == 0)
        {
            return TagValidationResult.Valid();
        }

        if (tags.Count > MaxTags)
        {
            return TagValidationResult.Invalid(
                $"Object tags cannot exceed {MaxTags} tags per object.");
        }

        foreach (var (key, value) in tags)
        {
            if (string.IsNullOrEmpty(key))
            {
                return TagValidationResult.Invalid("Tag key cannot be empty.");
            }

            if (key.Length > MaxKeyLength)
            {
                return TagValidationResult.Invalid(
                    $"Tag key exceeds maximum length of {MaxKeyLength} characters.");
            }

            if (value != null && value.Length > MaxValueLength)
            {
                return TagValidationResult.Invalid(
                    $"Tag value exceeds maximum length of {MaxValueLength} characters.");
            }
        }

        return TagValidationResult.Valid();
    }
}

public readonly record struct TagValidationResult(bool IsValid, string? ErrorMessage)
{
    public static TagValidationResult Valid() => new(true, null);
    public static TagValidationResult Invalid(string message) => new(false, message);
}
