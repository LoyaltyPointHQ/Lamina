namespace Lamina.Core.Models;

public class DeleteMultipleObjectsResponse
{
    public List<DeletedObjectResult> Deleted { get; set; } = new();
    public List<DeleteErrorResult> Errors { get; set; } = new();
    
    public DeleteMultipleObjectsResponse() { }

    public DeleteMultipleObjectsResponse(List<DeletedObjectResult> deleted, List<DeleteErrorResult> errors)
    {
        Deleted = deleted;
        Errors = errors;
    }
}

public class DeletedObjectResult
{
    public string Key { get; set; } = string.Empty;
    public string? VersionId { get; set; }
    public bool? DeleteMarker { get; set; }
    public string? DeleteMarkerVersionId { get; set; }

    public DeletedObjectResult() { }
    
    public DeletedObjectResult(string key, string? versionId = null)
    {
        Key = key;
        VersionId = versionId;
    }
}

public class DeleteErrorResult
{
    public string Key { get; set; } = string.Empty;
    public string Code { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public string? VersionId { get; set; }

    public DeleteErrorResult() { }
    
    public DeleteErrorResult(string key, string code, string message, string? versionId = null)
    {
        Key = key;
        Code = code;
        Message = message;
        VersionId = versionId;
    }
}