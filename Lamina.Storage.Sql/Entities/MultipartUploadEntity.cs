using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json;
using Lamina.Core.Models;

namespace Lamina.Storage.Sql.Entities;

[Table("MultipartUploads")]
public class MultipartUploadEntity
{
    [Key]
    [MaxLength(36)]
    public string UploadId { get; set; } = string.Empty;

    [Required]
    [MaxLength(63)]
    public string BucketName { get; set; } = string.Empty;

    [Required]
    [MaxLength(1024)]
    public string Key { get; set; } = string.Empty;

    [Required]
    public DateTime Initiated { get; set; }

    [MaxLength(256)]
    public string? ContentType { get; set; }

    [Column(TypeName = "TEXT")]
    public string MetadataJson { get; set; } = "{}";

    [NotMapped]
    public Dictionary<string, string> Metadata
    {
        get => string.IsNullOrEmpty(MetadataJson)
            ? new Dictionary<string, string>()
            : JsonSerializer.Deserialize<Dictionary<string, string>>(MetadataJson) ?? new Dictionary<string, string>();
        set => MetadataJson = JsonSerializer.Serialize(value);
    }

    public static MultipartUploadEntity FromMultipartUpload(MultipartUpload upload)
    {
        return new MultipartUploadEntity
        {
            UploadId = upload.UploadId,
            BucketName = upload.BucketName,
            Key = upload.Key,
            Initiated = upload.Initiated,
            ContentType = upload.ContentType,
            Metadata = upload.Metadata
        };
    }

    public MultipartUpload ToMultipartUpload()
    {
        return new MultipartUpload
        {
            UploadId = UploadId,
            BucketName = BucketName,
            Key = Key,
            Initiated = Initiated,
            ContentType = ContentType,
            Metadata = Metadata
        };
    }
}

[Table("UploadParts")]
public class UploadPartEntity
{
    [Key]
    public int Id { get; set; }

    [Required]
    [MaxLength(36)]
    public string UploadId { get; set; } = string.Empty;

    [Required]
    public int PartNumber { get; set; }

    [Required]
    [MaxLength(34)]
    public string ETag { get; set; } = string.Empty;

    [Required]
    public long Size { get; set; }

    [Required]
    public DateTime LastModified { get; set; }

    [ForeignKey(nameof(UploadId))]
    public virtual MultipartUploadEntity? Upload { get; set; }

    public static UploadPartEntity FromUploadPart(string uploadId, UploadPart part)
    {
        return new UploadPartEntity
        {
            UploadId = uploadId,
            PartNumber = part.PartNumber,
            ETag = part.ETag,
            Size = part.Size,
            LastModified = part.LastModified
        };
    }

    public UploadPart ToUploadPart()
    {
        return new UploadPart
        {
            PartNumber = PartNumber,
            ETag = ETag,
            Size = Size,
            LastModified = LastModified,
            Data = Array.Empty<byte>() // SQL storage doesn't store part data directly
        };
    }
}