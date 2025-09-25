using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json;
using Lamina.Models;

namespace Lamina.Storage.Sql.Entities;

[Table("Objects")]
public class ObjectEntity
{
    [Key]
    public int Id { get; set; }

    [Required]
    [MaxLength(63)]
    public string BucketName { get; set; } = string.Empty;

    [Required]
    [MaxLength(1024)]
    public string Key { get; set; } = string.Empty;

    [Required]
    public long Size { get; set; }

    [Required]
    public DateTime LastModified { get; set; }

    [Required]
    [MaxLength(34)]
    public string ETag { get; set; } = string.Empty;

    [Required]
    [MaxLength(256)]
    public string ContentType { get; set; } = "application/octet-stream";

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

    public static ObjectEntity FromS3Object(S3Object s3Object)
    {
        return new ObjectEntity
        {
            BucketName = s3Object.BucketName,
            Key = s3Object.Key,
            Size = s3Object.Size,
            LastModified = s3Object.LastModified,
            ETag = s3Object.ETag,
            ContentType = s3Object.ContentType,
            Metadata = s3Object.Metadata
        };
    }

    public static ObjectEntity FromS3ObjectInfo(string bucketName, S3ObjectInfo objectInfo)
    {
        return new ObjectEntity
        {
            BucketName = bucketName,
            Key = objectInfo.Key,
            Size = objectInfo.Size,
            LastModified = objectInfo.LastModified,
            ETag = objectInfo.ETag,
            ContentType = objectInfo.ContentType,
            Metadata = objectInfo.Metadata
        };
    }

    public S3ObjectInfo ToS3ObjectInfo()
    {
        return new S3ObjectInfo
        {
            Key = Key,
            Size = Size,
            LastModified = LastModified,
            ETag = ETag,
            ContentType = ContentType,
            Metadata = Metadata
        };
    }
}