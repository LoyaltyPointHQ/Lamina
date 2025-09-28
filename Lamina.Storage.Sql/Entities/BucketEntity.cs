using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json;
using Lamina.Core.Models;

namespace Lamina.Storage.Sql.Entities;

[Table("Buckets")]
public class BucketEntity
{
    [Key]
    [MaxLength(63)]
    public string Name { get; set; } = string.Empty;

    [Required]
    public DateTime CreationDate { get; set; }

    [Required]
    public int Type { get; set; } = (int)BucketType.Directory;

    [MaxLength(50)]
    public string? StorageClass { get; set; }

    [Column(TypeName = "TEXT")]
    public string TagsJson { get; set; } = "{}";

    [MaxLength(256)]
    public string? OwnerId { get; set; }

    [MaxLength(256)]
    public string? OwnerDisplayName { get; set; }

    [NotMapped]
    public Dictionary<string, string> Tags
    {
        get => string.IsNullOrEmpty(TagsJson)
            ? new Dictionary<string, string>()
            : JsonSerializer.Deserialize<Dictionary<string, string>>(TagsJson) ?? new Dictionary<string, string>();
        set => TagsJson = JsonSerializer.Serialize(value);
    }

    [NotMapped]
    public BucketType BucketType
    {
        get => (BucketType)Type;
        set => Type = (int)value;
    }

    public static BucketEntity FromBucket(Bucket bucket)
    {
        return new BucketEntity
        {
            Name = bucket.Name,
            CreationDate = bucket.CreationDate,
            BucketType = bucket.Type,
            StorageClass = bucket.StorageClass,
            Tags = bucket.Tags,
            OwnerId = bucket.OwnerId,
            OwnerDisplayName = bucket.OwnerDisplayName
        };
    }

    public Bucket ToBucket()
    {
        return new Bucket
        {
            Name = Name,
            CreationDate = CreationDate,
            Type = BucketType,
            StorageClass = StorageClass,
            Tags = Tags,
            OwnerId = OwnerId,
            OwnerDisplayName = OwnerDisplayName
        };
    }
}