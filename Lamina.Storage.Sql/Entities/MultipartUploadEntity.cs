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

    [MaxLength(20)]
    public string? ChecksumAlgorithm { get; set; }

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

    [Column(TypeName = "TEXT")]
    public string? PartsMetadataJson { get; set; }

    [NotMapped]
    public Dictionary<int, PartMetadata> Parts
    {
        get => string.IsNullOrEmpty(PartsMetadataJson)
            ? new Dictionary<int, PartMetadata>()
            : JsonSerializer.Deserialize<Dictionary<int, PartMetadata>>(PartsMetadataJson) ?? new Dictionary<int, PartMetadata>();
        set => PartsMetadataJson = value.Count > 0 ? JsonSerializer.Serialize(value) : null;
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
            Metadata = upload.Metadata,
            ChecksumAlgorithm = upload.ChecksumAlgorithm,
            Parts = upload.Parts
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
            Metadata = Metadata,
            ChecksumAlgorithm = ChecksumAlgorithm,
            Parts = Parts
        };
    }

    public void UpdateFromMultipartUpload(MultipartUpload upload)
    {
        // Update mutable fields
        ContentType = upload.ContentType;
        Metadata = upload.Metadata;
        ChecksumAlgorithm = upload.ChecksumAlgorithm;
        Parts = upload.Parts;
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

    [MaxLength(64)]
    public string? ChecksumCRC32 { get; set; }

    [MaxLength(64)]
    public string? ChecksumCRC32C { get; set; }

    [MaxLength(64)]
    public string? ChecksumCRC64NVME { get; set; }

    [MaxLength(64)]
    public string? ChecksumSHA1 { get; set; }

    [MaxLength(64)]
    public string? ChecksumSHA256 { get; set; }

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
            LastModified = part.LastModified,
            ChecksumCRC32 = part.ChecksumCRC32,
            ChecksumCRC32C = part.ChecksumCRC32C,
            ChecksumCRC64NVME = part.ChecksumCRC64NVME,
            ChecksumSHA1 = part.ChecksumSHA1,
            ChecksumSHA256 = part.ChecksumSHA256
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
            Data = Array.Empty<byte>(), // SQL storage doesn't store part data directly
            ChecksumCRC32 = ChecksumCRC32,
            ChecksumCRC32C = ChecksumCRC32C,
            ChecksumCRC64NVME = ChecksumCRC64NVME,
            ChecksumSHA1 = ChecksumSHA1,
            ChecksumSHA256 = ChecksumSHA256
        };
    }
}