using Microsoft.EntityFrameworkCore;
using Lamina.Storage.Sql.Entities;

namespace Lamina.Storage.Sql.Context;

public class LaminaDbContext : DbContext
{
    public LaminaDbContext(DbContextOptions<LaminaDbContext> options) : base(options)
    {
    }

    public DbSet<BucketEntity> Buckets { get; set; }
    public DbSet<ObjectEntity> Objects { get; set; }
    public DbSet<MultipartUploadEntity> MultipartUploads { get; set; }
    public DbSet<UploadPartEntity> UploadParts { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // Bucket configuration
        modelBuilder.Entity<BucketEntity>(entity =>
        {
            entity.HasKey(e => e.Name);
            entity.HasIndex(e => e.CreationDate);
            entity.HasIndex(e => e.Type);
        });

        // Object configuration
        modelBuilder.Entity<ObjectEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.HasIndex(e => new { e.BucketName, e.Key }).IsUnique();
            entity.HasIndex(e => e.BucketName);
            entity.HasIndex(e => e.LastModified);
            entity.HasIndex(e => new { e.BucketName, e.Key, e.LastModified });
        });

        // MultipartUpload configuration
        modelBuilder.Entity<MultipartUploadEntity>(entity =>
        {
            entity.HasKey(e => e.UploadId);
            entity.HasIndex(e => e.BucketName);
            entity.HasIndex(e => new { e.BucketName, e.Key });
            entity.HasIndex(e => e.Initiated);
        });

        // UploadPart configuration
        modelBuilder.Entity<UploadPartEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.HasIndex(e => new { e.UploadId, e.PartNumber }).IsUnique();
        });

        // Configure JSON columns for different database providers
        if (Database.ProviderName == "Microsoft.EntityFrameworkCore.Sqlite")
        {
            // SQLite doesn't have native JSON support, use TEXT
            modelBuilder.Entity<BucketEntity>()
                .Property(e => e.TagsJson)
                .HasColumnType("TEXT");

            modelBuilder.Entity<ObjectEntity>()
                .Property(e => e.MetadataJson)
                .HasColumnType("TEXT");

            modelBuilder.Entity<MultipartUploadEntity>()
                .Property(e => e.MetadataJson)
                .HasColumnType("TEXT");
        }
        else if (Database.ProviderName == "Npgsql.EntityFrameworkCore.PostgreSQL")
        {
            // PostgreSQL has native JSON support
            modelBuilder.Entity<BucketEntity>()
                .Property(e => e.TagsJson)
                .HasColumnType("jsonb");

            modelBuilder.Entity<ObjectEntity>()
                .Property(e => e.MetadataJson)
                .HasColumnType("jsonb");

            modelBuilder.Entity<MultipartUploadEntity>()
                .Property(e => e.MetadataJson)
                .HasColumnType("jsonb");
        }
    }
}