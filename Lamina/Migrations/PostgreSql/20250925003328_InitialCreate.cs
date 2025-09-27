using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace Lamina.Migrations.PostgreSql
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Buckets",
                columns: table => new
                {
                    Name = table.Column<string>(type: "character varying(63)", maxLength: 63, nullable: false),
                    CreationDate = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    Type = table.Column<int>(type: "integer", nullable: false),
                    StorageClass = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    TagsJson = table.Column<string>(type: "jsonb", nullable: false, defaultValue: "{}")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Buckets", x => x.Name);
                });

            migrationBuilder.CreateTable(
                name: "MultipartUploads",
                columns: table => new
                {
                    UploadId = table.Column<string>(type: "character varying(36)", maxLength: 36, nullable: false),
                    BucketName = table.Column<string>(type: "character varying(63)", maxLength: 63, nullable: false),
                    Key = table.Column<string>(type: "character varying(1024)", maxLength: 1024, nullable: false),
                    Initiated = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ContentType = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: true),
                    MetadataJson = table.Column<string>(type: "jsonb", nullable: false, defaultValue: "{}")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_MultipartUploads", x => x.UploadId);
                });

            migrationBuilder.CreateTable(
                name: "Objects",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    BucketName = table.Column<string>(type: "character varying(63)", maxLength: 63, nullable: false),
                    Key = table.Column<string>(type: "character varying(1024)", maxLength: 1024, nullable: false),
                    Size = table.Column<long>(type: "bigint", nullable: false),
                    LastModified = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ETag = table.Column<string>(type: "character varying(34)", maxLength: 34, nullable: false),
                    ContentType = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: false),
                    MetadataJson = table.Column<string>(type: "jsonb", nullable: false, defaultValue: "{}")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Objects", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "UploadParts",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    UploadId = table.Column<string>(type: "character varying(36)", maxLength: 36, nullable: false),
                    PartNumber = table.Column<int>(type: "integer", nullable: false),
                    ETag = table.Column<string>(type: "character varying(34)", maxLength: 34, nullable: false),
                    Size = table.Column<long>(type: "bigint", nullable: false),
                    LastModified = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_UploadParts", x => x.Id);
                    table.ForeignKey(
                        name: "FK_UploadParts_MultipartUploads_UploadId",
                        column: x => x.UploadId,
                        principalTable: "MultipartUploads",
                        principalColumn: "UploadId",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_Buckets_CreationDate",
                table: "Buckets",
                column: "CreationDate");

            migrationBuilder.CreateIndex(
                name: "IX_Buckets_Type",
                table: "Buckets",
                column: "Type");

            migrationBuilder.CreateIndex(
                name: "IX_MultipartUploads_BucketName",
                table: "MultipartUploads",
                column: "BucketName");

            migrationBuilder.CreateIndex(
                name: "IX_MultipartUploads_BucketName_Key",
                table: "MultipartUploads",
                columns: new[] { "BucketName", "Key" });

            migrationBuilder.CreateIndex(
                name: "IX_MultipartUploads_Initiated",
                table: "MultipartUploads",
                column: "Initiated");

            migrationBuilder.CreateIndex(
                name: "IX_Objects_BucketName",
                table: "Objects",
                column: "BucketName");

            migrationBuilder.CreateIndex(
                name: "IX_Objects_BucketName_Key",
                table: "Objects",
                columns: new[] { "BucketName", "Key" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Objects_BucketName_Key_LastModified",
                table: "Objects",
                columns: new[] { "BucketName", "Key", "LastModified" });

            migrationBuilder.CreateIndex(
                name: "IX_Objects_LastModified",
                table: "Objects",
                column: "LastModified");

            migrationBuilder.CreateIndex(
                name: "IX_UploadParts_UploadId_PartNumber",
                table: "UploadParts",
                columns: new[] { "UploadId", "PartNumber" },
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Buckets");

            migrationBuilder.DropTable(
                name: "Objects");

            migrationBuilder.DropTable(
                name: "UploadParts");

            migrationBuilder.DropTable(
                name: "MultipartUploads");
        }
    }
}