using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Lamina.Storage.Sql.Migrations.Sqlite
{
    /// <inheritdoc />
    public partial class AddChecksumFields : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "ChecksumCRC32",
                table: "UploadParts",
                type: "TEXT",
                maxLength: 64,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ChecksumCRC32C",
                table: "UploadParts",
                type: "TEXT",
                maxLength: 64,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ChecksumCRC64NVME",
                table: "UploadParts",
                type: "TEXT",
                maxLength: 64,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ChecksumSHA1",
                table: "UploadParts",
                type: "TEXT",
                maxLength: 64,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ChecksumSHA256",
                table: "UploadParts",
                type: "TEXT",
                maxLength: 64,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ChecksumCRC32",
                table: "Objects",
                type: "TEXT",
                maxLength: 64,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ChecksumCRC32C",
                table: "Objects",
                type: "TEXT",
                maxLength: 64,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ChecksumCRC64NVME",
                table: "Objects",
                type: "TEXT",
                maxLength: 64,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ChecksumSHA1",
                table: "Objects",
                type: "TEXT",
                maxLength: 64,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ChecksumSHA256",
                table: "Objects",
                type: "TEXT",
                maxLength: 64,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ChecksumAlgorithm",
                table: "MultipartUploads",
                type: "TEXT",
                maxLength: 20,
                nullable: true);

            migrationBuilder.AddForeignKey(
                name: "FK_UploadParts_MultipartUploads_UploadId",
                table: "UploadParts",
                column: "UploadId",
                principalTable: "MultipartUploads",
                principalColumn: "UploadId",
                onDelete: ReferentialAction.Cascade);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_UploadParts_MultipartUploads_UploadId",
                table: "UploadParts");

            migrationBuilder.DropColumn(
                name: "ChecksumCRC32",
                table: "UploadParts");

            migrationBuilder.DropColumn(
                name: "ChecksumCRC32C",
                table: "UploadParts");

            migrationBuilder.DropColumn(
                name: "ChecksumCRC64NVME",
                table: "UploadParts");

            migrationBuilder.DropColumn(
                name: "ChecksumSHA1",
                table: "UploadParts");

            migrationBuilder.DropColumn(
                name: "ChecksumSHA256",
                table: "UploadParts");

            migrationBuilder.DropColumn(
                name: "ChecksumCRC32",
                table: "Objects");

            migrationBuilder.DropColumn(
                name: "ChecksumCRC32C",
                table: "Objects");

            migrationBuilder.DropColumn(
                name: "ChecksumCRC64NVME",
                table: "Objects");

            migrationBuilder.DropColumn(
                name: "ChecksumSHA1",
                table: "Objects");

            migrationBuilder.DropColumn(
                name: "ChecksumSHA256",
                table: "Objects");

            migrationBuilder.DropColumn(
                name: "ChecksumAlgorithm",
                table: "MultipartUploads");
        }
    }
}
