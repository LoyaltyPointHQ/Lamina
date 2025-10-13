using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Lamina.Storage.Sql.Migrations.PostgreSql
{
    /// <inheritdoc />
    public partial class AddPartsMetadataJsonColumnPg : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "PartsMetadataJson",
                table: "MultipartUploads",
                type: "text",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "PartsMetadataJson",
                table: "MultipartUploads");
        }
    }
}
