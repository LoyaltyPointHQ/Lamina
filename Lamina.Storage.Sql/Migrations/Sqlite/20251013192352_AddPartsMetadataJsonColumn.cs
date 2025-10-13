using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Lamina.Storage.Sql.Migrations.Sqlite
{
    /// <inheritdoc />
    public partial class AddPartsMetadataJsonColumn : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "PartsMetadataJson",
                table: "MultipartUploads",
                type: "TEXT",
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
