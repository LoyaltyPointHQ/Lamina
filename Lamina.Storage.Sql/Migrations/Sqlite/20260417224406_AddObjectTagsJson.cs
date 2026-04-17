using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Lamina.Migrations.Sqlite
{
    /// <inheritdoc />
    public partial class AddObjectTagsJson : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "TagsJson",
                table: "Objects",
                type: "TEXT",
                nullable: false,
                defaultValue: "{}");

            migrationBuilder.AddColumn<string>(
                name: "TagsJson",
                table: "MultipartUploads",
                type: "TEXT",
                nullable: false,
                defaultValue: "{}");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "TagsJson",
                table: "Objects");

            migrationBuilder.DropColumn(
                name: "TagsJson",
                table: "MultipartUploads");
        }
    }
}
