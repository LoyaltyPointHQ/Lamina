using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Lamina.Storage.Sql.Migrations.PostgreSql
{
    /// <inheritdoc />
    public partial class AddObjectTagsJsonPg : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "TagsJson",
                table: "Objects",
                type: "jsonb",
                nullable: false,
                defaultValue: "{}");

            migrationBuilder.AddColumn<string>(
                name: "TagsJson",
                table: "MultipartUploads",
                type: "jsonb",
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
