using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Lamina.Migrations.Sqlite
{
    /// <inheritdoc />
    public partial class AddBucketLifecycleConfigurationJson : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "TagsJson",
                table: "Objects",
                type: "TEXT",
                nullable: false,
                defaultValue: "");

            migrationBuilder.AddColumn<string>(
                name: "LifecycleConfigurationJson",
                table: "Buckets",
                type: "TEXT",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "TagsJson",
                table: "Objects");

            migrationBuilder.DropColumn(
                name: "LifecycleConfigurationJson",
                table: "Buckets");
        }
    }
}
