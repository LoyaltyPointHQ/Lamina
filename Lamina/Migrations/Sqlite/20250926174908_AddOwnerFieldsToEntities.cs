using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Lamina.Migrations.Sqlite
{
    /// <inheritdoc />
    public partial class AddOwnerFieldsToEntities : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "OwnerDisplayName",
                table: "Objects",
                type: "TEXT",
                maxLength: 256,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "OwnerId",
                table: "Objects",
                type: "TEXT",
                maxLength: 256,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "OwnerDisplayName",
                table: "Buckets",
                type: "TEXT",
                maxLength: 256,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "OwnerId",
                table: "Buckets",
                type: "TEXT",
                maxLength: 256,
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "OwnerDisplayName",
                table: "Objects");

            migrationBuilder.DropColumn(
                name: "OwnerId",
                table: "Objects");

            migrationBuilder.DropColumn(
                name: "OwnerDisplayName",
                table: "Buckets");

            migrationBuilder.DropColumn(
                name: "OwnerId",
                table: "Buckets");
        }
    }
}
