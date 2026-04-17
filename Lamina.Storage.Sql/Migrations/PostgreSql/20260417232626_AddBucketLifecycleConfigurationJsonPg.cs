using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Lamina.Storage.Sql.Migrations.PostgreSql
{
    /// <inheritdoc />
    public partial class AddBucketLifecycleConfigurationJsonPg : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "LifecycleConfigurationJson",
                table: "Buckets",
                type: "jsonb",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "LifecycleConfigurationJson",
                table: "Buckets");
        }
    }
}
