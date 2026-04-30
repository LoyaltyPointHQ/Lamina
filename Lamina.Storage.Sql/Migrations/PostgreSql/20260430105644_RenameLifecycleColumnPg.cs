using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Lamina.Storage.Sql.Migrations.PostgreSql
{
    /// <inheritdoc />
    public partial class RenameLifecycleColumnPg : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.RenameColumn(
                name: "LifecycleConfigurationJson",
                table: "Buckets",
                newName: "Lifecycle");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.RenameColumn(
                name: "Lifecycle",
                table: "Buckets",
                newName: "LifecycleConfigurationJson");
        }
    }
}
