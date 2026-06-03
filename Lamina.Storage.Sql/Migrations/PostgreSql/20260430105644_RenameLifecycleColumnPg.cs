using Lamina.Storage.Sql.Context;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Lamina.Storage.Sql.Migrations.PostgreSql
{
    /// <inheritdoc />
    [DbContext(typeof(LaminaDbContext))]
    [Migration("20260430105644_RenameLifecycleColumnPg")]
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
