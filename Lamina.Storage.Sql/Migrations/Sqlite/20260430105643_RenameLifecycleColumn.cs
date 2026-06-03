using Lamina.Storage.Sql.Context;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Lamina.Migrations.Sqlite
{
    /// <inheritdoc />
    [DbContext(typeof(LaminaDbContext))]
    [Migration("20260430105643_RenameLifecycleColumn")]
    public partial class RenameLifecycleColumn : Migration
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
