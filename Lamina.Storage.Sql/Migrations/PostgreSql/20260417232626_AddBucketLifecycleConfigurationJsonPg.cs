using Lamina.Storage.Sql.Context;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Lamina.Storage.Sql.Migrations.PostgreSql
{
    /// <inheritdoc />
    [DbContext(typeof(LaminaDbContext))]
    [Migration("20260417232626_AddBucketLifecycleConfigurationJsonPg")]
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
