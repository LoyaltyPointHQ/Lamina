#pragma warning disable EF1001 // MigrationsAssembly is internal but stable; Npgsql uses the same extension pattern
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Internal;

namespace Lamina.Storage.Sql.Context;

// Both SQLite and PostgreSQL migrations live in the same assembly (Lamina.Storage.Sql).
// EF Core scans all migrations decorated with [DbContext(T)] and applies them together,
// causing "table already exists" failures. It also picks ModelSnapshot alphabetically —
// always the SQLite one — causing PendingModelChangesWarning for PostgreSQL.
//
// This override filters Migrations and ModelSnapshot to the active provider's namespace
// convention (PostgreSql vs Sqlite). Migrations with neither keyword in their namespace
// are treated as shared and included for both providers.
public class ProviderAwareMigrationsAssembly : MigrationsAssembly
{
    private readonly bool _isPostgreSql;
    private readonly ModelSnapshot? _providerSnapshot;
    private IReadOnlyDictionary<string, TypeInfo>? _filteredMigrations;

    public ProviderAwareMigrationsAssembly(
        ICurrentDbContext currentContext,
        IDbContextOptions options,
        IMigrationsIdGenerator idGenerator,
        IDiagnosticsLogger<DbLoggerCategory.Migrations> logger)
        : base(currentContext, options, idGenerator, logger)
    {
        var providerName = currentContext.Context.Database.ProviderName ?? string.Empty;
        _isPostgreSql = providerName.Contains("PostgreSQL", StringComparison.OrdinalIgnoreCase);
        var contextType = currentContext.Context.GetType();

        _providerSnapshot = base.Assembly
            .GetTypes()
            .Where(t => typeof(ModelSnapshot).IsAssignableFrom(t) && !t.IsAbstract)
            .Where(t => t.GetCustomAttribute<DbContextAttribute>()?.ContextType == contextType)
            .Where(t => IsProviderNamespace(t.Namespace))
            .Select(t => (ModelSnapshot?)Activator.CreateInstance(t))
            .FirstOrDefault();
    }

    public override IReadOnlyDictionary<string, TypeInfo> Migrations =>
        _filteredMigrations ??= base.Migrations
            .Where(kvp => IsProviderNamespace(kvp.Value.Namespace))
            .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

    public override ModelSnapshot? ModelSnapshot => _providerSnapshot ?? base.ModelSnapshot;

    private bool IsProviderNamespace(string? ns)
    {
        ns ??= string.Empty;
        var isPg = ns.Contains("PostgreSql", StringComparison.OrdinalIgnoreCase);
        var isSqlite = ns.Contains("Sqlite", StringComparison.OrdinalIgnoreCase);
        // Shared migrations (neither keyword) are always included.
        if (!isPg && !isSqlite) return true;
        return _isPostgreSql ? isPg : isSqlite;
    }
}
