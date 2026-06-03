#pragma warning disable EF1001 // MigrationsAssembly is internal but stable; Npgsql uses the same extension pattern
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Internal;

namespace Lamina.Storage.Sql.Context;

// EF Core selects ModelSnapshot by scanning the migrations assembly for the first type that
// inherits ModelSnapshot and carries [DbContext(typeof(T))]. With two providers sharing one
// assembly the selection is alphabetical — always the SQLite snapshot — causing a false
// PendingModelChangesWarning for PostgreSQL. This override picks the correct snapshot based
// on the active provider's namespace convention (PostgreSql vs non-PostgreSql).
public class ProviderAwareMigrationsAssembly : MigrationsAssembly
{
    private readonly ModelSnapshot? _providerSnapshot;

    public ProviderAwareMigrationsAssembly(
        ICurrentDbContext currentContext,
        IDbContextOptions options,
        IMigrationsIdGenerator idGenerator,
        IDiagnosticsLogger<DbLoggerCategory.Migrations> logger)
        : base(currentContext, options, idGenerator, logger)
    {
        var providerName = currentContext.Context.Database.ProviderName ?? string.Empty;
        var isPostgreSql = providerName.Contains("PostgreSQL", StringComparison.OrdinalIgnoreCase);
        var contextType = currentContext.Context.GetType();

        _providerSnapshot = base.Assembly
            .GetTypes()
            .Where(t => typeof(ModelSnapshot).IsAssignableFrom(t) && !t.IsAbstract)
            .Where(t => t.GetCustomAttribute<DbContextAttribute>()?.ContextType == contextType)
            .Select(t => (Type: t, IsPostgreSql: t.Namespace?.Contains("PostgreSql", StringComparison.OrdinalIgnoreCase) == true))
            .Where(t => t.IsPostgreSql == isPostgreSql)
            .Select(t => (ModelSnapshot?)Activator.CreateInstance(t.Type))
            .FirstOrDefault();
    }

    public override ModelSnapshot? ModelSnapshot => _providerSnapshot ?? base.ModelSnapshot;
}
