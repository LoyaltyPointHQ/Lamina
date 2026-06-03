using Lamina.Storage.Sql.Context;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.PostgreSql;

namespace Lamina.Storage.Sql.Tests;

// ── SQLite ────────────────────────────────────────────────────────────────────

public sealed class MigrationSqliteTests : IDisposable
{
    private readonly List<string> _tempFiles = [];

    [Fact]
    public async Task SQLite_AllMigrations_Apply()
    {
        await using var context = CreateContext();
        await context.Database.MigrateAsync();

        var pending = await context.Database.GetPendingMigrationsAsync();
        Assert.Empty(pending);
    }

    [Fact]
    public async Task SQLite_MigrateAsync_NoPendingModelChanges()
    {
        // EF Core 9+ throws PendingModelChangesWarning (as InvalidOperationException) inside
        // MigrateAsync when the model snapshot diverges from OnModelCreating.
        // A clean run is the regression proof for the snapshot mismatch fix.
        await using var context = CreateContext();
        await context.Database.MigrateAsync();
    }

    [Fact]
    public void SQLite_ProviderAwareMigrationsAssembly_SelectsSqliteSnapshot()
    {
        using var context = CreateContext();
#pragma warning disable EF1001
        var assembly = context.GetInfrastructure().GetRequiredService<IMigrationsAssembly>();
#pragma warning restore EF1001
        Assert.IsType<ProviderAwareMigrationsAssembly>(assembly);
        var ns = assembly.ModelSnapshot?.GetType().Namespace ?? string.Empty;
        Assert.Contains("Sqlite", ns, StringComparison.OrdinalIgnoreCase);
    }

    private LaminaDbContext CreateContext()
    {
        var dbFile = Path.GetTempFileName();
        _tempFiles.Add(dbFile);
        var options = new DbContextOptionsBuilder<LaminaDbContext>()
            .UseSqlite($"Data Source={dbFile}", o => o.MigrationsAssembly("Lamina.Storage.Sql"))
            .ReplaceService<IMigrationsAssembly, ProviderAwareMigrationsAssembly>()
            .Options;
        return new LaminaDbContext(options);
    }

    public void Dispose()
    {
        foreach (var file in _tempFiles.Where(File.Exists))
            File.Delete(file);
    }
}

// ── PostgreSQL (Testcontainers) ───────────────────────────────────────────────

public sealed class MigrationPostgreSqlTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _container.StartAsync();
    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    [Fact]
    public async Task PostgreSql_AllMigrations_Apply()
    {
        await using var context = CreateContext();
        await context.Database.MigrateAsync();

        var pending = await context.Database.GetPendingMigrationsAsync();
        Assert.Empty(pending);
    }

    [Fact]
    public async Task PostgreSql_MigrateAsync_NoPendingModelChanges()
    {
        // EF Core 9+ throws PendingModelChangesWarning (as InvalidOperationException) inside
        // MigrateAsync when the model snapshot diverges from OnModelCreating.
        // A clean run is the regression proof — the wrong SQLite snapshot would diverge on
        // jsonb vs TEXT column types and fail here.
        await using var context = CreateContext();
        await context.Database.MigrateAsync();
    }

    [Fact]
    public async Task PostgreSql_ProviderAwareMigrationsAssembly_SelectsPostgreSqlSnapshot()
    {
        await using var context = CreateContext();
#pragma warning disable EF1001
        var assembly = context.GetInfrastructure().GetRequiredService<IMigrationsAssembly>();
#pragma warning restore EF1001
        Assert.IsType<ProviderAwareMigrationsAssembly>(assembly);
        var ns = assembly.ModelSnapshot?.GetType().Namespace ?? string.Empty;
        Assert.Contains("PostgreSql", ns, StringComparison.OrdinalIgnoreCase);
    }

    private LaminaDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<LaminaDbContext>()
            .UseNpgsql(_container.GetConnectionString(), o =>
            {
                o.MigrationsAssembly("Lamina.Storage.Sql");
                o.MigrationsHistoryTable("__EFMigrationsHistory", "public");
            })
            .ReplaceService<IMigrationsAssembly, ProviderAwareMigrationsAssembly>()
            .Options;
        return new LaminaDbContext(options);
    }
}

// ── Assembly sanity ───────────────────────────────────────────────────────────

public sealed class MigrationAssemblyTests
{
    [Fact]
    public void MigrationsAssembly_ContainsSqliteSnapshot()
    {
        var snapshots = typeof(LaminaDbContext).Assembly
            .GetTypes()
            .Where(t => typeof(ModelSnapshot).IsAssignableFrom(t) && !t.IsAbstract)
            .Select(t => t.Namespace ?? string.Empty)
            .ToList();

        Assert.Contains(snapshots, ns => ns.Contains("Sqlite", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void MigrationsAssembly_ContainsPostgreSqlSnapshot()
    {
        var snapshots = typeof(LaminaDbContext).Assembly
            .GetTypes()
            .Where(t => typeof(ModelSnapshot).IsAssignableFrom(t) && !t.IsAbstract)
            .Select(t => t.Namespace ?? string.Empty)
            .ToList();

        Assert.Contains(snapshots, ns => ns.Contains("PostgreSql", StringComparison.OrdinalIgnoreCase));
    }
}
