using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Lamina.Storage.Sql.Configuration;

namespace Lamina.Storage.Sql.Context;

public class LaminaDbContextFactory : IDesignTimeDbContextFactory<LaminaDbContext>
{
    public LaminaDbContext CreateDbContext(string[] args)
    {
        var optionsBuilder = new DbContextOptionsBuilder<LaminaDbContext>();

        // Get provider from args or environment variable
        var provider = GetProviderFromArgs(args);
        var connectionString = GetConnectionStringFromArgs(args, provider);

        if (provider == DatabaseProvider.PostgreSQL)
        {
            optionsBuilder.UseNpgsql(connectionString, npgsqlOptions =>
            {
                npgsqlOptions.MigrationsAssembly("Lamina");
                npgsqlOptions.MigrationsHistoryTable("__EFMigrationsHistory", "public");
            });
        }
        else
        {
            optionsBuilder.UseSqlite(connectionString, sqliteOptions =>
            {
                sqliteOptions.MigrationsAssembly("Lamina");
            });
        }

        return new LaminaDbContext(optionsBuilder.Options);
    }

    private static DatabaseProvider GetProviderFromArgs(string[] args)
    {
        // Check command line arguments first
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i].StartsWith("--SqlStorage:Provider=", StringComparison.OrdinalIgnoreCase))
            {
                var providerValue = args[i].Split('=')[1];
                if (Enum.TryParse<DatabaseProvider>(providerValue, true, out var provider))
                {
                    return provider;
                }
            }
        }

        // Check environment variable
        var envProvider = Environment.GetEnvironmentVariable("LAMINA_DB_PROVIDER");
        if (!string.IsNullOrEmpty(envProvider) && Enum.TryParse<DatabaseProvider>(envProvider, true, out var envProviderEnum))
        {
            return envProviderEnum;
        }

        // Default to SQLite
        return DatabaseProvider.SQLite;
    }

    private static string GetConnectionStringFromArgs(string[] args, DatabaseProvider provider)
    {
        // Check command line arguments first
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i].StartsWith("--SqlStorage:ConnectionString=", StringComparison.OrdinalIgnoreCase))
            {
                return args[i].Split('=', 2)[1].Trim('"');
            }
        }

        // Check environment variable
        var envConnectionString = Environment.GetEnvironmentVariable("LAMINA_DB_CONNECTION_STRING");
        if (!string.IsNullOrEmpty(envConnectionString))
        {
            return envConnectionString;
        }

        // Default connection strings based on provider
        return provider == DatabaseProvider.PostgreSQL
            ? "Host=localhost;Database=lamina;Username=lamina;Password=lamina"
            : "Data Source=/tmp/lamina.db";
    }
}