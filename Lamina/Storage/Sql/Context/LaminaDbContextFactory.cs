using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Lamina.Storage.Sql.Configuration;

namespace Lamina.Storage.Sql.Context;

public class LaminaDbContextFactory : IDesignTimeDbContextFactory<LaminaDbContext>
{
    public LaminaDbContext CreateDbContext(string[] args)
    {
        var optionsBuilder = new DbContextOptionsBuilder<LaminaDbContext>();

        // Default to SQLite for migrations
        var provider = GetProviderFromArgs(args);
        var connectionString = GetConnectionStringFromArgs(args);

        if (provider == DatabaseProvider.PostgreSQL)
        {
            optionsBuilder.UseNpgsql(connectionString);
        }
        else
        {
            optionsBuilder.UseSqlite(connectionString);
        }

        return new LaminaDbContext(optionsBuilder.Options);
    }

    private static DatabaseProvider GetProviderFromArgs(string[] args)
    {
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

        // Default to SQLite
        return DatabaseProvider.SQLite;
    }

    private static string GetConnectionStringFromArgs(string[] args)
    {
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i].StartsWith("--SqlStorage:ConnectionString=", StringComparison.OrdinalIgnoreCase))
            {
                return args[i].Split('=', 2)[1].Trim('"');
            }
        }

        // Default connection strings
        return "Data Source=/tmp/lamina.db";
    }
}