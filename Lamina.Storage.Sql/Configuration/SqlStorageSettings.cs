namespace Lamina.Storage.Sql.Configuration;

public enum DatabaseProvider
{
    SQLite,
    PostgreSQL
}

public class SqlStorageSettings
{
    public DatabaseProvider Provider { get; set; } = DatabaseProvider.SQLite;
    public string ConnectionString { get; set; } = "Data Source=/tmp/lamina/metadata.db";
    public bool MigrateOnStartup { get; set; } = true;
    public int CommandTimeout { get; set; } = 30;
    public bool EnableSensitiveDataLogging { get; set; } = false;
    public bool EnableDetailedErrors { get; set; } = false;
}