using Lamina.Configuration;
using Lamina.Middleware;
using Lamina.Models;
using Lamina.Services;
using Lamina.Streaming;
using Lamina.Streaming.Chunked;
using Lamina.Storage.Abstract;
using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Lamina.Storage.InMemory;
using Lamina.Storage.Sql;
using Lamina.Storage.Sql.Configuration;
using Lamina.Storage.Sql.Context;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Validate configuration on startup
try
{
    ConfigurationValidator.ValidateConfiguration(builder.Configuration);
}
catch (InvalidOperationException ex)
{
    Console.Error.WriteLine($"Startup failed: {ex.Message}");
    Environment.Exit(1);
}

// Configure logging
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.AddDebug();

// Add services to the container.
builder.Services.AddControllers(options =>
{
    options.RespectBrowserAcceptHeader = true;
})
.AddXmlSerializerFormatters()
.AddXmlDataContractSerializerFormatters();
builder.Services.AddOpenApi();

// Add health checks
builder.Services.AddHealthChecks();

// Configure authentication
builder.Services.Configure<AuthenticationSettings>(
    builder.Configuration.GetSection("Authentication"));

// Configure auto bucket creation
builder.Services.Configure<AutoBucketCreationSettings>(
    builder.Configuration.GetSection("AutoBucketCreation"));

// Configure bucket defaults
builder.Services.Configure<BucketDefaultsSettings>(
    builder.Configuration.GetSection("BucketDefaults"));

// Register authentication service
builder.Services.AddSingleton<IAuthenticationService, AuthenticationService>();
builder.Services.AddSingleton<IStreamingAuthenticationService, StreamingAuthenticationService>();

// Register chunked data parser for streaming support
builder.Services.AddSingleton<IChunkedDataParser, ChunkedDataParser>();

// Register auto bucket creation service
builder.Services.AddScoped<IAutoBucketCreationService, AutoBucketCreationService>();

// Configure and register lock manager based on configuration
var lockManagerType = builder.Configuration["LockManager"] ?? "InMemory";

if (lockManagerType.Equals("Redis", StringComparison.OrdinalIgnoreCase))
{
    // Configure Redis settings
    builder.Services.Configure<RedisSettings>(
        builder.Configuration.GetSection("Redis"));

    // Register Redis connection multiplexer
    builder.Services.AddSingleton<ConnectionMultiplexer>(provider =>
    {
        var redisSettings = builder.Configuration.GetSection("Redis").Get<RedisSettings>() ?? new RedisSettings();
        var configuration = ConfigurationOptions.Parse(redisSettings.ConnectionString);
        return ConnectionMultiplexer.Connect(configuration);
    });

    // Register RedLock factory
    builder.Services.AddSingleton<RedLockFactory>(provider =>
    {
        var redis = provider.GetRequiredService<ConnectionMultiplexer>();
        var redisSettings = builder.Configuration.GetSection("Redis").Get<RedisSettings>() ?? new RedisSettings();

        var multiplexers = new List<RedLockMultiplexer>
        {
            new RedLockMultiplexer(redis)
        };

        return RedLockFactory.Create(multiplexers);
    });

    // Register Redis-based lock manager
    builder.Services.AddSingleton<IFileSystemLockManager, RedisLockManager>();
}
else
{
    // Register in-memory lock manager (default)
    builder.Services.AddSingleton<IFileSystemLockManager, InMemoryLockManager>();
}

// Register S3 services based on configuration
var storageType = builder.Configuration["StorageType"] ?? "InMemory";
var metadataStorageType = builder.Configuration["MetadataStorageType"] ?? storageType;

// Configure SQL metadata storage if needed
if (metadataStorageType.Equals("Sql", StringComparison.OrdinalIgnoreCase))
{
    // Configure SQL storage settings
    builder.Services.Configure<SqlStorageSettings>(
        builder.Configuration.GetSection("SqlStorage"));

    var sqlSettings = builder.Configuration.GetSection("SqlStorage").Get<SqlStorageSettings>() ?? new SqlStorageSettings();

    // Add DbContext based on provider
    builder.Services.AddDbContext<LaminaDbContext>(options =>
    {
        if (sqlSettings.Provider == DatabaseProvider.PostgreSQL)
        {
            options.UseNpgsql(sqlSettings.ConnectionString, npgsqlOptions =>
            {
                npgsqlOptions.CommandTimeout(sqlSettings.CommandTimeout);
                npgsqlOptions.MigrationsAssembly("Lamina");
                npgsqlOptions.MigrationsHistoryTable("__EFMigrationsHistory", "public");
            });
        }
        else // SQLite
        {
            options.UseSqlite(sqlSettings.ConnectionString, sqliteOptions =>
            {
                sqliteOptions.CommandTimeout(sqlSettings.CommandTimeout);
                sqliteOptions.MigrationsAssembly("Lamina");
            });
        }

        if (sqlSettings.EnableSensitiveDataLogging)
        {
            options.EnableSensitiveDataLogging();
        }

        if (sqlSettings.EnableDetailedErrors)
        {
            options.EnableDetailedErrors();
        }
    });
}

// Register data storage services
if (storageType.Equals("Filesystem", StringComparison.OrdinalIgnoreCase))
{
    // Configure and register FilesystemStorageSettings with IOptions
    builder.Services.Configure<FilesystemStorageSettings>(
        builder.Configuration.GetSection("FilesystemStorage"));

    // Register NetworkFileSystemHelper for CIFS/NFS support
    builder.Services.AddSingleton<NetworkFileSystemHelper>();

    // Register bucket data services
    builder.Services.AddSingleton<IBucketDataStorage, FilesystemBucketDataStorage>();

    // Register data services for objects
    builder.Services.AddSingleton<IObjectDataStorage, FilesystemObjectDataStorage>();

    // Register data services for multipart uploads
    builder.Services.AddSingleton<IMultipartUploadDataStorage, FilesystemMultipartUploadDataStorage>();
}
else
{
    // Register in-memory data services
    builder.Services.AddSingleton<IBucketDataStorage, InMemoryBucketDataStorage>();
    builder.Services.AddSingleton<IObjectDataStorage, InMemoryObjectDataStorage>();
    builder.Services.AddSingleton<IMultipartUploadDataStorage, InMemoryMultipartUploadDataStorage>();
}

// Register metadata storage services as scoped (works with all storage types)
if (metadataStorageType.Equals("Sql", StringComparison.OrdinalIgnoreCase))
{
    // Register SQL metadata services
    builder.Services.AddScoped<IBucketMetadataStorage, SqlBucketMetadataStorage>();
    builder.Services.AddScoped<IObjectMetadataStorage, SqlObjectMetadataStorage>();
    builder.Services.AddScoped<IMultipartUploadMetadataStorage, SqlMultipartUploadMetadataStorage>();
}
else if (storageType.Equals("Filesystem", StringComparison.OrdinalIgnoreCase))
{
    // Register filesystem metadata services based on metadata mode
    var metadataMode = builder.Configuration.GetValue<string>("FilesystemStorage:MetadataMode") ?? "Inline";
    if (metadataMode.Equals("Xattr", StringComparison.OrdinalIgnoreCase))
    {
        builder.Services.AddScoped<IBucketMetadataStorage, XattrBucketMetadataStorage>();
        builder.Services.AddScoped<IObjectMetadataStorage, XattrObjectMetadataStorage>();
    }
    else
    {
        builder.Services.AddScoped<IBucketMetadataStorage, FilesystemBucketMetadataStorage>();
        builder.Services.AddScoped<IObjectMetadataStorage, FilesystemObjectMetadataStorage>();
    }

    // Register filesystem multipart upload metadata service
    builder.Services.AddScoped<IMultipartUploadMetadataStorage, FilesystemMultipartUploadMetadataStorage>();
}
else
{
    // Register in-memory metadata services
    builder.Services.AddScoped<IBucketMetadataStorage, InMemoryBucketMetadataStorage>();
    builder.Services.AddScoped<IObjectMetadataStorage, InMemoryObjectMetadataStorage>();
    builder.Services.AddScoped<IMultipartUploadMetadataStorage, InMemoryMultipartUploadMetadataStorage>();
}

// Register facade services as scoped (works with all storage types)
builder.Services.AddScoped<IBucketStorageFacade, BucketStorageFacade>();
builder.Services.AddScoped<IObjectStorageFacade, ObjectStorageFacade>();
builder.Services.AddScoped<IMultipartUploadStorageFacade, MultipartUploadStorageFacade>();

// Register multipart upload cleanup service if enabled
var cleanupEnabled = builder.Configuration.GetValue<bool>("MultipartUploadCleanup:Enabled", true);
if (cleanupEnabled)
{
    builder.Services.AddHostedService<MultipartUploadCleanupService>();
}

// Register metadata cleanup service if enabled
var metadataCleanupEnabled = builder.Configuration.GetValue<bool>("MetadataCleanup:Enabled", true);
if (metadataCleanupEnabled)
{
    builder.Services.AddHostedService<MetadataCleanupService>();
}

// Register temp file cleanup service if enabled and using filesystem storage
var tempFileCleanupEnabled = builder.Configuration.GetValue<bool>("TempFileCleanup:Enabled", true);
if (tempFileCleanupEnabled && storageType.Equals("Filesystem", StringComparison.OrdinalIgnoreCase))
{
    builder.Services.AddHostedService<TempFileCleanupService>();
}

var app = builder.Build();

// Run database migrations if SQL metadata storage is enabled
if (metadataStorageType.Equals("Sql", StringComparison.OrdinalIgnoreCase))
{
    using (var scope = app.Services.CreateScope())
    {
        var sqlSettings = scope.ServiceProvider.GetRequiredService<IOptions<SqlStorageSettings>>().Value;
        if (sqlSettings.MigrateOnStartup)
        {
            try
            {
                var context = scope.ServiceProvider.GetRequiredService<LaminaDbContext>();
                await context.Database.MigrateAsync();
                app.Logger.LogInformation("Database migrations applied successfully");
            }
            catch (Exception ex)
            {
                app.Logger.LogError(ex, "Failed to apply database migrations");
                throw;
            }
        }
    }
}

// Create configured buckets on startup
using (var scope = app.Services.CreateScope())
{
    var autoBucketService = scope.ServiceProvider.GetRequiredService<IAutoBucketCreationService>();
    await autoBucketService.CreateConfiguredBucketsAsync();
}

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// Add authentication middleware before controllers
app.UseS3Authentication();

app.MapControllers();

// Map health check endpoint (bypasses authentication)
app.MapHealthChecks("/health");

app.Run();

namespace Lamina
{
    public partial class Program { }
}