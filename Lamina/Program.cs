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

// Register authentication service
builder.Services.AddSingleton<IAuthenticationService, AuthenticationService>();
builder.Services.AddSingleton<IStreamingAuthenticationService, StreamingAuthenticationService>();

// Register chunked data parser for streaming support
builder.Services.AddSingleton<IChunkedDataParser, ChunkedDataParser>();

// Register auto bucket creation service
builder.Services.AddSingleton<IAutoBucketCreationService, AutoBucketCreationService>();

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

    // Register metadata services based on metadata mode
    var metadataMode = builder.Configuration.GetValue<string>("FilesystemStorage:MetadataMode") ?? "Inline";
    if (metadataMode.Equals("Xattr", StringComparison.OrdinalIgnoreCase))
    {
        builder.Services.AddSingleton<IBucketMetadataStorage, XattrBucketMetadataStorage>();
        builder.Services.AddSingleton<IObjectMetadataStorage, XattrObjectMetadataStorage>();
    }
    else
    {
        builder.Services.AddSingleton<IBucketMetadataStorage, FilesystemBucketMetadataStorage>();
        builder.Services.AddSingleton<IObjectMetadataStorage, FilesystemObjectMetadataStorage>();
    }

    // Register data and metadata services for multipart uploads
    builder.Services.AddSingleton<IMultipartUploadDataStorage, FilesystemMultipartUploadDataStorage>();
    builder.Services.AddSingleton<IMultipartUploadMetadataStorage, FilesystemMultipartUploadMetadataStorage>();

    builder.Logging.AddConsole().SetMinimumLevel(LogLevel.Information);
}
else
{
    // Register bucket services
    builder.Services.AddSingleton<IBucketDataStorage, InMemoryBucketDataStorage>();
    builder.Services.AddSingleton<IBucketMetadataStorage, InMemoryBucketMetadataStorage>();

    // Register data and metadata services for objects
    builder.Services.AddSingleton<IObjectDataStorage, InMemoryObjectDataStorage>();
    builder.Services.AddSingleton<IObjectMetadataStorage, InMemoryObjectMetadataStorage>();

    // Register data and metadata services for multipart uploads
    builder.Services.AddSingleton<IMultipartUploadDataStorage, InMemoryMultipartUploadDataStorage>();
    builder.Services.AddSingleton<IMultipartUploadMetadataStorage, InMemoryMultipartUploadMetadataStorage>();
}

// Register facade services
builder.Services.AddSingleton<IBucketStorageFacade, BucketStorageFacade>();
builder.Services.AddSingleton<IObjectStorageFacade, ObjectStorageFacade>();
builder.Services.AddSingleton<IMultipartUploadStorageFacade, MultipartUploadStorageFacade>();

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