using Lamina.Configuration;
using Lamina.Middleware;
using Lamina.Models;
using Lamina.Services;
using Lamina.Storage.Abstract;
using Lamina.Storage.Filesystem;
using Lamina.Storage.Filesystem.Configuration;
using Lamina.Storage.Filesystem.Helpers;
using Lamina.Storage.Filesystem.Locking;
using Lamina.Storage.InMemory;

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

// Register auto bucket creation service
builder.Services.AddSingleton<IAutoBucketCreationService, AutoBucketCreationService>();

// Register FileSystemLockManager for thread-safe file operations
builder.Services.AddSingleton<IFileSystemLockManager, InMemoryLockManager>();

// Register S3 services based on configuration
var storageType = builder.Configuration["StorageType"] ?? "InMemory";

if (storageType.Equals("Filesystem", StringComparison.OrdinalIgnoreCase))
{
    // Configure and register FilesystemStorageSettings with IOptions
    builder.Services.Configure<FilesystemStorageSettings>(
        builder.Configuration.GetSection("FilesystemStorage"));

    // Register NetworkFileSystemHelper for CIFS/NFS support
    builder.Services.AddSingleton<NetworkFileSystemHelper>();

    // Register bucket services
    builder.Services.AddSingleton<IBucketDataStorage, FilesystemBucketDataStorage>();
    builder.Services.AddSingleton<IBucketMetadataStorage, FilesystemBucketMetadataStorage>();

    // Register data and metadata services for objects
    builder.Services.AddSingleton<IObjectDataStorage, FilesystemObjectDataStorage>();
    builder.Services.AddSingleton<IObjectMetadataStorage, FilesystemObjectMetadataStorage>();

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