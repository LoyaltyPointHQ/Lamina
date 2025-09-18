using S3Test.Services;
using S3Test.Configuration;
using S3Test.Models;
using S3Test.Middleware;

var builder = WebApplication.CreateBuilder(args);

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

// Configure storage limits
builder.Services.Configure<StorageLimits>(
    builder.Configuration.GetSection("StorageLimits"));

// Configure authentication
builder.Services.Configure<AuthenticationSettings>(
    builder.Configuration.GetSection("Authentication"));

// Register authentication service
builder.Services.AddSingleton<IAuthenticationService, AuthenticationService>();

// Register FileSystemLockManager for thread-safe file operations
builder.Services.AddSingleton<IFileSystemLockManager, FileSystemLockManager>();

// Register S3 services based on configuration
var storageType = builder.Configuration["StorageType"] ?? "InMemory";

if (storageType.Equals("Filesystem", StringComparison.OrdinalIgnoreCase))
{
    // Register bucket services
    builder.Services.AddSingleton<IBucketDataService, FilesystemBucketDataService>();
    builder.Services.AddSingleton<IBucketMetadataService, FilesystemBucketMetadataService>();

    // Register data and metadata services for objects
    builder.Services.AddSingleton<IObjectDataService, FilesystemObjectDataService>();
    builder.Services.AddSingleton<IObjectMetadataService, FilesystemObjectMetadataService>();

    // Register data and metadata services for multipart uploads
    builder.Services.AddSingleton<IMultipartUploadDataService, FilesystemMultipartUploadDataService>();
    builder.Services.AddSingleton<IMultipartUploadMetadataService, FilesystemMultipartUploadMetadataService>();

    builder.Logging.AddConsole().SetMinimumLevel(LogLevel.Information);
    Console.WriteLine("Using Filesystem storage");
}
else
{
    // Register bucket services
    builder.Services.AddSingleton<IBucketDataService, InMemoryBucketDataService>();
    builder.Services.AddSingleton<IBucketMetadataService, InMemoryBucketMetadataService>();

    // Register data and metadata services for objects
    builder.Services.AddSingleton<IObjectDataService, InMemoryObjectDataService>();
    builder.Services.AddSingleton<IObjectMetadataService, InMemoryObjectMetadataService>();

    // Register data and metadata services for multipart uploads
    builder.Services.AddSingleton<IMultipartUploadDataService, InMemoryMultipartUploadDataService>();
    builder.Services.AddSingleton<IMultipartUploadMetadataService, InMemoryMultipartUploadMetadataService>();

    Console.WriteLine("Using In-Memory storage");
}

// Register facade services
builder.Services.AddSingleton<IBucketServiceFacade, BucketServiceFacade>();
builder.Services.AddSingleton<IObjectServiceFacade, ObjectServiceFacade>();
builder.Services.AddSingleton<IMultipartUploadServiceFacade, MultipartUploadServiceFacade>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// Add authentication middleware before controllers
app.UseS3Authentication();

app.MapControllers();

app.Run();

public partial class Program { }