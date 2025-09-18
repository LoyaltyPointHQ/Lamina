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
    builder.Services.AddSingleton<IBucketService, FilesystemBucketService>();
    builder.Services.AddSingleton<IObjectService, FilesystemObjectService>();
    builder.Services.AddSingleton<IMultipartUploadService, FilesystemMultipartUploadService>();
    builder.Logging.AddConsole().SetMinimumLevel(LogLevel.Information);
    Console.WriteLine("Using Filesystem storage");
}
else
{
    builder.Services.AddSingleton<IBucketService, InMemoryBucketService>();
    builder.Services.AddSingleton<IObjectService, InMemoryObjectService>();
    builder.Services.AddSingleton<IMultipartUploadService, InMemoryMultipartUploadService>();
    Console.WriteLine("Using In-Memory storage");
}

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