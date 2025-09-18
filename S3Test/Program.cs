using S3Test.Services;
using S3Test.Configuration;

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

// Register S3 services
builder.Services.AddSingleton<IBucketService, InMemoryBucketService>();
builder.Services.AddSingleton<IObjectService, InMemoryObjectService>();
builder.Services.AddSingleton<IMultipartUploadService, InMemoryMultipartUploadService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapControllers();

app.Run();

public partial class Program { }