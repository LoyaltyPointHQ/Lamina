using S3Test.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers(options =>
{
    options.RespectBrowserAcceptHeader = true;
})
.AddXmlSerializerFormatters()
.AddXmlDataContractSerializerFormatters();
builder.Services.AddOpenApi();

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