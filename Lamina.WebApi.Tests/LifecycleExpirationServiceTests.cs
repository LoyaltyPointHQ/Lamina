using System.Net;
using System.Text;
using Lamina.Core.Models;
using Lamina.Storage.Core.Abstract;
using Lamina.WebApi.Services;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Lamina.WebApi.Tests.Controllers;

public class LifecycleExpirationServiceTests : IntegrationTestBase
{
    public LifecycleExpirationServiceTests(WebApplicationFactory<global::Program> factory) : base(factory) { }

    private async Task<string> CreateBucketAsync() =>
        await CreateBucketInternal();

    private async Task<string> CreateBucketInternal()
    {
        var name = $"lc-{Guid.NewGuid():N}";
        await Client.PutAsync($"/{name}", null);
        return name;
    }

    private async Task PutLifecycleAsync(string bucket, LifecycleConfiguration config)
    {
        using var scope = Factory.Services.CreateScope();
        var bucketStorage = scope.ServiceProvider.GetRequiredService<IBucketStorageFacade>();
        await bucketStorage.SetLifecycleConfigurationAsync(bucket, config);
    }

    private Task PutObjectAsync(string bucket, string key, string content = "data") =>
        Client.PutAsync($"/{bucket}/{key}", new StringContent(content, Encoding.UTF8, "text/plain"));

    private async Task<bool> ObjectExistsAsync(string bucket, string key)
    {
        var resp = await Client.SendAsync(new HttpRequestMessage(HttpMethod.Head, $"/{bucket}/{key}"));
        return resp.StatusCode == HttpStatusCode.OK;
    }

    private async Task<int> MultipartUploadCountAsync(string bucket)
    {
        using var scope = Factory.Services.CreateScope();
        var mpu = scope.ServiceProvider.GetRequiredService<IMultipartUploadStorageFacade>();
        var list = await mpu.ListMultipartUploadsAsync(bucket);
        return list.Count;
    }

    private LifecycleExpirationService GetService()
    {
        return Factory.Services.GetServices<IHostedService>()
            .OfType<LifecycleExpirationService>()
            .FirstOrDefault() ?? CreateStandalone();
    }

    private LifecycleExpirationService CreateStandalone()
    {
        return new LifecycleExpirationService(
            Factory.Services,
            Factory.Services.GetRequiredService<ILoggerFactory>().CreateLogger<LifecycleExpirationService>(),
            Factory.Services.GetRequiredService<IConfiguration>());
    }

    [Fact]
    public async Task ExecuteCycle_OldObjectMatchingRule_IsDeleted()
    {
        var bucket = await CreateBucketAsync();
        await PutObjectAsync(bucket, "old.txt");
        await PutLifecycleAsync(bucket, new LifecycleConfiguration
        {
            Rules = new() { new LifecycleRule
            {
                Id = "expire",
                Status = LifecycleRuleStatus.Enabled,
                Filter = new LifecycleFilter { Prefix = "" },
                Expiration = new LifecycleExpiration { Days = 1 }
            }}
        });

        var future = DateTime.UtcNow.AddDays(10);
        await GetService().ExecuteCycleAsync(CancellationToken.None, future);

        Assert.False(await ObjectExistsAsync(bucket, "old.txt"));
    }

    [Fact]
    public async Task ExecuteCycle_FreshObject_IsKept()
    {
        var bucket = await CreateBucketAsync();
        await PutObjectAsync(bucket, "fresh.txt");
        await PutLifecycleAsync(bucket, new LifecycleConfiguration
        {
            Rules = new() { new LifecycleRule
            {
                Id = "expire",
                Status = LifecycleRuleStatus.Enabled,
                Filter = new LifecycleFilter { Prefix = "" },
                Expiration = new LifecycleExpiration { Days = 30 }
            }}
        });

        var future = DateTime.UtcNow.AddDays(1);
        await GetService().ExecuteCycleAsync(CancellationToken.None, future);

        Assert.True(await ObjectExistsAsync(bucket, "fresh.txt"));
    }

    [Fact]
    public async Task ExecuteCycle_PrefixFilter_OnlyMatchingDeleted()
    {
        var bucket = await CreateBucketAsync();
        await PutObjectAsync(bucket, "logs/a.log");
        await PutObjectAsync(bucket, "data/a.txt");
        await PutLifecycleAsync(bucket, new LifecycleConfiguration
        {
            Rules = new() { new LifecycleRule
            {
                Id = "expire-logs",
                Status = LifecycleRuleStatus.Enabled,
                Filter = new LifecycleFilter { Prefix = "logs/" },
                Expiration = new LifecycleExpiration { Days = 1 }
            }}
        });

        var future = DateTime.UtcNow.AddDays(10);
        await GetService().ExecuteCycleAsync(CancellationToken.None, future);

        Assert.False(await ObjectExistsAsync(bucket, "logs/a.log"));
        Assert.True(await ObjectExistsAsync(bucket, "data/a.txt"));
    }

    [Fact]
    public async Task ExecuteCycle_DisabledRule_NothingDeleted()
    {
        var bucket = await CreateBucketAsync();
        await PutObjectAsync(bucket, "old.txt");
        await PutLifecycleAsync(bucket, new LifecycleConfiguration
        {
            Rules = new() { new LifecycleRule
            {
                Id = "expire",
                Status = LifecycleRuleStatus.Disabled,
                Filter = new LifecycleFilter { Prefix = "" },
                Expiration = new LifecycleExpiration { Days = 1 }
            }}
        });

        var future = DateTime.UtcNow.AddDays(100);
        await GetService().ExecuteCycleAsync(CancellationToken.None, future);

        Assert.True(await ObjectExistsAsync(bucket, "old.txt"));
    }

    [Fact]
    public async Task ExecuteCycle_NoLifecycleConfig_NothingDeleted()
    {
        var bucket = await CreateBucketAsync();
        await PutObjectAsync(bucket, "old.txt");

        var future = DateTime.UtcNow.AddDays(100);
        await GetService().ExecuteCycleAsync(CancellationToken.None, future);

        Assert.True(await ObjectExistsAsync(bucket, "old.txt"));
    }

    [Fact]
    public async Task ExecuteCycle_AbortMPU_StaleUpload_Aborted()
    {
        var bucket = await CreateBucketAsync();
        await PutLifecycleAsync(bucket, new LifecycleConfiguration
        {
            Rules = new() { new LifecycleRule
            {
                Id = "abort-mpu",
                Status = LifecycleRuleStatus.Enabled,
                Filter = new LifecycleFilter { Prefix = "" },
                AbortIncompleteMultipartUpload = new LifecycleAbortIncompleteMultipartUpload { DaysAfterInitiation = 1 }
            }}
        });

        using (var scope = Factory.Services.CreateScope())
        {
            var mpu = scope.ServiceProvider.GetRequiredService<IMultipartUploadStorageFacade>();
            await mpu.InitiateMultipartUploadAsync(bucket, "big.bin",
                new InitiateMultipartUploadRequest { Key = "big.bin" });
        }

        var future = DateTime.UtcNow.AddDays(5);
        await GetService().ExecuteCycleAsync(CancellationToken.None, future);

        Assert.Equal(0, await MultipartUploadCountAsync(bucket));
    }
}
