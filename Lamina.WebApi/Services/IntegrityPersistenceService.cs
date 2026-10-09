using Microsoft.EntityFrameworkCore;
using StackExchange.Redis;
using Lamina.Storage.Core;
using System.Data.Common;
using System.Diagnostics.Metrics;
using Lamina.Storage.Core.Abstract;
using Lamina.Storage.Core.Integrity;

namespace Lamina.WebApi.Services;

/// <summary>Persists detached integrity results only while their observed data version remains current.</summary>
public sealed class IntegrityPersistenceService(
    IntegrityPersistenceQueue queue,
    IntegrityPersistenceSettings settings,
    IServiceScopeFactory scopes,
    IObjectPublicationLock locks,
    ILogger<IntegrityPersistenceService> logger) : BackgroundService
{
    private static readonly Meter Meter = new("Lamina.Integrity.Persistence");
    private static readonly Counter<long> Outcomes = Meter.CreateCounter<long>("integrity.persistence.results");
    private readonly CancellationTokenSource _abort = new();

    private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await base.StartAsync(cancellationToken);
        // .NET runs ExecuteAsync on the thread pool. Ensure an immediate stop cannot cancel
        // its scheduling before consumers have taken ownership of accepted queue entries.
        await _started.Task.WaitAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _started.TrySetResult();
        return !queue.Enabled ? Task.CompletedTask
            : Task.WhenAll(Enumerable.Range(0, settings.Workers).Select(_ => ConsumeAsync(_abort.Token)));
    }

    private async Task ConsumeAsync(CancellationToken cancellationToken)
    {
        await foreach (var write in queue.ReadAllAsync(cancellationToken))
        {
            for (var attempt = 0; attempt < 3; attempt++)
            {
                try
                {
                    // Scope and publication lock are both disposed before retry backoff.
                    await using var scope = scopes.CreateAsyncScope();
                    var data = scope.ServiceProvider.GetRequiredService<IObjectDataStorage>();
                    var metadata = scope.ServiceProvider.GetRequiredService<IObjectMetadataStorage>();
                    if (metadata is not IConditionalObjectIntegrityStorage conditional) break;
                    await using var publication = await locks.AcquireAsync(write.BucketName, write.Key, cancellationToken);
                    var current = await data.GetDataInfoAsync(write.BucketName, write.Key, cancellationToken);
                    var result = current == null || current.Value.size != write.Size || current.Value.lastModified != write.DataLastModified
                        ? IntegrityWriteResult.Conflict
                        : await conditional.TryWriteIntegrityAsync(write, cancellationToken);
                    Outcomes.Add(1, new KeyValuePair<string, object?>("outcome", result.ToString()));
                    break;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch (Exception ex)
                {
                    if (!IsTransient(ex) || attempt == 2)
                    {
                        Outcomes.Add(1, new KeyValuePair<string, object?>("outcome", "error"));
                        logger.LogWarning(ex, "Unable to persist object integrity for {Bucket}/{Key}", write.BucketName, write.Key);
                        break;
                    }
                    await Task.Delay(TimeSpan.FromSeconds(attempt + 1), cancellationToken);
                }
            }
        }
    }

    private static bool IsTransient(Exception exception) => exception is
        IOException or TimeoutException or LockAcquisitionException or RedisConnectionException
        or RedisTimeoutException or DbException { IsTransient: true }
        || exception is DbUpdateException { InnerException: { } inner } && IsTransient(inner);

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        queue.Complete();
        // Do not cancel consumers until the host's graceful shutdown budget is exhausted.
        using var registration = cancellationToken.Register(() => _abort.Cancel());
        await base.StopAsync(cancellationToken);
    }

    public override void Dispose()
    {
        _abort.Cancel();
        base.Dispose();
        _abort.Dispose();
    }
}
