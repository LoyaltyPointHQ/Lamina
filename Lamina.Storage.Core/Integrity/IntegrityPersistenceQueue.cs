using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading.Channels;

namespace Lamina.Storage.Core.Integrity;

public sealed class IntegrityPersistenceQueue
{
    private static readonly Meter Meter = new("Lamina.Integrity");
    private static readonly UpDownCounter<long> Pending = Meter.CreateUpDownCounter<long>("integrity.queue.pending");
    private static readonly Histogram<double> Wait = Meter.CreateHistogram<double>("integrity.queue.wait", "ms");
    private readonly Channel<ObjectIntegrityWrite> _channel;
    public bool Enabled { get; }

    public IntegrityPersistenceQueue(IntegrityPersistenceSettings settings)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(settings.Capacity, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(settings.Workers, 1);
        Enabled = settings.Enabled;
        _channel = Channel.CreateBounded<ObjectIntegrityWrite>(new BoundedChannelOptions(settings.Capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        });
    }

    public async ValueTask EnqueueAsync(ObjectIntegrityWrite write, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        // Copy before awaiting: no mutable request-owned state survives admission.
        var detached = write with { Checksums = new Dictionary<string, string>(write.Checksums) };
        await _channel.Writer.WriteAsync(detached, cancellationToken);
        Pending.Add(1);
        Wait.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
    }

    public async IAsyncEnumerable<ObjectIntegrityWrite> ReadAllAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var write in _channel.Reader.ReadAllAsync(cancellationToken))
        {
            Pending.Add(-1);
            yield return write;
        }
    }

    public void Complete() => _channel.Writer.TryComplete();
}
