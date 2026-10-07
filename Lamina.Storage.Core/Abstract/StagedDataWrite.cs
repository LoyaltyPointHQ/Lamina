namespace Lamina.Storage.Core.Abstract;

/// <summary>A private write session. Disposal discards uncommitted bytes, including after sealing.</summary>
public sealed class StagedDataWrite : IAsyncDisposable
{
    private readonly Func<long, PreparedData> _seal;
    private readonly Action _cleanup;
    private bool _closed;
    private bool _disposed;

    public Stream Stream { get; }

    public StagedDataWrite(Stream stream, Func<long, PreparedData> seal, Action cleanup)
    {
        Stream = stream;
        _seal = seal;
        _cleanup = cleanup;
    }

    public async Task<PreparedData> SealAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_closed, this);
        cancellationToken.ThrowIfCancellationRequested();
        await Stream.FlushAsync(cancellationToken);
        var size = Stream.Length;
        await Stream.DisposeAsync();
        _closed = true;
        return _seal(size);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        try { await Stream.DisposeAsync(); }
        finally { _closed = true; _cleanup(); }
    }
}
