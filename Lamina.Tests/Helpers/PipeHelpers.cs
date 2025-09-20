using System.Buffers;
using System.IO.Pipelines;

namespace Lamina.Tests.Helpers;

public static class PipeHelpers
{
    public static PipeReader CreatePipeReader(byte[] data)
    {
        var pipe = new Pipe();
        var writer = pipe.Writer;

        // Write data to pipe and complete
        _ = Task.Run(async () =>
        {
            try
            {
                await writer.WriteAsync(data);
            }
            finally
            {
                await writer.CompleteAsync();
            }
        });

        return pipe.Reader;
    }

    public static async Task<byte[]> ReadAllBytesAsync(PipeReader reader)
    {
        var segments = new List<byte[]>();

        try
        {
            while (true)
            {
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;

                if (buffer.Length > 0)
                {
                    // Convert ReadOnlySequence to byte array
                    var bytes = new byte[buffer.Length];
                    buffer.CopyTo(bytes);
                    segments.Add(bytes);
                }

                reader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        finally
        {
            await reader.CompleteAsync();
        }

        // Combine all segments
        var totalLength = segments.Sum(s => s.Length);
        var combinedData = new byte[totalLength];
        int offset = 0;
        foreach (var segment in segments)
        {
            Buffer.BlockCopy(segment, 0, combinedData, offset, segment.Length);
            offset += segment.Length;
        }

        return combinedData;
    }
}