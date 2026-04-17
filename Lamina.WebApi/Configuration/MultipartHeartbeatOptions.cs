namespace Lamina.WebApi.Configuration;

public class MultipartHeartbeatOptions
{
    public const string SectionName = "MultipartUpload:Heartbeat";

    public bool Enabled { get; set; } = true;
    public int IntervalSeconds { get; set; } = 5;
}
