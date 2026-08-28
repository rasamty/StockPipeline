namespace StockPipeline.Web.Monitoring;

public class MonitorStateStore
{
    private readonly object _lock = new();
    private object? _payload;

    public void SetPayload(object? payload)
    {
        lock (_lock)
        {
            _payload = payload;
        }
    }

    public object? GetPayload()
    {
        lock (_lock)
        {
            return _payload;
        }
    }
}
