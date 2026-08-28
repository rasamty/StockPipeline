using StockPipeline.Web.Processing;

namespace StockPipeline.Web.Messaging;

// Wires the listener and the processor together and starts them when the web
// app starts. This is the only place those two classes know about each other.
public class StockQueueListenerHostedService : BackgroundService
{
    private readonly StockQueueListener _listener;
    private readonly StockProcessor _processor;

    public StockQueueListenerHostedService(StockQueueListener listener, StockProcessor processor)
    {
        _listener = listener;
        _processor = processor;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _listener.TickReceivedAsync += _processor.HandleTickAsync;
        await _listener.StartAsync();
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await _listener.DisposeAsync();
        await base.StopAsync(cancellationToken);
    }
}
