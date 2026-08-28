using Microsoft.AspNetCore.SignalR;
using StockPipeline.Web.Hubs;
using StockPipeline.Web.Messaging;

namespace StockPipeline.Web.Processing;

// Reacts to ticks raised by StockQueueListener: adds a constant to the raw
// price and pushes the result back out to every connected browser.
public class StockProcessor
{
    // The "simply add a constant number" step. A plain static method with no
    // dependencies — which is exactly what makes it easy to unit test (see
    // StockPipeline.Tests) and exactly what runs as the automated QA gate.
    public const double AdjustmentConstant = 5.0;

    public static double ApplyAdjustment(double rawPrice) => rawPrice + AdjustmentConstant;

    private readonly IHubContext<StockHub> _hubContext;
    private readonly ILogger<StockProcessor> _logger;

    public StockProcessor(IHubContext<StockHub> hubContext, ILogger<StockProcessor> logger)
    {
        _hubContext = hubContext;
        _logger = logger;
    }

    public async Task HandleTickAsync(StockTickMessage message)
    {
        var processedPrice = ApplyAdjustment(message.RawPrice);

        _logger.LogInformation(
            "Processed tick: raw {RawPrice:F2} -> processed {ProcessedPrice:F2}",
            message.RawPrice, processedPrice);

        await _hubContext.Clients.All.SendAsync(
            "ReceiveProcessedPrice", processedPrice, message.TimestampUtc);
    }
}
