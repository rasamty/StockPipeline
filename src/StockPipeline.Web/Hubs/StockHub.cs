using Microsoft.AspNetCore.SignalR;
using StockPipeline.Web.Messaging;

namespace StockPipeline.Web.Hubs;

// The real-time link to the browser. The frontend calls SendRawPrice() every
// tick; the backend calls the browser's "ReceiveProcessedPrice" client method
// (from StockProcessor) whenever a processed value comes back off the queue.
public class StockHub : Hub
{
    private readonly StockQueuePublisher _publisher;

    public StockHub(StockQueuePublisher publisher)
    {
        _publisher = publisher;
    }

    public Task SendRawPrice(double rawPrice) => _publisher.PublishRawPriceAsync(rawPrice);
}
