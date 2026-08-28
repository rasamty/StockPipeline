using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace StockPipeline.Web.Messaging;

// The class that "raises events as the queue is filled". It knows nothing about
// stock prices, adjustments, or the frontend — it only watches the queue and
// raises TickReceivedAsync whenever a message arrives. Anything that cares
// about ticks (here, StockProcessor) subscribes to that event.
public class StockQueueListener : IAsyncDisposable
{
    private readonly RabbitMqOptions _options;
    private IConnection? _connection;
    private IChannel? _channel;

    // Raised once per message pulled off the queue.
    public event Func<StockTickMessage, Task>? TickReceivedAsync;

    public StockQueueListener(IOptions<RabbitMqOptions> options)
    {
        _options = options.Value;
    }

    public async Task StartAsync()
    {
        var factory = new ConnectionFactory
        {
            HostName = _options.HostName,
            Port = _options.Port,
            UserName = _options.UserName,
            Password = _options.Password,
            VirtualHost = _options.VirtualHost,
            Ssl = { Enabled = _options.UseTls, ServerName = _options.HostName }
        };

        _connection = await factory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();
        // Use a durable queue to avoid server-side features that may be
        // restricted in newer RabbitMQ versions (transient/non-exclusive
        // queues are deprecated). Durable queue persists across broker
        // restarts and aligns with publisher DeliveryMode.Persistent.
        await _channel.QueueDeclareAsync(
            queue: _options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false);

        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.ReceivedAsync += async (_, eventArgs) =>
        {
            var json = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            var message = JsonSerializer.Deserialize<StockTickMessage>(json);

            if (message is not null && TickReceivedAsync is not null)
            {
                // This is the "raises events" moment: every subscriber (StockProcessor)
                // gets a chance to react to this tick before we acknowledge it.
                await TickReceivedAsync.Invoke(message);
            }

            await _channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false);
        };

        await _channel.BasicConsumeAsync(
            queue: _options.QueueName,
            autoAck: false,
            consumer: consumer);
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.CloseAsync();
        }

        if (_connection is not null)
        {
            await _connection.CloseAsync();
        }
    }
}
