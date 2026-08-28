using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace StockPipeline.Web.Messaging;

// The "buffer" side: takes a raw price from the frontend (via the SignalR hub)
// and drops it onto a RabbitMQ queue. This class never talks to the frontend
// directly and never adds anything to the number — its only job is publishing.
public class StockQueuePublisher : IAsyncDisposable
{
    private readonly RabbitMqOptions _options;
    private readonly Task<IChannel> _channelTask;
    private IConnection? _connection;
    private readonly ILogger<StockQueuePublisher> _logger;

    public StockQueuePublisher(IOptions<RabbitMqOptions> options, ILogger<StockQueuePublisher> logger)
    {
        _options = options.Value;
        _logger = logger;
        // Connecting is async, but this constructor can't be async, so we kick the
        // connection off here and await the completed Task the first time we publish.
        _channelTask = InitializeAsync();
    }

    // Lightweight health check: attempts a short-lived connection to the broker
    // and returns true when the broker accepts connections within the timeout.
    public async Task<bool> IsRabbitMqAvailableAsync(TimeSpan timeout)
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

        try
        {
            var createTask = factory.CreateConnectionAsync();
            var completed = await Task.WhenAny(createTask, Task.Delay(timeout));
            if (completed != createTask)
            {
                _logger.LogWarning("RabbitMQ health check timed out after {Timeout}s", timeout.TotalSeconds);
                return false;
            }

            using var conn = await createTask;
            await conn.CloseAsync();
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RabbitMQ health check failed");
            return false;
        }
    }

    private async Task<IChannel> InitializeAsync()
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
        var channel = await _connection.CreateChannelAsync();
        await channel.QueueDeclareAsync(
            queue: _options.QueueName,
            durable: false,
            exclusive: false,
            autoDelete: false);

        return channel;
    }

    public async Task PublishRawPriceAsync(double rawPrice)
    {
        var channel = await _channelTask;

        var message = new StockTickMessage(rawPrice, DateTimeOffset.UtcNow);
        var json = JsonSerializer.Serialize(message);
        var body = Encoding.UTF8.GetBytes(json);

        var properties = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent // persistent: survives a broker restart while still queued
        };

        await channel.BasicPublishAsync(
            exchange: string.Empty,          // the default exchange...
            routingKey: _options.QueueName,  // ...routes straight to a queue of this name
            mandatory: false,
            basicProperties: properties,
            body: body);
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.CloseAsync();
        }
    }
}
