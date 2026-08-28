namespace StockPipeline.Web.Messaging;

// The message shape that travels through RabbitMQ: the raw, frontend-generated
// price and the moment it was published. Serialized to JSON on the way in,
// deserialized on the way out.
public record StockTickMessage(double RawPrice, DateTimeOffset TimestampUtc);
