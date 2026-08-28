namespace StockPipeline.Web.Messaging;

// Bound from the "RabbitMq" section of appsettings.json (and its per-environment
// overrides, e.g. appsettings.QA.json). Every environment points at a different
// queue name so DEV/QA/UAT/PROD never see each other's messages, even when they
// share one RabbitMQ broker.
public class RabbitMqOptions
{
    public const string SectionName = "RabbitMq";

    public string HostName { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string UserName { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string VirtualHost { get; set; } = "/";
    public string QueueName { get; set; } = "stock.raw.dev";
    public string EnvironmentLabel { get; set; } = "DEV";

    // Off by default (matches Docker's local, non-TLS broker). The cloud
    // environments (Part 8) turn this on via an Azure Application Setting,
    // once RabbitMQ is CloudAMQP instead of Docker.
    public bool UseTls { get; set; } = false;
}
