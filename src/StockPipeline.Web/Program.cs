using Microsoft.Extensions.Options;
using System.Linq;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using StockPipeline.Web.Hubs;
using StockPipeline.Web.Messaging;
using StockPipeline.Web.Processing;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<RabbitMqOptions>(
    builder.Configuration.GetSection(RabbitMqOptions.SectionName));

builder.Services.AddSignalR();
builder.Services.AddSingleton<StockQueuePublisher>();
builder.Services.AddSingleton<StockQueueListener>();
builder.Services.AddSingleton<StockProcessor>();
builder.Services.AddHostedService<StockQueueListenerHostedService>();
builder.Services.AddHostedService<StockPipeline.Web.Monitoring.DockerRabbitMonitorService>();
builder.Services.AddSingleton<StockPipeline.Web.Monitoring.MonitorStateStore>();

var app = builder.Build();

// Log the effective RabbitMQ configuration at startup so it's easy to
// verify which settings were loaded for the current environment.
{
    var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("StartupConfig");
    try
    {
        var opts = app.Services.GetRequiredService<Microsoft.Extensions.Options.IOptions<StockPipeline.Web.Messaging.RabbitMqOptions>>().Value;
        logger.LogInformation("RabbitMQ config loaded: Host={Host} Port={Port} Queue={Queue} EnvLabel={Label} UseTls={UseTls}", opts.HostName, opts.Port, opts.QueueName, opts.EnvironmentLabel, opts.UseTls);
    }
    catch (Exception ex)
    {
        logger.LogWarning(ex, "Failed to read RabbitMqOptions from configuration");
    }
}

app.UseDefaultFiles();  // serves wwwroot/index.html at "/"
app.UseStaticFiles();   // serves site.css / site.js / build-info.json from wwwroot

app.MapHub<StockHub>("/hubs/stock");

// Tells the frontend which environment it's talking to, so the on-screen
// badge (DEV/QA/UAT/PROD) always matches the app that's actually running.
app.MapGet("/api/environment", (IOptions<RabbitMqOptions> options) =>
    Results.Ok(new { environment = options.Value.EnvironmentLabel }));

// Expose current monitor state so clients can fetch it after they connect
app.MapGet("/api/monitor/status", (StockPipeline.Web.Monitoring.MonitorStateStore store) =>
{
    var payload = store.GetPayload();
    if (payload is null) return Results.NoContent();
    return Results.Ok(payload);
});

// Deliberately NOT a build-info API endpoint: build-info.json is a plain static
// file (see the pipeline's "stamp build info" step), generated once at build
// time and served as-is by UseStaticFiles() above. That's what lets the
// promotion checks (Part 6) compare "what's really running in QA" against
// "what we're about to promote" with a single GET — no API surface of our own
// to keep in sync.

// -----------------------------
// TEST REGION
// Quick, non-invasive runtime checks you can use during local development
// to validate registrations and simple behavior without touching external
// resources (RabbitMQ, SignalR clients, etc.). This runs only in
// Development environment so it won't affect staging/production.
if (app.Environment.IsDevelopment())
{
    var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("StartupTests");

    try
    {
        // Validate service registrations (no instantiation of networked services)
        var services = builder.Services;
        var hasPublisher = services.Any(sd => sd.ServiceType == typeof(StockQueuePublisher) || sd.ImplementationType == typeof(StockQueuePublisher));
        var hasListener = services.Any(sd => sd.ServiceType == typeof(StockQueueListener) || sd.ImplementationType == typeof(StockQueueListener));
        var hasProcessor = services.Any(sd => sd.ServiceType == typeof(StockProcessor) || sd.ImplementationType == typeof(StockProcessor));
        var hasHosted = services.Any(sd => sd.ServiceType == typeof(Microsoft.Extensions.Hosting.IHostedService) && sd.ImplementationType == typeof(StockQueueListenerHostedService));

        logger.LogInformation("TEST: registrations - Publisher:{Publisher} Listener:{Listener} Processor:{Processor} HostedService:{Hosted}", hasPublisher, hasListener, hasProcessor, hasHosted);

        // Lightweight functional checks that don't require external connections
        var tick = new StockTickMessage(123.45, DateTimeOffset.UtcNow);
        var json = JsonSerializer.Serialize(tick);
        var tick2 = JsonSerializer.Deserialize<StockTickMessage>(json);
        var adjusted = StockProcessor.ApplyAdjustment(tick.RawPrice);

        logger.LogInformation("TEST: serialization OK: {Ok}, ApplyAdjustment: {Adj}", tick2 is not null, adjusted);

        // Resolve StockProcessor from DI (safe: doesn't open network connections)
        using var scope = app.Services.CreateScope();
        var processor = scope.ServiceProvider.GetService<StockProcessor>();
        // Check RabbitMQ availability using the publisher health check (non-blocking
        // from a user perspective, but synchronous here so it completes before Run())
        var publisher = scope.ServiceProvider.GetService<StockQueuePublisher>();
        if (publisher is not null)
        {
            try
            {
                var available = publisher.IsRabbitMqAvailableAsync(TimeSpan.FromSeconds(3)).GetAwaiter().GetResult();
                if (!available)
                {
                    logger.LogWarning("RabbitMQ broker not reachable. Start RabbitMQ (e.g., docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management) and restart the app.");
                }
                else
                {
                    logger.LogInformation("RabbitMQ broker appears reachable.");
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Unexpected error while performing RabbitMQ availability check");
            }
        }
        if (processor is null)
        {
            logger.LogWarning("TEST: StockProcessor could not be resolved from DI");
        }
        else
        {
            logger.LogInformation("TEST: StockProcessor resolved from DI");
        }
    }
    catch (Exception ex)
    {
        var logger2 = app.Services.GetRequiredService<ILogger<Program>>();
        logger2.LogError(ex, "TEST region encountered an exception");
    }
}

app.Run();
