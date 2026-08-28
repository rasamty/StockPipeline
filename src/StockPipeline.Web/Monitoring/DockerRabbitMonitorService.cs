using System.Diagnostics;
using Microsoft.AspNetCore.SignalR;
using StockPipeline.Web.Hubs;
using StockPipeline.Web.Messaging;

namespace StockPipeline.Web.Monitoring;

// Monitors Docker for RabbitMQ containers and checks RabbitMQ broker reachability.
// Sends updates to connected SignalR clients (StockHub) when state changes.
public class DockerRabbitMonitorService : BackgroundService
{
    private readonly IHubContext<StockHub> _hubContext;
    private readonly StockQueuePublisher _publisher;
    private readonly ILogger<DockerRabbitMonitorService> _logger;
    private readonly MonitorStateStore _stateStore;
    private readonly TimeSpan _pollInterval = TimeSpan.FromSeconds(5);

    private bool _lastDockerAvailable = false;
    private bool _lastContainerFound = false;
    private string? _lastContainerId;
    private string? _lastContainerName;
    private bool _lastRabbitReachable = false;

    public DockerRabbitMonitorService(IHubContext<StockHub> hubContext, StockQueuePublisher publisher, ILogger<DockerRabbitMonitorService> logger, MonitorStateStore stateStore)
    {
        _hubContext = hubContext;
        _publisher = publisher;
        _logger = logger;
        _stateStore = stateStore;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("DockerRabbitMonitorService starting");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var dockerAvailable = true;
                var containerFound = false;
                string? containerId = null;
                string? containerName = null;
                string? containerImage = null;
                string? containerStatus = null;

                // Try to run `docker ps` to discover containers. If docker CLI is not
                // available or fails, consider docker not available.
                try
                {
                    var psi = new ProcessStartInfo("docker", "ps --format \"{{.ID}}|{{.Names}}|{{.Image}}|{{.Status}}\"")
                    {
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        UseShellExecute = false,
                        CreateNoWindow = true
                    };

                    using var proc = Process.Start(psi);
                    if (proc is null)
                    {
                        dockerAvailable = false;
                    }
                    else
                    {
                        var output = await proc.StandardOutput.ReadToEndAsync();
                        var err = await proc.StandardError.ReadToEndAsync();
                        await proc.WaitForExitAsync(stoppingToken);

                        if (!string.IsNullOrWhiteSpace(err) && string.IsNullOrWhiteSpace(output))
                        {
                            dockerAvailable = false;
                            _logger.LogDebug("docker ps returned error: {Error}", err.Trim());
                        }
                        else
                        {
                            // Parse lines looking for a rabbitmq image or container name
                            var lines = output.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                            foreach (var line in lines)
                            {
                                // format: id|name|image|status
                                var parts = line.Split('|');
                                if (parts.Length >= 4)
                                {
                                    var id = parts[0];
                                    var name = parts[1];
                                    var image = parts[2];
                                    var status = parts[3];

                                    if (image.IndexOf("rabbitmq", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                        name.IndexOf("rabbitmq", StringComparison.OrdinalIgnoreCase) >= 0)
                                    {
                                        containerFound = true;
                                        containerId = id;
                                        containerName = name;
                                        containerImage = image;
                                        containerStatus = status;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    dockerAvailable = false;
                    _logger.LogDebug(ex, "docker ps failed");
                }

                // Check RabbitMQ broker reachability using publisher health check
                var rabbitReachable = await _publisher.IsRabbitMqAvailableAsync(TimeSpan.FromSeconds(3));

                // If anything changed since last poll, notify connected clients
                var changed = dockerAvailable != _lastDockerAvailable ||
                              containerFound != _lastContainerFound ||
                              containerId != _lastContainerId ||
                              containerName != _lastContainerName ||
                              rabbitReachable != _lastRabbitReachable;

                if (changed)
                {
                    _lastDockerAvailable = dockerAvailable;
                    _lastContainerFound = containerFound;
                    _lastContainerId = containerId;
                    _lastContainerName = containerName;
                    _lastRabbitReachable = rabbitReachable;

                    var payload = new
                    {
                        DockerAvailable = dockerAvailable,
                        RabbitContainerFound = containerFound,
                        ContainerId = containerId,
                        ContainerName = containerName,
                        ContainerImage = containerImage,
                        ContainerStatus = containerStatus,
                        RabbitMqReachable = rabbitReachable,
                        TimestampUtc = DateTimeOffset.UtcNow
                    };

                    _logger.LogInformation("Monitor update: {@Payload}", payload);
                    // Update shared state store so HTTP clients can fetch current status
                    try
                    {
                        _stateStore.SetPayload(payload);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "Failed to set monitor state in store");
                    }

                    // Send to all connected clients. Client should implement
                    // ReceiveMonitorUpdate(payload) to show a dialog.
                    await _hubContext.Clients.All.SendAsync("ReceiveMonitorUpdate", payload, cancellationToken: stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // shutting down
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in DockerRabbitMonitorService loop");
            }

            await Task.Delay(_pollInterval, stoppingToken);
        }

        _logger.LogInformation("DockerRabbitMonitorService stopping");
    }
}
