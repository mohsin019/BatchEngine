using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace BatchEngine;

internal sealed class BatchWorkerService(Store store, WorkerOptions options, ILogger<BatchWorkerService> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await new Worker(store, new PluginRegistry(), options).RunAsync(stoppingToken);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                logger.LogError(ex, "Worker {WorkerId} exited unexpectedly. Restarting in five seconds.", options.Id);
            }

            if (!stoppingToken.IsCancellationRequested)
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }
    }
}
