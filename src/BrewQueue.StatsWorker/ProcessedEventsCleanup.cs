using BrewQueue.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;

namespace BrewQueue.StatsWorker;

/// <summary>
/// Deletes old rows from processed_events so the dedupe table doesn't grow forever.
/// Safe as long as the retention here is longer than the topic's retention (14 days): an event
/// older than that can't be redelivered, so its id is no longer needed.
/// </summary>
public sealed class ProcessedEventsCleanup(
    IServiceScopeFactory scopes,
    TimeProvider clock,
    IConfiguration config,
    ILogger<ProcessedEventsCleanup> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var retention = config.GetValue("Stats:ProcessedEventRetention", TimeSpan.FromDays(30));
        using var timer = new PeriodicTimer(TimeSpan.FromHours(1));

        do
        {
            try
            {
                using var scope = scopes.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<BrewQueueDbContext>();
                var cutoff = clock.GetUtcNow() - retention;

                int deleted, total = 0;
                do
                {
                    deleted = await db.Database.ExecuteSqlAsync($"""
                        DELETE FROM processed_events
                        WHERE "EventId" IN (SELECT "EventId" FROM processed_events WHERE "ProcessedAt" < {cutoff} LIMIT 1000)
                        """, stoppingToken);
                    total += deleted;
                } while (deleted > 0);

                if (total > 0) logger.LogInformation("Pruned {Count} processed event ids older than {Cutoff}", total, cutoff);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Pruning processed_events failed; will try again next hour");
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }
}
