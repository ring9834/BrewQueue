using BrewQueue.Contracts;
using BrewQueue.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace BrewQueue.Infrastructure.Stats;

/// <summary>
/// Applies Kafka order events to the stats read model. Idempotent: an event that was already
/// processed is skipped, and the "processed" marker is saved in the same transaction as the
/// projection change, so a crash can never leave one without the other.
/// </summary>
public sealed class OrderStatsProjector(BrewQueueDbContext db, ILogger<OrderStatsProjector> logger)
{
    /// <returns>True if the event changed the read model, false if it was a duplicate or irrelevant.</returns>
    public async Task<bool> ApplyAsync(OrderEvent evt, CancellationToken ct)
    {
        if (await db.ProcessedEvents.AnyAsync(e => e.EventId == evt.EventId, ct))
        {
            logger.LogInformation("Skipping duplicate event {EventId}", evt.EventId);
            return false;
        }

        var hourStart = TruncateToHour(evt.OccurredAt);
        var changed = false;

        switch (evt.EventType)
        {
            case OrderEventTypes.OrderPlaced:
            {
                var hour = await GetOrAddHourAsync(hourStart, ct);
                hour.OrdersPlaced++;
                hour.Revenue += evt.Total;

                foreach (var line in evt.Lines)
                {
                    var item = await db.ItemPopularity.FindAsync([line.MenuItemId], ct);
                    if (item is null)
                    {
                        item = new ItemPopularity { MenuItemId = line.MenuItemId, Name = line.Name };
                        db.ItemPopularity.Add(item);
                    }
                    item.Name = line.Name;
                    item.QuantitySold += line.Quantity;
                }
                changed = true;
                break;
            }
            case OrderEventTypes.OrderStatusChanged when evt.Status == "Collected":
            {
                var hour = await GetOrAddHourAsync(hourStart, ct);
                hour.OrdersCollected++;
                changed = true;
                break;
            }
        }

        db.ProcessedEvents.Add(new ProcessedEvent { EventId = evt.EventId, ProcessedAt = DateTimeOffset.UtcNow });
        await db.SaveChangesAsync(ct); // one SaveChanges = one transaction
        return changed;
    }

    private async Task<HourlyOrderStat> GetOrAddHourAsync(DateTimeOffset hourStart, CancellationToken ct)
    {
        var hour = await db.HourlyOrderStats.FindAsync([hourStart], ct);
        if (hour is not null) return hour;

        hour = new HourlyOrderStat { HourStart = hourStart };
        db.HourlyOrderStats.Add(hour);
        return hour;
    }

    public static DateTimeOffset TruncateToHour(DateTimeOffset value)
    {
        var utc = value.ToUniversalTime();
        return new DateTimeOffset(utc.Year, utc.Month, utc.Day, utc.Hour, 0, 0, TimeSpan.Zero);
    }
}
