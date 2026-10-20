using BrewQueue.Api.Contracts;
using BrewQueue.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;

namespace BrewQueue.Api.Services;

public sealed class StatsService(BrewQueueDbContext db, TimeProvider clock)
{
    public async Task<StatsSummaryDto> GetSummaryAsync(int hours, CancellationToken ct)
    {
        var since = clock.GetUtcNow().AddHours(-hours);

        var hourly = await db.HourlyOrderStats.AsNoTracking()
            .Where(h => h.HourStart >= since)
            .OrderBy(h => h.HourStart)
            .Select(h => new HourlyStatDto(h.HourStart, h.OrdersPlaced, h.OrdersCollected, h.Revenue))
            .ToListAsync(ct);

        var topItems = await db.ItemPopularity.AsNoTracking()
            .OrderByDescending(i => i.QuantitySold)
            .Take(5)
            .Select(i => new TopItemDto(i.MenuItemId, i.Name, i.QuantitySold))
            .ToListAsync(ct);

        // EF translates Sum to COALESCE(SUM(...), 0), so an empty table gives 0, not an error.
        var totalOrders = await db.HourlyOrderStats.SumAsync(h => h.OrdersPlaced, ct);
        var totalRevenue = await db.HourlyOrderStats.SumAsync(h => h.Revenue, ct);

        var liveByStatus = await db.Orders.AsNoTracking()
            .GroupBy(o => o.Status)
            .Select(g => new { Status = g.Key, Count = g.Count() })
            .ToDictionaryAsync(x => x.Status.ToString(), x => x.Count, ct);

        return new StatsSummaryDto(totalOrders, totalRevenue, hourly, topItems, liveByStatus);
    }
}
