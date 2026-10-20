using BrewQueue.Contracts;
using BrewQueue.Infrastructure.Persistence;
using BrewQueue.Infrastructure.Stats;
using BrewQueue.IntegrationTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace BrewQueue.IntegrationTests;

/// <summary>Tests the Kafka read-model projection directly against PostgreSQL.</summary>
[Collection(ApiCollection.Name)]
public class StatsProjectorTests(ApiFactory factory)
{
    [Fact]
    public async Task Duplicate_events_are_applied_only_once()
    {
        // A menu item id that no other test uses, so the assertion is isolated.
        var menuItemId = Random.Shared.Next(100_000, 1_000_000);
        var evt = new OrderEvent(
            Guid.NewGuid(), OrderEventTypes.OrderPlaced, Guid.NewGuid(), "Received", null, 11.00m,
            [new OrderEventLine(menuItemId, "Test Brew", 2, 5.50m)],
            DateTimeOffset.UtcNow);

        Assert.True(await ApplyAsync(evt));
        Assert.False(await ApplyAsync(evt)); // Kafka redelivery: must be skipped

        using var scope = factory.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<BrewQueueDbContext>();
        var item = await db.ItemPopularity.SingleAsync(i => i.MenuItemId == menuItemId);
        Assert.Equal(2, item.QuantitySold);
    }

    [Fact]
    public async Task Collected_status_increments_the_hourly_collected_count()
    {
        // Use an hour far in the past so this test owns the row.
        var hour = new DateTimeOffset(2001, 1, 1, Random.Shared.Next(0, 24), 0, 0, TimeSpan.Zero)
            .AddDays(Random.Shared.Next(0, 3000));
        var collected = new OrderEvent(
            Guid.NewGuid(), OrderEventTypes.OrderStatusChanged, Guid.NewGuid(), "Collected", "Ready", 5m, [],
            hour.AddMinutes(30));

        await ApplyAsync(collected);

        using var scope = factory.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<BrewQueueDbContext>();
        var row = await db.HourlyOrderStats.SingleAsync(h => h.HourStart == hour);
        Assert.Equal(1, row.OrdersCollected);
        Assert.Equal(0, row.OrdersPlaced);
    }

    private async Task<bool> ApplyAsync(OrderEvent evt)
    {
        using var scope = factory.Services.CreateScope();
        var projector = scope.ServiceProvider.GetRequiredService<OrderStatsProjector>();
        return await projector.ApplyAsync(evt, CancellationToken.None);
    }
}
