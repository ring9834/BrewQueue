using System.Net.Http.Json;
using BrewQueue.Api.Contracts;
using BrewQueue.Api.Services;
using BrewQueue.Infrastructure.Persistence;
using BrewQueue.IntegrationTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using static BrewQueue.IntegrationTests.Infrastructure.TestHelpers;

namespace BrewQueue.IntegrationTests;

/// <summary>
/// The outbox must have a single active publisher so events reach Kafka in the order they were
/// written. These tests take the publisher's advisory lock from "another instance" (a separate
/// database connection) and check that this instance stops publishing while it's held.
/// </summary>
[Collection(ApiCollection.Name)]
public class OutboxPublisherTests(ApiFactory factory)
{
    private readonly HttpClient _client = factory.CreateClient();

    [Fact]
    public async Task Only_the_instance_holding_the_lock_publishes()
    {
        await using var otherInstance = new NpgsqlConnection(factory.Postgres.GetConnectionString());
        await otherInstance.OpenAsync();
        await using var otherTx = await otherInstance.BeginTransactionAsync();
        await using (var cmd = new NpgsqlCommand($"SELECT pg_advisory_xact_lock({OutboxPublisher.PublisherLockKey})", otherInstance, otherTx))
            await cmd.ExecuteNonQueryAsync();

        var order = await PlaceAsync();

        // This instance's publisher can't get the lock: it reports so and publishes nothing.
        var publisher = factory.Services.GetRequiredService<OutboxPublisher>();
        var result = await publisher.PublishBatchAsync(CancellationToken.None);
        Assert.False(result.HadLock);

        await Task.Delay(TimeSpan.FromSeconds(3)); // the background loop is blocked too
        Assert.Equal(0, await CountPublishedAsync(order.Id));

        // "The other instance" goes away (or crashes): the lock is released with its transaction.
        await otherTx.RollbackAsync();

        await EventuallyAsync(async () => await CountPublishedAsync(order.Id) > 0 ? "published" : null,
            TimeSpan.FromSeconds(30), "outbox messages published after the lock was released");
    }

    [Fact]
    public async Task Messages_are_published_in_sequence_order()
    {
        var order = await PlaceAsync();
        foreach (var status in new[] { "Preparing", "Ready", "Collected" })
        {
            var r = await _client.PatchAsJsonAsync($"/api/orders/{order.Id}/status", new { status }, ApiFactory.Json);
            r.EnsureSuccessStatusCode();
        }

        await EventuallyAsync(async () => await CountPublishedAsync(order.Id) == 5 ? "all" : null,
            TimeSpan.FromSeconds(30), "all 5 outbox messages published (1 RabbitMQ + 4 Kafka)");

        using var scope = factory.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<BrewQueueDbContext>();
        var rows = await db.OutboxMessages.AsNoTracking()
            .Where(m => m.Key == order.Id.ToString())
            .OrderBy(m => m.Sequence)
            .ToListAsync();

        // Published timestamps never go backwards along the sequence.
        var published = rows.Select(m => m.ProcessedAt!.Value).ToList();
        Assert.Equal(published.OrderBy(t => t), published);
    }

    private async Task<OrderDto> PlaceAsync()
    {
        var response = await _client.SendAsync(PlaceOrderRequest(OrderBody(), Guid.NewGuid()));
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<OrderDto>(ApiFactory.Json))!;
    }

    private async Task<int> CountPublishedAsync(Guid orderId)
    {
        using var scope = factory.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<BrewQueueDbContext>();
        return await db.OutboxMessages.CountAsync(m => m.Key == orderId.ToString() && m.ProcessedAt != null);
    }
}
