namespace BrewQueue.Infrastructure.Persistence;

public enum OutboxDestination
{
    RabbitMq,
    Kafka
}

/// <summary>
/// Transactional outbox row. Written in the SAME database transaction as the business change,
/// then published to the broker by a background process. This avoids the "saved to DB but
/// the broker call failed" (or vice versa) dual-write problem.
/// </summary>
public sealed class OutboxMessage
{
    public Guid Id { get; set; } = Guid.CreateVersion7();
    public OutboxDestination Destination { get; set; }

    /// <summary>Message type name, e.g. "PrepareOrderCommand" or "OrderPlaced".</summary>
    public required string Type { get; set; }

    /// <summary>Kafka message key / RabbitMQ message id. We use the order id.</summary>
    public required string Key { get; set; }

    /// <summary>JSON payload.</summary>
    public required string Payload { get; set; }

    public string? CorrelationId { get; set; }
    public DateTimeOffset OccurredAt { get; set; }
    public DateTimeOffset? ProcessedAt { get; set; }
    public int Attempts { get; set; }
    public string? LastError { get; set; }
}

/// <summary>Read model built by the stats worker from Kafka: orders per hour.</summary>
public sealed class HourlyOrderStat
{
    public DateTimeOffset HourStart { get; set; }
    public int OrdersPlaced { get; set; }
    public int OrdersCollected { get; set; }
    public decimal Revenue { get; set; }
}

/// <summary>Read model built by the stats worker from Kafka: how many of each item sold.</summary>
public sealed class ItemPopularity
{
    public int MenuItemId { get; set; }
    public required string Name { get; set; }
    public int QuantitySold { get; set; }
}

/// <summary>
/// Kafka delivers at-least-once, so the same event can arrive twice. Recording processed event
/// ids in the same transaction as the projection update makes the consumer idempotent.
/// </summary>
public sealed class ProcessedEvent
{
    public Guid EventId { get; set; }
    public DateTimeOffset ProcessedAt { get; set; }
}
