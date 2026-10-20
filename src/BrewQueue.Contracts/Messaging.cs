namespace BrewQueue.Contracts;

/// <summary>Names of RabbitMQ exchanges/queues and Kafka topics, in one place.</summary>
public static class MessagingNames
{
    // RabbitMQ: a work queue for the kitchen (a command, handled once by one worker).
    public const string KitchenExchange = "brewqueue.kitchen";
    public const string KitchenQueue = "kitchen.orders";
    public const string KitchenRoutingKey = "order.prepare";
    public const string KitchenDeadLetterExchange = "brewqueue.kitchen.dlx";
    public const string KitchenDeadLetterQueue = "kitchen.orders.dlq";

    // Kafka: a durable stream of facts that many consumers can read and replay.
    public const string OrderEventsTopic = "order-events";

    // Header names used to carry metadata across process boundaries.
    public const string CorrelationIdHeader = "x-correlation-id";
    public const string RetryCountHeader = "x-retry-count";
    public const string MessageTypeHeader = "x-message-type";
}

/// <summary>
/// RabbitMQ command: "please prepare this order".
/// </summary>
public sealed record PrepareOrderCommand(
    Guid OrderId,
    string CustomerName,
    IReadOnlyList<PrepareOrderLine> Lines,
    DateTimeOffset PlacedAt);

public sealed record PrepareOrderLine(string Name, int Quantity);

/// <summary>
/// Kafka event types published to <see cref="MessagingNames.OrderEventsTopic"/>.
/// </summary>
public static class OrderEventTypes
{
    public const string OrderPlaced = "OrderPlaced";
    public const string OrderStatusChanged = "OrderStatusChanged";
}

/// <summary>
/// Kafka event envelope. Keyed by OrderId so all events for one order land on the same
/// partition and are therefore consumed in order.
/// </summary>
public sealed record OrderEvent(
    Guid EventId,
    string EventType,
    Guid OrderId,
    string Status,
    string? PreviousStatus,
    decimal Total,
    IReadOnlyList<OrderEventLine> Lines,
    DateTimeOffset OccurredAt);

public sealed record OrderEventLine(int MenuItemId, string Name, int Quantity, decimal UnitPrice);
