using Proto = BrewQueue.Contracts.Events.V1;

namespace BrewQueue.Contracts;

/// <summary>
/// Converts between <see cref="OrderEvent"/> (what the outbox stores and the projector consumes)
/// and the Protobuf wire contract in Protos/order_event.proto (what goes over Kafka).
/// Keeping the two separate means the wire format can evolve under Schema Registry's rules
/// without forcing changes through the rest of the code.
/// </summary>
public static class OrderEventMapper
{
    public static Proto.OrderEvent ToProto(OrderEvent evt)
    {
        var message = new Proto.OrderEvent
        {
            EventId = evt.EventId.ToString(),
            EventType = evt.EventType switch
            {
                OrderEventTypes.OrderPlaced => Proto.OrderEventType.Placed,
                OrderEventTypes.OrderStatusChanged => Proto.OrderEventType.StatusChanged,
                _ => throw new ArgumentException($"Unknown event type '{evt.EventType}'.", nameof(evt))
            },
            OrderId = evt.OrderId.ToString(),
            Status = ParseStatus(evt.Status),
            PreviousStatus = evt.PreviousStatus is null ? Proto.OrderStatus.Unspecified : ParseStatus(evt.PreviousStatus),
            TotalCents = ToCents(evt.Total),
            OccurredAtUnixMs = evt.OccurredAt.ToUnixTimeMilliseconds()
        };

        foreach (var line in evt.Lines)
        {
            message.Lines.Add(new Proto.OrderLine
            {
                MenuItemId = line.MenuItemId,
                Name = line.Name,
                Quantity = line.Quantity,
                UnitPriceCents = ToCents(line.UnitPrice)
            });
        }

        return message;
    }

    /// <summary>
    /// Converts a received message back to <see cref="OrderEvent"/>. Throws
    /// <see cref="InvalidEventException"/> when required fields are missing or malformed, so the
    /// consumer can dead-letter the message instead of projecting garbage.
    /// </summary>
    public static OrderEvent FromProto(Proto.OrderEvent message)
    {
        if (!Guid.TryParse(message.EventId, out var eventId) || eventId == Guid.Empty)
            throw new InvalidEventException($"event_id '{message.EventId}' is not a valid UUID.");
        if (!Guid.TryParse(message.OrderId, out var orderId) || orderId == Guid.Empty)
            throw new InvalidEventException($"order_id '{message.OrderId}' is not a valid UUID.");
        if (message.Status == Proto.OrderStatus.Unspecified)
            throw new InvalidEventException("status is missing.");
        if (message.OccurredAtUnixMs <= 0)
            throw new InvalidEventException("occurred_at_unix_ms is missing.");

        var eventType = message.EventType switch
        {
            Proto.OrderEventType.Placed => OrderEventTypes.OrderPlaced,
            Proto.OrderEventType.StatusChanged => OrderEventTypes.OrderStatusChanged,
            _ => throw new InvalidEventException($"event_type {message.EventType} is not supported.")
        };

        return new OrderEvent(
            eventId,
            eventType,
            orderId,
            message.Status.ToString(),
            message.PreviousStatus == Proto.OrderStatus.Unspecified ? null : message.PreviousStatus.ToString(),
            FromCents(message.TotalCents),
            message.Lines
                .Select(l => new OrderEventLine(l.MenuItemId, l.Name, l.Quantity, FromCents(l.UnitPriceCents)))
                .ToList(),
            DateTimeOffset.FromUnixTimeMilliseconds(message.OccurredAtUnixMs));
    }

    // The domain's status names ("Received", "Collected"…) match the generated C# enum names.
    private static Proto.OrderStatus ParseStatus(string status) =>
        Enum.TryParse<Proto.OrderStatus>(status, ignoreCase: false, out var parsed) && parsed != Proto.OrderStatus.Unspecified
            ? parsed
            : throw new ArgumentException($"Unknown order status '{status}'.", nameof(status));

    public static long ToCents(decimal amount) => (long)decimal.Round(amount * 100m, 0, MidpointRounding.AwayFromZero);

    public static decimal FromCents(long cents) => cents / 100m;
}

/// <summary>A message that deserialized but doesn't satisfy the contract. Never retried.</summary>
public sealed class InvalidEventException(string message) : Exception(message);
