using BrewQueue.Contracts;
using Proto = BrewQueue.Contracts.Events.V1;

namespace BrewQueue.UnitTests.Infrastructure;

public class OrderEventMapperTests
{
    private static OrderEvent Sample(string type = OrderEventTypes.OrderPlaced, string status = "Received", string? previous = null) => new(
        Guid.NewGuid(), type, Guid.NewGuid(), status, previous, 15.50m,
        [new OrderEventLine(1, "Flat White", 2, 5.50m), new OrderEventLine(7, "Cheese Scone", 1, 4.50m)],
        new DateTimeOffset(2026, 10, 24, 5, 30, 15, 123, TimeSpan.Zero));

    [Fact]
    public void Round_trips_every_field()
    {
        var original = Sample();

        var back = OrderEventMapper.FromProto(OrderEventMapper.ToProto(original));

        Assert.Equal(original.EventId, back.EventId);
        Assert.Equal(original.EventType, back.EventType);
        Assert.Equal(original.OrderId, back.OrderId);
        Assert.Equal(original.Status, back.Status);
        Assert.Null(back.PreviousStatus);
        Assert.Equal(original.Total, back.Total);
        Assert.Equal(original.OccurredAt, back.OccurredAt);
        Assert.Equal(original.Lines, back.Lines);
    }

    [Fact]
    public void Round_trips_a_status_change()
    {
        var original = Sample(OrderEventTypes.OrderStatusChanged, "Collected", "Ready");

        var back = OrderEventMapper.FromProto(OrderEventMapper.ToProto(original));

        Assert.Equal(OrderEventTypes.OrderStatusChanged, back.EventType);
        Assert.Equal("Collected", back.Status);
        Assert.Equal("Ready", back.PreviousStatus);
    }

    [Fact]
    public void Money_is_sent_as_whole_cents()
    {
        var proto = OrderEventMapper.ToProto(Sample());

        Assert.Equal(1550, proto.TotalCents);
        Assert.Equal(550, proto.Lines[0].UnitPriceCents);
    }

    [Theory]
    [InlineData(0.005, 1)]   // rounds half away from zero, never truncates
    [InlineData(0.004, 0)]
    [InlineData(12.345, 1235)]
    public void ToCents_rounds(decimal amount, long expected) =>
        Assert.Equal(expected, OrderEventMapper.ToCents(amount));

    [Fact]
    public void Previous_status_none_is_sent_as_unspecified()
    {
        Assert.Equal(Proto.OrderStatus.Unspecified, OrderEventMapper.ToProto(Sample()).PreviousStatus);
    }

    public static TheoryData<string, Action<Proto.OrderEvent>> InvalidMessages => new()
    {
        { "bad event id", m => m.EventId = "not-a-guid" },
        { "empty event id", m => m.EventId = Guid.Empty.ToString() },
        { "bad order id", m => m.OrderId = "" },
        { "no status", m => m.Status = Proto.OrderStatus.Unspecified },
        { "no event type", m => m.EventType = Proto.OrderEventType.Unspecified },
        { "no timestamp", m => m.OccurredAtUnixMs = 0 }
    };

    [Theory]
    [MemberData(nameof(InvalidMessages))]
    public void FromProto_rejects_invalid_messages(string _, Action<Proto.OrderEvent> corrupt)
    {
        var message = OrderEventMapper.ToProto(Sample());
        corrupt(message);

        Assert.Throws<InvalidEventException>(() => OrderEventMapper.FromProto(message));
    }

    [Fact]
    public void ToProto_rejects_unknown_status()
    {
        Assert.Throws<ArgumentException>(() => OrderEventMapper.ToProto(Sample(status: "Teleported")));
    }
}
