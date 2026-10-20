using BrewQueue.Domain;

namespace BrewQueue.UnitTests.Domain;

public class OrderTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 17, 9, 0, 0, TimeSpan.Zero);

    private static Order NewOrder() =>
        Order.Place("Aroha", [new OrderLine(1, "Flat White", 5.50m, 2), new OrderLine(7, "Cheese Scone", 4.50m, 1)], Now);

    [Fact]
    public void Place_sets_initial_state_and_total()
    {
        var order = NewOrder();

        Assert.Equal(OrderStatus.Received, order.Status);
        Assert.Equal("Aroha", order.CustomerName);
        Assert.Equal(15.50m, order.Total);
        Assert.Equal(Now, order.CreatedAt);
        Assert.NotEqual(Guid.Empty, order.Id);
    }

    [Fact]
    public void Place_merges_duplicate_menu_items()
    {
        var order = Order.Place("Aroha",
            [new OrderLine(1, "Flat White", 5.50m, 1), new OrderLine(1, "Flat White", 5.50m, 2)], Now);

        var line = Assert.Single(order.Lines);
        Assert.Equal(3, line.Quantity);
        Assert.Equal(16.50m, order.Total);
    }

    [Fact]
    public void Place_trims_customer_name()
    {
        var order = Order.Place("  Aroha  ", [new OrderLine(1, "Flat White", 5.50m, 1)], Now);
        Assert.Equal("Aroha", order.CustomerName);
    }

    [Fact]
    public void Place_rejects_empty_order()
    {
        var ex = Assert.Throws<DomainException>(() => Order.Place("Aroha", [], Now));
        Assert.Equal("order_empty", ex.ErrorCode);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Place_rejects_blank_customer_name(string name)
    {
        var ex = Assert.Throws<DomainException>(() => Order.Place(name, [new OrderLine(1, "Flat White", 5.50m, 1)], Now));
        Assert.Equal("customer_name_required", ex.ErrorCode);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(21)]
    public void OrderLine_rejects_invalid_quantity(int quantity)
    {
        var ex = Assert.Throws<DomainException>(() => new OrderLine(1, "Flat White", 5.50m, quantity));
        Assert.Equal("invalid_quantity", ex.ErrorCode);
    }

    [Fact]
    public void Happy_path_through_all_statuses()
    {
        var order = NewOrder();

        Assert.True(order.ChangeStatus(OrderStatus.Preparing, Now.AddMinutes(1)));
        Assert.True(order.ChangeStatus(OrderStatus.Ready, Now.AddMinutes(2)));
        Assert.True(order.ChangeStatus(OrderStatus.Collected, Now.AddMinutes(3)));

        Assert.Equal(OrderStatus.Collected, order.Status);
        Assert.Equal(Now.AddMinutes(3), order.UpdatedAt);
    }

    [Fact]
    public void Changing_to_the_current_status_is_a_no_op()
    {
        var order = NewOrder();
        order.ChangeStatus(OrderStatus.Preparing, Now.AddMinutes(1));

        var changed = order.ChangeStatus(OrderStatus.Preparing, Now.AddMinutes(5));

        Assert.False(changed);
        Assert.Equal(Now.AddMinutes(1), order.UpdatedAt);
    }

    [Theory]
    [InlineData(OrderStatus.Received, OrderStatus.Ready)]
    [InlineData(OrderStatus.Received, OrderStatus.Collected)]
    [InlineData(OrderStatus.Preparing, OrderStatus.Received)]
    [InlineData(OrderStatus.Preparing, OrderStatus.Cancelled)]
    [InlineData(OrderStatus.Ready, OrderStatus.Preparing)]
    public void Invalid_transitions_throw(OrderStatus from, OrderStatus to)
    {
        var order = NewOrder();
        MoveTo(order, from);

        var ex = Assert.Throws<InvalidStatusTransitionException>(() => order.ChangeStatus(to, Now));
        Assert.Equal(from, ex.From);
        Assert.Equal(to, ex.To);
        Assert.Equal("invalid_status_transition", ex.ErrorCode);
    }

    [Theory]
    [InlineData(OrderStatus.Collected)]
    [InlineData(OrderStatus.Cancelled)]
    public void Terminal_statuses_have_no_way_out(OrderStatus status)
    {
        Assert.True(OrderStatusRules.IsTerminal(status));
        foreach (var next in Enum.GetValues<OrderStatus>())
            Assert.False(OrderStatusRules.CanTransition(status, next));
    }

    private static void MoveTo(Order order, OrderStatus target)
    {
        OrderStatus[] path = [OrderStatus.Preparing, OrderStatus.Ready, OrderStatus.Collected];
        foreach (var step in path)
        {
            if (order.Status == target) return;
            order.ChangeStatus(step, Now);
        }
    }
}
