namespace BrewQueue.Domain;

public enum OrderStatus
{
    Received,
    Preparing,
    Ready,
    Collected,
    Cancelled
}

/// <summary>
/// The allowed status transitions, as a single source of truth.
/// </summary>
public static class OrderStatusRules
{
    private static readonly Dictionary<OrderStatus, OrderStatus[]> Allowed = new()
    {
        [OrderStatus.Received] = [OrderStatus.Preparing, OrderStatus.Cancelled],
        [OrderStatus.Preparing] = [OrderStatus.Ready],
        [OrderStatus.Ready] = [OrderStatus.Collected],
        [OrderStatus.Collected] = [],
        [OrderStatus.Cancelled] = []
    };

    public static bool CanTransition(OrderStatus from, OrderStatus to) => Allowed[from].Contains(to);

    public static bool IsTerminal(OrderStatus status) => Allowed[status].Length == 0;
}

/// <summary>
/// Order aggregate root. All state changes go through its methods.
/// </summary>
public sealed class Order
{
    private Order() { } // EF Core
    private readonly List<OrderLine> _lines = [];
    public Guid Id { get; private set; }
    public string CustomerName { get; private set; } = "";
    public OrderStatus Status { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }
    public DateTimeOffset UpdatedAt { get; private set; }
    public IReadOnlyList<OrderLine> Lines => _lines;
    public decimal Total => _lines.Sum(l => l.LineTotal);

    /// <summary>
    /// Concurrency token, mapped to PostgreSQL's xmin system column.
    /// </summary>
    public uint Version { get; private set; }

    public static Order Place(string customerName, IEnumerable<OrderLine> lines, DateTimeOffset now)
    {
        if (string.IsNullOrWhiteSpace(customerName))
            throw new DomainException("customer_name_required", "A customer name is required.");

        var lineList = lines.ToList();
        if (lineList.Count == 0)
            throw new DomainException("order_empty", "An order must contain at least one item.");

        var order = new Order
        {
            Id = Guid.CreateVersion7(now),
            CustomerName = customerName.Trim(),
            Status = OrderStatus.Received,
            CreatedAt = now,
            UpdatedAt = now
        };

        // Merge duplicate menu items into one line.
        foreach (var group in lineList.GroupBy(l => l.MenuItemId))
        {
            var first = group.First();
            order._lines.Add(new OrderLine(first.MenuItemId, first.Name, first.UnitPrice, group.Sum(l => l.Quantity)));
        }

        return order;
    }

    /// <summary>
    /// Moves the order to <paramref name="next"/>. Returns false when the order is already in that
    /// status, which makes the operation safe to retry (the kitchen worker relies on this).
    /// </summary>
    public bool ChangeStatus(OrderStatus next, DateTimeOffset now)
    {
        if (Status == next) return false;

        if (!OrderStatusRules.CanTransition(Status, next))
            throw new InvalidStatusTransitionException(Status, next);

        Status = next;
        UpdatedAt = now;
        return true;
    }
}

public sealed class OrderLine
{
    private OrderLine() { } // EF Core

    public OrderLine(int menuItemId, string name, decimal unitPrice, int quantity)
    {
        if (quantity is < 1 or > 20)
            throw new DomainException("invalid_quantity", "Quantity must be between 1 and 20.");
        if (unitPrice < 0)
            throw new DomainException("invalid_price", "Unit price cannot be negative.");

        MenuItemId = menuItemId;
        Name = name;
        UnitPrice = unitPrice;
        Quantity = quantity;
    }

    public int Id { get; private set; }
    public int MenuItemId { get; private set; }

    /// <summary>
    /// Name and price are snapshotted so later menu changes don't rewrite history.
    /// </summary>
    public string Name { get; private set; } = "";
    public decimal UnitPrice { get; private set; }
    public int Quantity { get; private set; }
    public decimal LineTotal => UnitPrice * Quantity;
}
