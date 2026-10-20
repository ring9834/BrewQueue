using System.ComponentModel.DataAnnotations;
using BrewQueue.Domain;

namespace BrewQueue.Api.Contracts;

/// <summary>An item on the café menu.</summary>
public sealed record MenuItemDto(int Id, string Name, string Category, string Description, decimal Price, bool IsAvailable);

/// <summary>Request body for placing an order.</summary>
public sealed class PlaceOrderRequest
{
    /// <summary>Name called out when the order is ready.</summary>
    /// <example>Aroha</example>
    [Required, StringLength(50, MinimumLength = 1)]
    public string CustomerName { get; init; } = "";

    /// <summary>Items to order (1–20 lines).</summary>
    [Required, MinLength(1), MaxLength(20)]
    public List<PlaceOrderLineRequest> Lines { get; init; } = [];
}

public sealed class PlaceOrderLineRequest
{
    /// <example>1</example>
    [Range(1, int.MaxValue)]
    public int MenuItemId { get; init; }

    /// <example>2</example>
    [Range(1, 20)]
    public int Quantity { get; init; }
}

/// <summary>Request body for moving an order to a new status.</summary>
public sealed class ChangeOrderStatusRequest
{
    /// <example>Preparing</example>
    [Required]
    public OrderStatus? Status { get; init; }
}

public sealed record OrderLineDto(int MenuItemId, string Name, decimal UnitPrice, int Quantity, decimal LineTotal);

public sealed record OrderDto(
    Guid Id,
    string CustomerName,
    OrderStatus Status,
    decimal Total,
    IReadOnlyList<OrderLineDto> Lines,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);

public sealed record PagedResult<T>(IReadOnlyList<T> Items, int Page, int PageSize, int TotalCount);

public sealed record HourlyStatDto(DateTimeOffset HourStart, int OrdersPlaced, int OrdersCollected, decimal Revenue);

public sealed record TopItemDto(int MenuItemId, string Name, int QuantitySold);

/// <summary>
/// Stats summary. <c>Hourly</c> and <c>TopItems</c> come from the Kafka-built read model
/// (eventually consistent); <c>LiveByStatus</c> is queried directly from the orders table.
/// </summary>
public sealed record StatsSummaryDto(
    int TotalOrders,
    decimal TotalRevenue,
    IReadOnlyList<HourlyStatDto> Hourly,
    IReadOnlyList<TopItemDto> TopItems,
    IReadOnlyDictionary<string, int> LiveByStatus);

public static class DtoMapping
{
    public static MenuItemDto ToDto(this MenuItem m) =>
        new(m.Id, m.Name, m.Category, m.Description, m.Price, m.IsAvailable);

    public static OrderDto ToDto(this Order o) => new(
        o.Id,
        o.CustomerName,
        o.Status,
        o.Total,
        o.Lines.Select(l => new OrderLineDto(l.MenuItemId, l.Name, l.UnitPrice, l.Quantity, l.LineTotal)).ToList(),
        o.CreatedAt,
        o.UpdatedAt);
}
