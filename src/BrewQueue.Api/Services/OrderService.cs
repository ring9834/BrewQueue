using System.Text.Json;
using BrewQueue.Api.Contracts;
using BrewQueue.Api.CrossCutting;
using BrewQueue.Contracts;
using BrewQueue.Domain;
using BrewQueue.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;

namespace BrewQueue.Api.Services;

public sealed class OrderService(
    BrewQueueDbContext db,
    TimeProvider clock,
    ICorrelationIdAccessor correlation,
    ILogger<OrderService> logger)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public async Task<OrderDto> PlaceAsync(PlaceOrderRequest request, CancellationToken ct)
    {
        var ids = request.Lines.Select(l => l.MenuItemId).Distinct().ToList();
        var menu = await db.MenuItems.Where(m => ids.Contains(m.Id)).ToDictionaryAsync(m => m.Id, ct);

        var missing = ids.Where(id => !menu.ContainsKey(id)).ToList();
        if (missing.Count > 0)
            throw new DomainException("menu_item_not_found", $"Unknown menu item(s): {string.Join(", ", missing)}.");

        var unavailable = menu.Values.Where(m => !m.IsAvailable).Select(m => m.Name).ToList();
        if (unavailable.Count > 0)
            throw new DomainException("menu_item_unavailable", $"Currently unavailable: {string.Join(", ", unavailable)}.");

        var now = clock.GetUtcNow();
        var order = Order.Place(
            request.CustomerName,
            request.Lines.Select(l => new OrderLine(l.MenuItemId, menu[l.MenuItemId].Name, menu[l.MenuItemId].Price, l.Quantity)),
            now);

        db.Orders.Add(order);

        // Both messages are saved in the same transaction as the order (transactional outbox).
        AddOutbox(OutboxDestination.RabbitMq, nameof(PrepareOrderCommand), order.Id, new PrepareOrderCommand(
            order.Id,
            order.CustomerName,
            order.Lines.Select(l => new PrepareOrderLine(l.Name, l.Quantity)).ToList(),
            now), now);

        AddOutbox(OutboxDestination.Kafka, OrderEventTypes.OrderPlaced, order.Id,
            ToEvent(order, OrderEventTypes.OrderPlaced, previous: null, now), now);

        await db.SaveChangesAsync(ct);

        logger.LogInformation("Order {OrderId} placed by {CustomerName}: {LineCount} lines, total {Total}",
            order.Id, order.CustomerName, order.Lines.Count, order.Total);

        return order.ToDto();
    }

    public async Task<OrderDto> ChangeStatusAsync(Guid id, OrderStatus next, CancellationToken ct)
    {
        var order = await db.Orders.Include(o => o.Lines).FirstOrDefaultAsync(o => o.Id == id, ct)
            ?? throw new NotFoundException("Order", id);

        var previous = order.Status;
        var now = clock.GetUtcNow();

        if (!order.ChangeStatus(next, now))
        {
            logger.LogInformation("Order {OrderId} already {Status}; nothing to do", id, next);
            return order.ToDto();
        }

        AddOutbox(OutboxDestination.Kafka, OrderEventTypes.OrderStatusChanged, order.Id,
            ToEvent(order, OrderEventTypes.OrderStatusChanged, previous, now), now);

        try
        {
            await db.SaveChangesAsync(ct);
        }
        catch (DbUpdateConcurrencyException)
        {
            // Someone else changed the order between our read and write (xmin changed).
            throw new ConflictException($"Order {id} was modified concurrently. Reload and try again.");
        }

        logger.LogInformation("Order {OrderId} moved from {PreviousStatus} to {Status}", id, previous, next);
        return order.ToDto();
    }

    public async Task<OrderDto> GetAsync(Guid id, CancellationToken ct)
    {
        var order = await db.Orders.AsNoTracking().Include(o => o.Lines).FirstOrDefaultAsync(o => o.Id == id, ct)
            ?? throw new NotFoundException("Order", id);
        return order.ToDto();
    }

    public async Task<PagedResult<OrderDto>> ListAsync(OrderStatus[]? statuses, int page, int pageSize, CancellationToken ct)
    {
        var query = db.Orders.AsNoTracking();
        if (statuses is { Length: > 0 })
            query = query.Where(o => statuses.Contains(o.Status));

        var total = await query.CountAsync(ct);
        var orders = await query
            .OrderBy(o => o.CreatedAt)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .Include(o => o.Lines)
            .ToListAsync(ct);

        return new PagedResult<OrderDto>(orders.Select(o => o.ToDto()).ToList(), page, pageSize, total);
    }

    private void AddOutbox<T>(OutboxDestination destination, string type, Guid orderId, T payload, DateTimeOffset now) =>
        db.OutboxMessages.Add(new OutboxMessage
        {
            Destination = destination,
            Type = type,
            Key = orderId.ToString(),
            Payload = JsonSerializer.Serialize(payload, Json),
            CorrelationId = correlation.CorrelationId,
            OccurredAt = now
        });

    private static OrderEvent ToEvent(Order order, string type, OrderStatus? previous, DateTimeOffset now) => new(
        Guid.CreateVersion7(now),
        type,
        order.Id,
        order.Status.ToString(),
        previous?.ToString(),
        order.Total,
        order.Lines.Select(l => new OrderEventLine(l.MenuItemId, l.Name, l.Quantity, l.UnitPrice)).ToList(),
        now);
}
