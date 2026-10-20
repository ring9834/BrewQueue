using System.ComponentModel.DataAnnotations;
using BrewQueue.Api.Contracts;
using BrewQueue.Api.CrossCutting;
using BrewQueue.Api.Services;
using BrewQueue.Domain;
using Microsoft.AspNetCore.Mvc;

namespace BrewQueue.Api.Controllers;

[ApiController]
[Route("api/orders")]
[Produces("application/json")]
[Tags("Orders")]
public sealed class OrdersController(OrderService orders) : ControllerBase
{
    /// <summary>Places a new order.</summary>
    /// <remarks>
    /// Requires an <c>Idempotency-Key</c> header (UUID). Retrying with the same key and body returns
    /// the original response with an <c>Idempotent-Replayed: true</c> header instead of creating a
    /// second order. Reusing a key with a different body returns 422.
    /// </remarks>
    /// <param name="idempotencyKey">Client-generated UUID, unique per checkout attempt.</param>
    /// <param name="request">The order to place.</param>
    /// <param name="ct">Cancellation token.</param>
    [HttpPost]
    [Idempotent] // It runs after model binding and validation, and before and after the action itself. 'action' here means the PlaceOrder method below. 
    [ProducesResponseType<OrderDto>(StatusCodes.Status201Created)]
    [ProducesResponseType<ValidationProblemDetails>(StatusCodes.Status400BadRequest, "application/problem+json")]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status409Conflict, "application/problem+json")]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status422UnprocessableEntity, "application/problem+json")]
    public async Task<ActionResult<OrderDto>> PlaceOrder(
        [FromHeader(Name = IdempotencyFilter.HeaderName), Required] Guid? idempotencyKey, // A missing key, or one that isn’t a UUID, gets a 400 ValidationProblemDetails from [ApiController]’s automatic validation, before the filter even runs. The filter checks again as a safety net.
        [FromBody] PlaceOrderRequest request,
        CancellationToken ct)
    {
        var order = await orders.PlaceAsync(request, ct);
        // Created(...) (rather than CreatedAtAction) gives the idempotency filter a concrete Location to store.
        // This ensures that subsequent retries with the same idempotency key return the same response.
        return Created($"/api/orders/{order.Id}", order); // 201 Created, with Location header and the order in the body.
    }

    /// <summary>Gets one order.</summary>
    [HttpGet("{id:guid}")]
    [ProducesResponseType<OrderDto>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound, "application/problem+json")]
    public Task<OrderDto> GetOrder(Guid id, CancellationToken ct) => orders.GetAsync(id, ct);

    /// <summary>Lists orders, oldest first. Filter by one or more statuses.</summary>
    /// <param name="status">e.g. <c>?status=Received&amp;status=Preparing</c></param>
    /// <param name="page">1-based page number.</param>
    /// <param name="pageSize">Page size (1–100).</param>
    /// <param name="ct">Cancellation token.</param>
    [HttpGet]
    [ProducesResponseType<PagedResult<OrderDto>>(StatusCodes.Status200OK)]
    [ProducesResponseType<ValidationProblemDetails>(StatusCodes.Status400BadRequest, "application/problem+json")]
    public Task<PagedResult<OrderDto>> ListOrders(
        [FromQuery] OrderStatus[]? status,
        [FromQuery, Range(1, int.MaxValue)] int page = 1,
        [FromQuery, Range(1, 100)] int pageSize = 50,
        CancellationToken ct = default) =>
        orders.ListAsync(status, page, pageSize, ct);

    /// <summary>Moves an order to a new status.</summary>
    /// <remarks>
    /// Allowed: Received → Preparing | Cancelled, Preparing → Ready, Ready → Collected.
    /// Setting the status the order already has is a no-op (safe to retry).
    /// </remarks>
    [HttpPatch("{id:guid}/status")]
    [ProducesResponseType<OrderDto>(StatusCodes.Status200OK)]
    [ProducesResponseType<ValidationProblemDetails>(StatusCodes.Status400BadRequest, "application/problem+json")]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound, "application/problem+json")]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status409Conflict, "application/problem+json")]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status422UnprocessableEntity, "application/problem+json")]
    public Task<OrderDto> ChangeStatus(Guid id, [FromBody] ChangeOrderStatusRequest request, CancellationToken ct) =>
        orders.ChangeStatusAsync(id, request.Status!.Value, ct);
}
