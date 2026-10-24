namespace BrewQueue.Domain;

/// <summary>
/// Base type for business-rule violations. The API maps these to 422 Unprocessable Entity
/// ProblemDetails, using <see cref="ErrorCode"/> as a machine-readable code for clients.
/// </summary>
public class DomainException(string errorCode, string message) : Exception(message)
{
    public string ErrorCode { get; } = errorCode;
}

public sealed class InvalidStatusTransitionException(OrderStatus from, OrderStatus to)
    : DomainException("invalid_status_transition", $"An order cannot move from {from} to {to}.")
{
    public OrderStatus From { get; } = from;
    public OrderStatus To { get; } = to;
}

/// <summary>Maps to 404 Not Found.</summary>
public sealed class NotFoundException(string resource, object key)
    : Exception($"{resource} '{key}' was not found.")
{
    public string Resource { get; } = resource;
    public object Key { get; } = key;
}

/// <summary>Maps to 409 Conflict (e.g. optimistic concurrency failures).</summary>
public sealed class ConflictException(string message) : Exception(message);
