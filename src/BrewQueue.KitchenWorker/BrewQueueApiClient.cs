using System.Net;
using System.Net.Http.Json;

namespace BrewQueue.KitchenWorker;

/// <summary>Thrown for failures that will not succeed on retry (e.g. 404, 422).</summary>
public sealed class PermanentFailureException(string message) : Exception(message);

/// <summary>
/// The kitchen changes order status through the API rather than writing to the database
/// directly, so all business rules and outbox events stay in one place.
/// Transient failures (5xx, timeouts) are retried by the standard resilience handler.
/// </summary>
public sealed class BrewQueueApiClient(HttpClient http, ILogger<BrewQueueApiClient> logger)
{
    public async Task SetStatusAsync(Guid orderId, string status, string? correlationId, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Patch, $"api/orders/{orderId}/status")
        {
            Content = JsonContent.Create(new { status })
        };
        if (correlationId is not null) request.Headers.Add("X-Correlation-Id", correlationId);

        using var response = await http.SendAsync(request, ct);
        if (response.IsSuccessStatusCode)
        {
            logger.LogInformation("Order {OrderId} set to {Status}", orderId, status);
            return;
        }

        var problem = await response.Content.ReadAsStringAsync(ct);
        if (response.StatusCode is HttpStatusCode.NotFound or HttpStatusCode.UnprocessableEntity or HttpStatusCode.BadRequest)
            throw new PermanentFailureException($"API rejected status change ({(int)response.StatusCode}): {problem}");

        // Anything else (409, 5xx) is worth retrying later.
        throw new HttpRequestException($"API returned {(int)response.StatusCode}: {problem}", null, response.StatusCode);
    }
}
