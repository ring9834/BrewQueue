using System.ComponentModel.DataAnnotations;
using BrewQueue.Api.Contracts;
using BrewQueue.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace BrewQueue.Api.Controllers;

[ApiController]
[Route("api/stats")]
[Produces("application/json")]
[Tags("Stats")]
public sealed class StatsController(StatsService stats) : ControllerBase
{
    /// <summary>Order statistics.</summary>
    /// <remarks>
    /// Hourly figures and top items come from a read model built by the stats worker from Kafka
    /// events, so they lag a second or two behind the orders themselves.
    /// </remarks>
    /// <param name="hours">How many hours of history to return (1–168).</param>
    /// <param name="ct">Cancellation token.</param>
    [HttpGet("summary")]
    [ProducesResponseType<StatsSummaryDto>(StatusCodes.Status200OK)]
    public Task<StatsSummaryDto> GetSummary([FromQuery, Range(1, 168)] int hours = 12, CancellationToken ct = default) =>
        stats.GetSummaryAsync(hours, ct);
}
