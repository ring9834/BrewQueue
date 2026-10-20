using BrewQueue.Api.Contracts;
using BrewQueue.Infrastructure.Persistence;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace BrewQueue.Api.Controllers;

[ApiController]
[Route("api/menu")]
[Produces("application/json")]
[Tags("Menu")]
public sealed class MenuController(BrewQueueDbContext db) : ControllerBase
{
    /// <summary>Lists the café menu.</summary>
    /// <remarks>Unavailable items are included with <c>isAvailable: false</c> so the UI can grey them out.</remarks>
    [HttpGet]
    [ProducesResponseType<IReadOnlyList<MenuItemDto>>(StatusCodes.Status200OK)]
    public async Task<IReadOnlyList<MenuItemDto>> GetMenu(CancellationToken ct) =>
        await db.MenuItems.AsNoTracking()
            .OrderBy(m => m.Category).ThenBy(m => m.Id)
            .Select(m => new MenuItemDto(m.Id, m.Name, m.Category, m.Description, m.Price, m.IsAvailable))
            .ToListAsync(ct);
}
