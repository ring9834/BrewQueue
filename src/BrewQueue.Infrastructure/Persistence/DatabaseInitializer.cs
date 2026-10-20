using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace BrewQueue.Infrastructure.Persistence;

public static class DatabaseInitializer
{
    /// <summary>
    /// Applies EF Core migrations if the project has any; otherwise creates the schema directly.
    /// This lets the project run out of the box. Once you add your first migration
    /// (see README), migrations take over automatically.
    /// </summary>
    public static async Task InitializeAsync(BrewQueueDbContext db, ILogger logger, CancellationToken ct = default)
    {
        // Postgres may still be starting when containers come up together, so retry briefly.
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                if (db.Database.GetMigrations().Any())
                {
                    logger.LogInformation("Applying EF Core migrations");
                    await db.Database.MigrateAsync(ct);
                }
                else
                {
                    logger.LogWarning("No EF Core migrations found; creating schema with EnsureCreated");
                    await db.Database.EnsureCreatedAsync(ct);
                }
                return;
            }
            catch (Exception ex) when (attempt < 10 && ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Database not ready (attempt {Attempt}); retrying", attempt);
                await Task.Delay(TimeSpan.FromSeconds(2), ct);
            }
        }
    }
}
