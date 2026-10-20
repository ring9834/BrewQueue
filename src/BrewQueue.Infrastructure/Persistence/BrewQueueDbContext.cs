using BrewQueue.Domain;
using Microsoft.EntityFrameworkCore;

namespace BrewQueue.Infrastructure.Persistence;

public sealed class BrewQueueDbContext(DbContextOptions<BrewQueueDbContext> options) : DbContext(options)
{
    public DbSet<MenuItem> MenuItems => Set<MenuItem>();
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<HourlyOrderStat> HourlyOrderStats => Set<HourlyOrderStat>();
    public DbSet<ItemPopularity> ItemPopularity => Set<ItemPopularity>();
    public DbSet<ProcessedEvent> ProcessedEvents => Set<ProcessedEvent>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<MenuItem>(e =>
        {
            e.ToTable("menu_items");
            e.Property(x => x.Name).HasMaxLength(80);
            e.Property(x => x.Category).HasMaxLength(40);
            e.Property(x => x.Description).HasMaxLength(300);
            e.Property(x => x.Price).HasPrecision(10, 2);
            e.HasData(SeedData.MenuItems);
        });

        b.Entity<Order>(e =>
        {
            e.ToTable("orders");
            e.Property(x => x.CustomerName).HasMaxLength(50);
            e.Property(x => x.Status).HasConversion<string>().HasMaxLength(20);
            e.HasIndex(x => x.Status);
            e.Ignore(x => x.Total);

            // Npgsql maps a uint row version to PostgreSQL's built-in xmin column.
            e.Property(x => x.Version).IsRowVersion();

            e.HasMany(x => x.Lines).WithOne().HasForeignKey("OrderId").OnDelete(DeleteBehavior.Cascade);
            e.Navigation(x => x.Lines).HasField("_lines").UsePropertyAccessMode(PropertyAccessMode.Field);
        });

        b.Entity<OrderLine>(e =>
        {
            e.ToTable("order_lines");
            e.Property(x => x.Name).HasMaxLength(80);
            e.Property(x => x.UnitPrice).HasPrecision(10, 2);
            e.Ignore(x => x.LineTotal);
        });

        b.Entity<OutboxMessage>(e =>
        {
            e.ToTable("outbox_messages");
            e.Property(x => x.Destination).HasConversion<string>().HasMaxLength(20);
            e.Property(x => x.Type).HasMaxLength(100);
            e.Property(x => x.Key).HasMaxLength(100);
            e.Property(x => x.Payload).HasColumnType("jsonb");
            e.Property(x => x.CorrelationId).HasMaxLength(100);
            // Partial index: the publisher only ever scans unprocessed rows.
            e.HasIndex(x => x.OccurredAt).HasFilter("\"ProcessedAt\" IS NULL");
        });

        b.Entity<HourlyOrderStat>(e =>
        {
            e.ToTable("stats_hourly_orders");
            e.HasKey(x => x.HourStart);
            e.Property(x => x.Revenue).HasPrecision(12, 2);
        });

        b.Entity<ItemPopularity>(e =>
        {
            e.ToTable("stats_item_popularity");
            e.HasKey(x => x.MenuItemId);
            e.Property(x => x.MenuItemId).ValueGeneratedNever();
            e.Property(x => x.Name).HasMaxLength(80);
        });

        b.Entity<ProcessedEvent>(e =>
        {
            e.ToTable("processed_events");
            e.HasKey(x => x.EventId);
        });
    }
}
