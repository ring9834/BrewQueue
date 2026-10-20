using BrewQueue.Infrastructure;
using BrewQueue.Infrastructure.Persistence;
using BrewQueue.StatsWorker;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSerilog((services, lc) => lc
    .ReadFrom.Configuration(builder.Configuration)
    .ReadFrom.Services(services)
    .Enrich.FromLogContext()
    .Enrich.WithProperty("Service", "stats"));

builder.Services
    .AddPersistence(builder.Configuration)
    .AddKafka(builder.Configuration);

builder.Services.AddHostedService<OrderEventsConsumer>();

var host = builder.Build();

// The API normally creates the schema; this makes the worker safe to start first.
using (var scope = host.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<BrewQueueDbContext>();
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
    await DatabaseInitializer.InitializeAsync(db, logger);
}

host.Run();
