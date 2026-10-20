using BrewQueue.Infrastructure;
using BrewQueue.KitchenWorker;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSerilog((services, lc) => lc
    .ReadFrom.Configuration(builder.Configuration)
    .ReadFrom.Services(services)
    .Enrich.FromLogContext()
    .Enrich.WithProperty("Service", "kitchen"));

builder.Services.AddRabbitMq(builder.Configuration);
builder.Services.Configure<KitchenOptions>(builder.Configuration.GetSection(KitchenOptions.SectionName));

builder.Services
    .AddHttpClient<BrewQueueApiClient>(c =>
        c.BaseAddress = new Uri(builder.Configuration["BrewQueueApi:BaseUrl"] ?? "http://localhost:5080/"))
    // Retries with jittered backoff, circuit breaker and timeouts, out of the box.
    .AddStandardResilienceHandler();

builder.Services.AddHostedService<KitchenConsumer>();

builder.Build().Run();
