using System.Text.Json.Serialization;
using BrewQueue.Api.CrossCutting;
using BrewQueue.Api.Services;
using BrewQueue.Infrastructure;
using BrewQueue.Infrastructure.Persistence;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Scalar.AspNetCore;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// ---------- Logging (Serilog: structured logs to console + Seq) ----------
builder.Services.AddSerilog((services, lc) => lc
    .ReadFrom.Configuration(builder.Configuration)
    .ReadFrom.Services(services)
    .Enrich.FromLogContext()
    .Enrich.WithProperty("Service", "api"));

// ---------- MVC controllers + JSON ----------
builder.Services
    .AddControllers()
    .AddJsonOptions(o => o.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter()));

// ---------- Error handling: ProblemDetails everywhere ----------
builder.Services.AddBrewQueueProblemDetails();
builder.Services.AddExceptionHandler<GlobalExceptionHandler>();

// ---------- OpenAPI (built-in generator) ----------
builder.Services.AddOpenApi(options =>
{
    options.AddDocumentTransformer((document, _, _) =>
    {
        document.Info.Title = "BrewQueue API";
        document.Info.Version = "v1";
        document.Info.Description =
            "Café ordering API. Errors are RFC 9457 ProblemDetails with `traceId`, `correlationId` and `errorCode`.";
        return Task.CompletedTask;
    });
});

// ---------- Cross-cutting services ----------
builder.Services.AddHttpContextAccessor();
builder.Services.AddSingleton<ICorrelationIdAccessor, CorrelationIdAccessor>();
builder.Services.AddSingleton(TimeProvider.System);

// ---------- Infrastructure ----------
builder.Services
    .AddPersistence(builder.Configuration)
    .AddRedisIdempotency(builder.Configuration)
    .AddRabbitMq(builder.Configuration)
    .AddKafka(builder.Configuration);

// ---------- Application services ----------
builder.Services.AddScoped<OrderService>();
builder.Services.AddScoped<StatsService>();
builder.Services.AddSingleton<OutboxPublisher>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<OutboxPublisher>());

// ---------- Health checks ----------
builder.Services.AddHealthChecks()
    .AddDbContextCheck<BrewQueueDbContext>("postgres", tags: ["ready"])
    .AddCheck<RedisHealthCheck>("redis", tags: ["ready"])
    .AddCheck<RabbitMqHealthCheck>("rabbitmq", tags: ["ready"]);

// ---------- CORS for the Nuxt dev server ----------
const string CorsPolicy = "web";
builder.Services.AddCors(o => o.AddPolicy(CorsPolicy, p => p
    .WithOrigins(builder.Configuration.GetSection("Cors:Origins").Get<string[]>() ?? ["http://localhost:3000"])
    .AllowAnyHeader()
    .AllowAnyMethod()
    .WithExposedHeaders("Location", IdempotencyFilter.ReplayedHeader, CorrelationIdMiddleware.HeaderName)));
// allow the Nuxt dev server to read the Location header for redirects. For the browser to read that header at all, the API’s CORS policy lists it in WithExposedHeaders.

var app = builder.Build();

// ---------- Database ----------
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<BrewQueueDbContext>();
    await DatabaseInitializer.InitializeAsync(db, app.Logger);
}

// ---------- HTTP pipeline (order matters) ----------
app.UseMiddleware<CorrelationIdMiddleware>(); // first, so everything after can log the id
app.UseExceptionHandler();                     // unhandled exceptions -> ProblemDetails
app.UseStatusCodePages();                      // bare 404/405 -> ProblemDetails
app.UseSerilogRequestLogging(o =>
{
    o.MessageTemplate = "HTTP {RequestMethod} {RequestPath} responded {StatusCode} in {Elapsed:0.0} ms";
});
app.UseCors(CorsPolicy);

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();                    // /openapi/v1.json
    app.MapScalarApiReference(o => o     // /scalar/v1 — interactive API docs
        .WithTitle("BrewQueue API"));
}

app.MapControllers();
app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = _ => false });
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = c => c.Tags.Contains("ready"),
    ResponseWriter = HealthCheckResponseWriter.WriteAsync
});

app.Run();

/// <summary>Exposed for WebApplicationFactory in the integration tests.</summary>
public partial class Program { }
