using System.Text.Json;
using BrewQueue.Contracts;
using BrewQueue.Infrastructure.Messaging;
using BrewQueue.Infrastructure.Persistence;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace BrewQueue.Api.CrossCutting;

/// <summary>
/// Kafka: can we reach the cluster, is the order-events topic visible to our user, are all
/// brokers up, and is the producer still usable?
/// </summary>
public sealed class KafkaHealthCheck : IHealthCheck, IDisposable
{
    private readonly IAdminClient _admin;
    private readonly IOrderEventPublisher _publisher;
    private readonly int _expectedBrokers;

    public KafkaHealthCheck(IOptions<KafkaOptions> options, IOrderEventPublisher publisher)
    {
        _publisher = publisher;
        _expectedBrokers = options.Value.ReplicationFactor;
        _admin = new AdminClientBuilder(KafkaClientConfig.Apply(new AdminClientConfig(), options.Value)).Build();
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        if (!_publisher.IsHealthy)
            return HealthCheckResult.Unhealthy("Kafka producer hit a fatal error; restart the API.");

        try
        {
            // GetMetadata is synchronous; keep it off the request thread.
            var metadata = await Task.Run(() => _admin.GetMetadata(MessagingNames.OrderEventsTopic, TimeSpan.FromSeconds(5)), ct);
            var topic = metadata.Topics.SingleOrDefault();
            if (topic is null || topic.Error.IsError)
                return HealthCheckResult.Unhealthy($"Topic {MessagingNames.OrderEventsTopic} unavailable: {topic?.Error.Reason}");

            var data = new Dictionary<string, object> { ["brokers"] = metadata.Brokers.Count, ["partitions"] = topic.Partitions.Count };
            return metadata.Brokers.Count < _expectedBrokers
                ? HealthCheckResult.Degraded($"{metadata.Brokers.Count} of {_expectedBrokers} brokers reachable", data: data)
                : HealthCheckResult.Healthy(data: data);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return HealthCheckResult.Unhealthy("Kafka unreachable", ex);
        }
    }

    public void Dispose() => _admin.Dispose();
}

/// <summary>Degraded when the outbox is backing up or has abandoned messages that need a human.</summary>
public sealed class OutboxHealthCheck(IServiceScopeFactory scopes, TimeProvider clock) : IHealthCheck
{
    private static readonly TimeSpan MaxPendingAge = TimeSpan.FromMinutes(2);

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        using var scope = scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<BrewQueueDbContext>();

        var abandoned = await db.OutboxMessages.CountAsync(m => m.AbandonedAt != null, ct);
        var oldestPending = await db.OutboxMessages
            .Where(m => m.ProcessedAt == null && m.AbandonedAt == null)
            .OrderBy(m => m.Sequence)
            .Select(m => (DateTimeOffset?)m.OccurredAt)
            .FirstOrDefaultAsync(ct);

        var data = new Dictionary<string, object> { ["abandoned"] = abandoned };
        if (oldestPending is { } oldest) data["oldestPendingSeconds"] = (int)(clock.GetUtcNow() - oldest).TotalSeconds;

        if (abandoned > 0)
            return HealthCheckResult.Degraded($"{abandoned} abandoned outbox message(s); see LastError in outbox_messages", data: data);
        if (oldestPending is { } o && clock.GetUtcNow() - o > MaxPendingAge)
            return HealthCheckResult.Degraded($"Outbox message waiting for more than {MaxPendingAge.TotalMinutes} minutes", data: data);
        return HealthCheckResult.Healthy(data: data);
    }
}

public sealed class RedisHealthCheck(IConnectionMultiplexer redis) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        try
        {
            var latency = await redis.GetDatabase().PingAsync();
            return HealthCheckResult.Healthy($"Ping {latency.TotalMilliseconds:0.0} ms");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Redis unreachable", ex);
        }
    }
}

public sealed class RabbitMqHealthCheck(RabbitMqConnection connection) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        try
        {
            var conn = await connection.GetAsync(ct);
            return conn.IsOpen ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy("Connection closed");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("RabbitMQ unreachable", ex);
        }
    }
}

public static class HealthCheckResponseWriter
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    public static Task WriteAsync(HttpContext context, HealthReport report)
    {
        context.Response.ContentType = "application/json";
        return context.Response.WriteAsync(JsonSerializer.Serialize(new
        {
            status = report.Status.ToString(),
            totalMs = report.TotalDuration.TotalMilliseconds,
            checks = report.Entries.ToDictionary(e => e.Key, e => new
            {
                status = e.Value.Status.ToString(),
                description = e.Value.Description,
                error = e.Value.Exception?.Message,
                data = e.Value.Data
            })
        }, Json));
    }
}
