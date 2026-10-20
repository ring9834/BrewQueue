using BrewQueue.Contracts;
using BrewQueue.Infrastructure.Messaging;
using BrewQueue.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Serilog.Context;

namespace BrewQueue.Api.Services;

/// <summary>
/// Polls the outbox table and publishes pending messages to RabbitMQ or Kafka.
///
/// <para><c>FOR UPDATE SKIP LOCKED</c> lets you run several API instances safely: each one
/// locks a different batch of rows instead of publishing the same message twice.</para>
///
/// <para>Delivery is at-least-once: if the process dies after publishing but before marking the
/// row as processed, the message is published again. Consumers must therefore be idempotent
/// (the kitchen's status change is a no-op if repeated; the stats projector dedupes by EventId).</para>
/// </summary>
public sealed class OutboxPublisher(
    IServiceScopeFactory scopes,
    IRabbitMqPublisher rabbit,
    IKafkaProducer kafka,
    IOptions<KafkaOptions> kafkaOptions,
    TimeProvider clock,
    ILogger<OutboxPublisher> logger) : BackgroundService
{
    private const int BatchSize = 50;
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield(); // don't block application startup
        try
        {
            await KafkaTopics.EnsureCreatedAsync(kafkaOptions.Value, logger, stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Don't take the API down over this; failed Kafka publishes stay in the outbox and are retried.
            logger.LogError(ex, "Could not ensure Kafka topic exists; continuing");
        }

        using var timer = new PeriodicTimer(PollInterval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await PublishBatchAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Outbox publishing cycle failed");
            }
        }
    }

    public async Task<int> PublishBatchAsync(CancellationToken ct)
    {
        using var scope = scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<BrewQueueDbContext>();

        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var batch = await db.OutboxMessages
            .FromSql($"""
                SELECT * FROM outbox_messages
                WHERE "ProcessedAt" IS NULL
                ORDER BY "OccurredAt"
                LIMIT {BatchSize}
                FOR UPDATE SKIP LOCKED
                """)
            .ToListAsync(ct);

        if (batch.Count == 0) return 0;

        var published = 0;
        foreach (var message in batch)
        {
            using var _ = LogContext.PushProperty("CorrelationId", message.CorrelationId);
            try
            {
                if (message.Destination == OutboxDestination.RabbitMq)
                {
                    await rabbit.PublishAsync(MessagingNames.KitchenExchange, MessagingNames.KitchenRoutingKey,
                        message.Id.ToString(), message.Type, message.Payload, message.CorrelationId, ct: ct);
                }
                else
                {
                    await kafka.ProduceAsync(MessagingNames.OrderEventsTopic, message.Key, message.Type,
                        message.Payload, message.CorrelationId, ct);
                }

                message.ProcessedAt = clock.GetUtcNow();
                published++;
                logger.LogInformation("Published outbox message {MessageId} ({Type}) to {Destination}",
                    message.Id, message.Type, message.Destination);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                message.Attempts++;
                message.LastError = ex.Message.Length > 2000 ? ex.Message[..2000] : ex.Message;
                logger.LogWarning(ex, "Failed to publish outbox message {MessageId} (attempt {Attempts})",
                    message.Id, message.Attempts);
                // Stop here to preserve ordering: later messages may belong to the same order.
                break;
            }
        }

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
        return published;
    }
}
