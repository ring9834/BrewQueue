using System.Text.Json;
using BrewQueue.Contracts;
using BrewQueue.Infrastructure.Messaging;
using BrewQueue.Infrastructure.Persistence;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Serilog.Context;

namespace BrewQueue.Api.Services;

public sealed class OutboxOptions
{
    public const string SectionName = "Outbox";

    public int BatchSize { get; set; } = 200;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Upper bound for the backoff after failed cycles (e.g. while Kafka is down).</summary>
    public TimeSpan MaxBackoff { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Attempts before a message that fails *permanently* is abandoned. Transient failures never abandon.</summary>
    public int MaxPermanentFailures { get; set; } = 3;

    /// <summary>Published rows are deleted after this long.</summary>
    public TimeSpan Retention { get; set; } = TimeSpan.FromDays(3);
}

/// <summary>
/// Publishes outbox rows to Kafka and RabbitMQ in the order they were written.
///
/// <para><b>One publisher at a time.</b> Every cycle runs in a transaction that first takes a
/// PostgreSQL advisory lock (<c>pg_try_advisory_xact_lock</c>). With several API instances,
/// whichever gets the lock publishes; the others skip the cycle. Two instances publishing
/// different batches in parallel could deliver one order's events out of order, which is what
/// this prevents. The lock is released automatically when the transaction ends, including when
/// the instance crashes, so another instance takes over on its next cycle.</para>
///
/// <para><b>In order.</b> Rows are read by <see cref="OutboxMessage.Sequence"/>. Kafka messages are
/// pipelined: queued in order, then awaited together. A row is only marked published if it and
/// every row before it in the batch succeeded; anything after a failure is sent again next cycle.</para>
///
/// <para><b>At least once.</b> A crash after sending but before committing re-sends the batch.
/// Consumers deduplicate (stats on EventId; the kitchen's status change is a no-op when repeated).</para>
///
/// <para>Remaining gap: if this instance loses its database connection mid-cycle while Kafka stays
/// reachable, another instance can start publishing before this one notices, briefly overlapping.
/// Closing that fully needs Kafka transactions with a shared transactional.id (zombie fencing) or
/// change data capture (Debezium) reading the outbox from the write-ahead log.</para>
/// </summary>
public sealed class OutboxPublisher(
    IServiceScopeFactory scopes,
    IRabbitMqPublisher rabbit,
    IOrderEventPublisher kafka,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<OutboxOptions> outboxOptions,
    TimeProvider clock,
    ILogger<OutboxPublisher> logger) : BackgroundService
{
    /// <summary>
    /// Advisory lock id shared by every API instance (ASCII "BREWQUEU"). Any constant works,
    /// as long as all instances use the same one and nothing else in the database does.
    /// </summary>
    public const long PublisherLockKey = 0x4252_4557_5155_4555;

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private static readonly TimeSpan CleanupInterval = TimeSpan.FromHours(1);

    private readonly OutboxOptions _options = outboxOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield(); // don't block application startup

        if (kafkaOptions.Value.ManageTopics)
        {
            try
            {
                await KafkaTopics.EnsureCreatedAsync(kafkaOptions.Value, logger, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Could not ensure Kafka topics exist; continuing");
            }
        }

        var delay = _options.PollInterval;
        var nextCleanup = clock.GetUtcNow();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(delay, stoppingToken);
                var result = await PublishBatchAsync(stoppingToken);

                // Back off while publishing keeps failing (e.g. Kafka down); reset once it works.
                delay = result.Failed
                    ? TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, _options.MaxBackoff.Ticks))
                    : _options.PollInterval;

                if (clock.GetUtcNow() >= nextCleanup) // periodically delete old published rows so the table doesn't grow forever
                {
                    await CleanupAsync(stoppingToken);
                    nextCleanup = clock.GetUtcNow() + CleanupInterval;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Outbox publishing cycle failed");
                delay = _options.MaxBackoff;
            }
        }
    }

    public readonly record struct BatchResult(bool HadLock, int Published, bool Failed);

    public async Task<BatchResult> PublishBatchAsync(CancellationToken ct)
    {
        using var scope = scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<BrewQueueDbContext>();

        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var hasLock = await db.Database
            .SqlQuery<bool>($"SELECT pg_try_advisory_xact_lock({PublisherLockKey}) AS \"Value\"")
            .SingleAsync(ct);
        //// This is the equivalent for SQL Server, but we use PostgreSQL for the advisory lock because it works across multiple databases on the same server.
        // var result = new SqlParameter("@result", SqlDbType.Int) { Direction = ParameterDirection.Output };
        // await db.Database.ExecuteSqlRawAsync(
        //     """
        //     EXEC @result = sp_getapplock
        //         @Resource = N'BrewQueue.OutboxPublisher',
        //         @LockMode = 'Exclusive',
        //         @LockOwner = 'Transaction',
        //         @LockTimeout = 0;
        //     """,
        //     [result], ct);
        // var hasLock = (int)result.Value >= 0;
        if (!hasLock)
            return new BatchResult(HadLock: false, Published: 0, Failed: false); // another instance is publishing

        var batch = await db.OutboxMessages
            .Where(m => m.ProcessedAt == null && m.AbandonedAt == null)
            .OrderBy(m => m.Sequence)
            .Take(_options.BatchSize)
            .ToListAsync(ct);

        if (batch.Count == 0)
        {
            await tx.CommitAsync(ct);
            return new BatchResult(HadLock: true, Published: 0, Failed: false);
        }

        var (published, failed) = await PublishInOrderAsync(batch, ct);

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
        return new BatchResult(HadLock: true, published, failed);
    }

    private async Task<(int Published, bool Failed)> PublishInOrderAsync(List<OutboxMessage> batch, CancellationToken ct)
    {
        var published = 0;
        var inFlight = new List<(OutboxMessage Message, Task Delivery)>();

        // Waits for queued Kafka messages in order. Stops at the first failure: rows after it stay
        // unpublished even if Kafka accepted them, so they're re-sent after the failed one.
        async Task<bool> DrainAsync()
        {
            foreach (var (message, delivery) in inFlight)
            {
                try
                {
                    await delivery;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    RecordFailure(message, ex);
                    inFlight.Clear();
                    return false;
                }
                MarkPublished(message);
                published++;
            }
            inFlight.Clear();
            return true;
        }

        foreach (var message in batch)
        {
            using var _ = LogContext.PushProperty("CorrelationId", message.CorrelationId);
            try
            {
                if (message.Destination == OutboxDestination.Kafka)
                {
                    var evt = JsonSerializer.Deserialize<OrderEvent>(message.Payload, Json)
                              ?? throw new JsonException("Outbox payload is null.");
                    inFlight.Add((message, await kafka.EnqueueAsync(evt, message.CorrelationId, ct)));
                }
                else
                {
                    // Everything before this RabbitMQ message must be confirmed first.
                    if (!await DrainAsync()) return (published, true);

                    await rabbit.PublishAsync(MessagingNames.KitchenExchange, MessagingNames.KitchenRoutingKey,
                        message.Id.ToString(), message.Type, message.Payload, message.CorrelationId, ct: ct);
                    MarkPublished(message);
                    published++;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Failed before it was queued (serialization, Schema Registry, RabbitMQ).
                // Confirm what's already queued; only count this failure if everything before it
                // succeeded (otherwise it never really got its turn). Then stop the batch.
                if (await DrainAsync()) RecordFailure(message, ex);
                return (published, true);
            }
        }

        var allDelivered = await DrainAsync();
        return (published, !allDelivered);
    }

    private void MarkPublished(OutboxMessage message)
    {
        message.ProcessedAt = clock.GetUtcNow();
        logger.LogDebug("Published outbox message {MessageId} #{Sequence} ({Type}) to {Destination}",
            message.Id, message.Sequence, message.Type, message.Destination);
    }

    private void RecordFailure(OutboxMessage message, Exception ex)
    {
        message.Attempts++;
        message.LastError = ex.Message.Length > 2000 ? ex.Message[..2000] : ex.Message;

        if (IsPermanent(ex) && message.Attempts >= _options.MaxPermanentFailures)
        {
            // Without this, one unpublishable row would block the outbox forever. The cost: later
            // events for the same order are published without it. Health check reports Degraded.
            message.AbandonedAt = clock.GetUtcNow();
            logger.LogCritical(ex,
                "Abandoned outbox message {MessageId} #{Sequence} ({Type}) after {Attempts} permanent failures",
                message.Id, message.Sequence, message.Type, message.Attempts);
            return;
        }

        logger.LogWarning(ex, "Failed to publish outbox message {MessageId} #{Sequence} (attempt {Attempts}); will retry",
            message.Id, message.Sequence, message.Attempts);
    }

    /// <summary>Failures that retrying can't fix. Anything else (broker down, timeouts) is retried forever.</summary>
    internal static bool IsPermanent(Exception ex) => ex switch
    {
        JsonException or ArgumentException or InvalidEventException => true,
        ProduceException<string, byte[]> pe => pe.Error.Code is ErrorCode.MsgSizeTooLarge
            or ErrorCode.InvalidMsg or ErrorCode.InvalidRecord,
        _ => false
    };

    private async Task CleanupAsync(CancellationToken ct)
    {
        using var scope = scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<BrewQueueDbContext>();
        var cutoff = clock.GetUtcNow() - _options.Retention;

        int deleted, total = 0;
        do
        {
            // Small batches keep locks and WAL volume short.
            deleted = await db.Database.ExecuteSqlAsync($"""
                DELETE FROM outbox_messages
                WHERE "Id" IN (SELECT "Id" FROM outbox_messages WHERE "ProcessedAt" < {cutoff} LIMIT 1000)
                """, ct);
            total += deleted;
        } while (deleted > 0);

        if (total > 0) logger.LogInformation("Deleted {Count} published outbox messages older than {Cutoff}", total, cutoff);
    }
}
