using System.Text.Json;
using BrewQueue.Contracts;
using BrewQueue.Infrastructure.Messaging;
using BrewQueue.Infrastructure.Stats;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Serilog.Context;

namespace BrewQueue.StatsWorker;

/// <summary>
/// Reads the order-events topic and updates the stats read model.
///
/// <para>Offsets are committed manually AFTER the database update. If the worker crashes in
/// between, the event is redelivered, and the projector's ProcessedEvents check skips it.
/// That combination is "effectively once".</para>
///
/// <para>Try it: stop this worker, place a few orders, start it again — it catches up from its
/// last committed offset. Or change the GroupId to rebuild the stats from the beginning.</para>
/// </summary>
public sealed class OrderEventsConsumer(
    IServiceScopeFactory scopes,
    IOptions<KafkaOptions> options,
    ILogger<OrderEventsConsumer> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        // Consume() is a blocking call, so run the loop on its own thread.
        Task.Factory.StartNew(() => RunAsync(stoppingToken), stoppingToken,
            TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

    private async Task RunAsync(CancellationToken ct)
    {
        await KafkaTopics.EnsureCreatedAsync(options.Value, logger, ct);

        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = options.Value.ConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false // we commit manually after the DB update. The worker saves its position (commits the offset) itself, and only after the database update succeeds.
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => logger.LogWarning("Kafka consumer error: {Reason}", e.Reason))
            .SetPartitionsAssignedHandler((_, partitions) =>
                logger.LogInformation("Assigned partitions: {Partitions}", string.Join(", ", partitions.Select(p => p.Partition.Value))))
            .Build();

        consumer.Subscribe(MessagingNames.OrderEventsTopic);
        logger.LogInformation("Stats worker consuming {Topic} as group {GroupId}",
            MessagingNames.OrderEventsTopic, config.GroupId);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result;
                try
                {
                    // Blocks until a message arrives. That’s why the loop runs on its own long-running thread instead of the shared thread pool..
                    result = consumer.Consume(ct);
                }
                catch (ConsumeException ex)
                {
                    logger.LogWarning(ex, "Consume failed: {Reason}", ex.Error.Reason);
                    continue;
                }

                if (result?.Message is null) continue;
                await HandleAsync(result, ct);
                consumer.Commit(result);
            }
        }
        catch (OperationCanceledException)
        {
            // normal shutdown
        }
        finally
        {
            consumer.Close(); // leave the group cleanly so partitions are reassigned quickly
        }
    }

    private async Task HandleAsync(ConsumeResult<string, string> result, CancellationToken ct)
    {
        var correlationId = KafkaTopics.ReadHeader(result.Message.Headers, MessagingNames.CorrelationIdHeader);
        using var _ = LogContext.PushProperty("CorrelationId", correlationId);

        OrderEvent? evt;
        try
        {
            evt = JsonSerializer.Deserialize<OrderEvent>(result.Message.Value, Json);
        }
        catch (JsonException ex)
        {
            // A poison event must not block the partition forever: log it and move on.
            logger.LogError(ex, "Skipping undeserializable event at {TopicPartitionOffset}", result.TopicPartitionOffset);
            return;
        }
        if (evt is null) return; // Should never happen, but the compiler doesn’t know that.

        // Retry DB failures in place; we must not commit the offset until the update succeeds.
        // Note: it is an unlimited retry loop, but the delay grows exponentially to avoid hammering the database. The worker will eventually succeed if the database is healthy again.
        // It’s deliberate for temporary failures, such as Postgres restarting or a network blip. Skipping the event would lose a stat, and moving past it would break per-order ordering. Waiting until the database comes back is the right behaviour then.
        // But, It blocks every partition this worker owns, not just one.
        // A permanent failure blocks forever. Some errors will never fix themselves: a value too long for a column (Name is limited to 80 characters), a numeric overflow, or a bug in the projector. That event then becomes a poison pill, retried every 30 seconds for good, while the stats page silently stops updating.
        // Kafka eventually kicks the worker out of the group. If a consumer doesn’t call Consume() for longer than max.poll.interval.ms (5 minutes by default), Kafka assumes it’s stuck. It removes the consumer from the group and reassigns its partitions. If the database later recovers, the Commit for that message then fails because the worker no longer owns the partition. Commit isn’t inside a try, so the worker crashes, Docker restarts it, and the event is reprocessed. The data stays correct thanks to the dedupe table, but it’s an uncontrolled path.
        // A better design is: Keep retrying temporary errors, but put a bound on it, and dead-letter what can’t succeed.
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                using var scope = scopes.CreateScope();
                var projector = scope.ServiceProvider.GetRequiredService<OrderStatsProjector>();
                var changed = await projector.ApplyAsync(evt, ct);

                logger.LogInformation("{EventType} for order {OrderId} at {TopicPartitionOffset} (applied: {Applied})",
                    evt.EventType, evt.OrderId, result.TopicPartitionOffset, changed);
                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var delay = TimeSpan.FromSeconds(Math.Min(30, Math.Pow(2, attempt)));
                logger.LogWarning(ex, "Projection failed (attempt {Attempt}); retrying in {Delay}", attempt, delay);
                await Task.Delay(delay, ct);
            }
        }
    }
}
