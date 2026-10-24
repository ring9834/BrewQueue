using BrewQueue.Infrastructure.Messaging;
using BrewQueue.Infrastructure.Stats;
using BrewQueue.Contracts;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace BrewQueue.StatsWorker;

/// <summary>
/// The consume loop: fetch a message, let <see cref="OrderEventProcessor"/> decide what to do with
/// it, then either move past it or rewind and back off.
///
/// <list type="bullet">
/// <item><b>Offsets:</b> stored only after the message is fully handled (applied, duplicate, or
/// dead-lettered) and committed in the background every few seconds. A crash re-delivers at most
/// a few seconds of messages, which the projector deduplicates.</item>
/// <item><b>Transient failures</b> (database down): the processor returns RetryLater, we seek back
/// to the same offset and wait with exponential backoff. We keep calling Consume() while waiting,
/// so the worker stays in the group instead of exceeding max.poll.interval.ms and being kicked
/// out. Lag then grows, and the StatsConsumerStuck alert fires.</item>
/// <item><b>Rebalancing:</b> cooperative-sticky, so adding or removing a worker only pauses the
/// partitions that actually move. Optional static membership (Kafka:GroupInstanceId) avoids
/// rebalances on restarts.</item>
/// </list>
/// Scaling: up to 6 instances (one per partition of order-events).
/// </summary>
public sealed class OrderEventsConsumer(
    OrderEventProcessor processor,
    IOptions<KafkaOptions> options,
    ILogger<OrderEventsConsumer> logger) : BackgroundService
{
    private static readonly TimeSpan MinBackoff = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(30);

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        // Consume() blocks, so the loop gets its own thread instead of a thread-pool thread.
        Task.Factory.StartNew(() => RunAsync(stoppingToken), stoppingToken,
            TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

    private async Task RunAsync(CancellationToken ct)
    {
        var o = options.Value;
        if (o.ManageTopics) await KafkaTopics.EnsureCreatedAsync(o, logger, ct);

        var config = KafkaClientConfig.Apply(new ConsumerConfig
        {
            GroupId = o.ConsumerGroupId,
            GroupInstanceId = string.IsNullOrWhiteSpace(o.GroupInstanceId) ? null : o.GroupInstanceId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            // We decide *which* offsets are safe (StoreOffset); librdkafka commits them in the background.
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false,
            AutoCommitIntervalMs = 5_000,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            IsolationLevel = IsolationLevel.ReadCommitted,
            SessionTimeoutMs = 45_000,
            MaxPollIntervalMs = 300_000
        }, o);

        using var consumer = new ConsumerBuilder<string, byte[]>(config)
            .SetErrorHandler((_, e) =>
            {
                if (e.IsFatal) logger.LogCritical("Fatal Kafka consumer error {Code}: {Reason}", e.Code, e.Reason);
                else logger.LogWarning("Kafka consumer error {Code}: {Reason}", e.Code, e.Reason);
            })
            .SetPartitionsAssignedHandler((_, partitions) =>
                logger.LogInformation("Assigned partitions: {Partitions}", Describe(partitions)))
            .SetPartitionsRevokedHandler((_, partitions) =>
                logger.LogInformation("Revoked partitions: {Partitions}", Describe(partitions.Select(p => p.TopicPartition))))
            .SetPartitionsLostHandler((_, partitions) =>
                logger.LogWarning("Lost partitions: {Partitions}", Describe(partitions.Select(p => p.TopicPartition))))
            .Build();

        consumer.Subscribe(MessagingNames.OrderEventsTopic);
        logger.LogInformation("Stats worker consuming {Topic} as group {GroupId} (instance {InstanceId})",
            MessagingNames.OrderEventsTopic, config.GroupId, config.GroupInstanceId ?? "dynamic");

        var backoff = MinBackoff;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]>? result;
                try
                {
                    result = consumer.Consume(ct);
                }
                catch (ConsumeException ex)
                {
                    logger.LogWarning(ex, "Consume failed: {Reason}", ex.Error.Reason);
                    continue;
                }

                if (result?.Message is null) continue;

                var outcome = await processor.ProcessAsync(result, ct);
                if (outcome == ProcessingOutcome.Completed)
                {
                    StoreOffset(consumer, result);
                    backoff = MinBackoff;
                    continue;
                }

                // RetryLater: rewind so the next Consume() returns this message again.
                consumer.Seek(result.TopicPartitionOffset);
                logger.LogWarning("Pausing {Delay} before retrying {TopicPartitionOffset}", backoff, result.TopicPartitionOffset);
                await Task.Delay(backoff, ct);
                backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, MaxBackoff.Ticks));
            }
        }
        catch (OperationCanceledException)
        {
            // normal shutdown
        }
        finally
        {
            consumer.Close(); // commits stored offsets and leaves the group cleanly
        }
    }

    private void StoreOffset(IConsumer<string, byte[]> consumer, ConsumeResult<string, byte[]> result)
    {
        try
        {
            consumer.StoreOffset(result);
        }
        catch (KafkaException ex) when (ex.Error.Code == ErrorCode.Local_State)
        {
            // The partition was revoked while we processed the message. Its new owner will
            // re-read it, and the projector skips it as a duplicate. Nothing to do.
            logger.LogDebug("Partition {Partition} no longer assigned; offset not stored", result.TopicPartition);
        }
    }

    private static string Describe(IEnumerable<TopicPartition> partitions) =>
        string.Join(", ", partitions.Select(p => p.Partition.Value));
}
