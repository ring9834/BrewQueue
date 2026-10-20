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
            EnableAutoCommit = false
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
        if (evt is null) return;

        // Retry DB failures in place; we must not commit the offset until the update succeeds.
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
