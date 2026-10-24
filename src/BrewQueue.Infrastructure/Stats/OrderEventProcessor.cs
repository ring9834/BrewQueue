using BrewQueue.Contracts;
using BrewQueue.Infrastructure.Messaging;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Proto = BrewQueue.Contracts.Events.V1;

namespace BrewQueue.Infrastructure.Stats;

public enum ProcessingOutcome
{
    /// <summary>Applied, skipped as a duplicate, or dead-lettered. Move past this offset.</summary>
    Completed,

    /// <summary>A transient failure outlasted the in-place retries. Don't move past this offset.</summary>
    RetryLater
}

public sealed class OrderEventProcessorOptions
{
    public const string SectionName = "Kafka:Processing";

    /// <summary>Attempts per message before giving up on it for now (transient) or for good (permanent).</summary>
    public int MaxAttempts { get; set; } = 3;

    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromMilliseconds(200);
}

/// <summary>Applies one decoded event to the read model. A seam for unit tests.</summary>
public interface IOrderEventApplier
{
    Task<bool> ApplyAsync(OrderEvent evt, CancellationToken ct);
}

/// <summary>Runs <see cref="OrderStatsProjector"/> in a fresh DI scope (fresh DbContext) per attempt.</summary>
public sealed class ScopedProjectorApplier(IServiceScopeFactory scopes) : IOrderEventApplier
{
    public async Task<bool> ApplyAsync(OrderEvent evt, CancellationToken ct)
    {
        using var scope = scopes.CreateScope();
        return await scope.ServiceProvider.GetRequiredService<OrderStatsProjector>().ApplyAsync(evt, ct);
    }
}

/// <summary>
/// Decides what happens to each consumed message. The rules:
/// <list type="number">
/// <item><b>Can't be decoded or fails validation</b> → dead-letter now. Retrying can't help.</item>
/// <item><b>Applying it throws</b> → retry in place a few times with backoff.</item>
/// <item><b>Still failing, transient</b> (database down, timeout, concurrency conflict) →
/// <see cref="ProcessingOutcome.RetryLater"/>. Never dead-letter because of an outage: every event
/// during the outage would end up there. The consumer rewinds and the lag alert fires instead.</item>
/// <item><b>Still failing, permanent</b> (a bug, data that violates a constraint) → dead-letter, so
/// one bad event can't block the partition.</item>
/// </list>
/// Kept free of the consume loop so every rule is unit-testable.
/// </summary>
public sealed class OrderEventProcessor(
    IAsyncDeserializer<Proto.OrderEvent> deserializer,
    IOrderEventApplier applier,
    IDeadLetterPublisher deadLetters,
    IOptions<OrderEventProcessorOptions> options,
    ILogger<OrderEventProcessor> logger)
{
    private readonly OrderEventProcessorOptions _options = options.Value;

    public async Task<ProcessingOutcome> ProcessAsync(ConsumeResult<string, byte[]> result, CancellationToken ct)
    {
        var correlationId = KafkaTopics.ReadHeader(result.Message.Headers, MessagingNames.CorrelationIdHeader);
        // Serilog turns logger scopes into log properties, so every line below carries the id.
        using var _ = logger.BeginScope(new Dictionary<string, object?> { ["CorrelationId"] = correlationId });

        OrderEvent evt;
        try
        {
            evt = await DecodeAsync(result);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "Invalid message at {TopicPartitionOffset}; dead-lettering", result.TopicPartitionOffset);
            return await DeadLetterAsync(result, DeadLetterReasons.InvalidMessage, ex, attempts: 1, ct);
        }

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var applied = await applier.ApplyAsync(evt, ct);
                logger.LogInformation("{EventType} for order {OrderId} at {TopicPartitionOffset} (applied: {Applied})",
                    evt.EventType, evt.OrderId, result.TopicPartitionOffset, applied);
                return ProcessingOutcome.Completed;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var transient = FailureClassifier.IsTransient(ex);

                if (attempt < _options.MaxAttempts)
                {
                    var delay = _options.InitialRetryDelay * Math.Pow(2, attempt - 1);
                    logger.LogWarning(ex, "Attempt {Attempt}/{MaxAttempts} failed for event {EventId} ({Kind}); retrying in {Delay}",
                        attempt, _options.MaxAttempts, evt.EventId, transient ? "transient" : "permanent", delay);
                    await Task.Delay(delay, ct);
                    continue;
                }

                if (transient)
                {
                    logger.LogWarning(ex, "Transient failure for event {EventId} after {Attempts} attempts; will retry later",
                        evt.EventId, attempt);
                    return ProcessingOutcome.RetryLater;
                }

                logger.LogError(ex, "Event {EventId} failed permanently after {Attempts} attempts; dead-lettering",
                    evt.EventId, attempt);
                return await DeadLetterAsync(result, DeadLetterReasons.ProcessingFailed, ex, attempt, ct);
            }
        }
    }

    private async Task<OrderEvent> DecodeAsync(ConsumeResult<string, byte[]> result)
    {
        var value = result.Message.Value;
        if (value is null || value.Length == 0)
            throw new InvalidEventException("Message has no value (tombstone or empty).");

        var message = await deserializer.DeserializeAsync(value, isNull: false,
            new SerializationContext(MessageComponentType.Value, result.Topic, result.Message.Headers));

        return OrderEventMapper.FromProto(message ?? throw new InvalidEventException("Message decoded to null."));
    }

    private async Task<ProcessingOutcome> DeadLetterAsync(
        ConsumeResult<string, byte[]> result, string reason, Exception error, int attempts, CancellationToken ct)
    {
        try
        {
            await deadLetters.PublishAsync(result, reason, error, attempts, ct);
            logger.LogWarning("Message at {TopicPartitionOffset} moved to {DeadLetterTopic} ({Reason})",
                result.TopicPartitionOffset, MessagingNames.OrderEventsDeadLetterTopic, reason);
            return ProcessingOutcome.Completed;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // If we can't park it, we mustn't skip it either.
            logger.LogError(ex, "Could not dead-letter message at {TopicPartitionOffset}; will retry later", result.TopicPartitionOffset);
            return ProcessingOutcome.RetryLater;
        }
    }
}

public static class FailureClassifier
{
    /// <summary>True for failures that are expected to go away by themselves.</summary>
    public static bool IsTransient(Exception ex) => ex switch
    {
        // Another stats instance updated the same row (xmin changed). A retry re-reads it.
        DbUpdateConcurrencyException => true,
        // Two instances inserted the same hourly/item row at once. A retry finds the row and updates it.
        DbUpdateException { InnerException: PostgresException { SqlState: PostgresErrorCodes.UniqueViolation } } => true,
        // Connection refused, broken socket, server shutting down, ...
        NpgsqlException { IsTransient: true } => true,
        TimeoutException => true,
        _ => ex.InnerException is not null && IsTransient(ex.InnerException)
    };
}
