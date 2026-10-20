using System.Text;
using System.Text.Json;
using BrewQueue.Contracts;
using BrewQueue.Infrastructure.Messaging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog.Context;

namespace BrewQueue.KitchenWorker;

public sealed class KitchenOptions
{
    public const string SectionName = "Kitchen";

    /// <summary>Simulated preparation time per order.</summary>
    public TimeSpan PrepareTime { get; set; } = TimeSpan.FromSeconds(8);
}

/// <summary>
/// Consumes <see cref="PrepareOrderCommand"/> from the kitchen work queue.
/// <list type="bullet">
/// <item>Success → ack.</item>
/// <item>Transient failure → re-publish with an incremented retry header after a backoff, then ack the original.</item>
/// <item>Permanent failure, bad JSON, or too many retries → nack(requeue: false) → dead-letter queue.</item>
/// </list>
/// </summary>
public sealed class KitchenConsumer(
    RabbitMqConnection connection,
    IRabbitMqPublisher publisher,
    BrewQueueApiClient api,
    IOptions<RabbitMqOptions> rabbitOptions,
    IOptions<KitchenOptions> kitchenOptions,
    ILogger<KitchenConsumer> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var channel = await ConnectWithRetryAsync(stoppingToken);

        await RabbitMqTopology.DeclareAsync(channel, stoppingToken);
        // Prefetch limits how many unacked messages this worker holds at once.
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: rabbitOptions.Value.PrefetchCount, global: false,
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, delivery) => HandleAsync(channel, delivery, stoppingToken);

        await channel.BasicConsumeAsync(MessagingNames.KitchenQueue, autoAck: false, consumer: consumer,
            cancellationToken: stoppingToken);
        logger.LogInformation("Kitchen is open: consuming {Queue}", MessagingNames.KitchenQueue);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Kitchen closing");
        }
        finally
        {
            await channel.CloseAsync();
        }
    }

    private async Task HandleAsync(IChannel channel, BasicDeliverEventArgs delivery, CancellationToken ct)
    {
        var headers = delivery.BasicProperties.Headers;
        var retryCount = RetryPolicy.ReadIntHeader(headers, MessagingNames.RetryCountHeader);
        var correlationId = RetryPolicy.ReadStringHeader(headers, MessagingNames.CorrelationIdHeader);
        var body = Encoding.UTF8.GetString(delivery.Body.Span);

        using var _ = LogContext.PushProperty("CorrelationId", correlationId);
        using var __ = LogContext.PushProperty("MessageId", delivery.BasicProperties.MessageId);

        PrepareOrderCommand? command;
        try
        {
            command = JsonSerializer.Deserialize<PrepareOrderCommand>(body, Json);
            if (command is null || command.OrderId == Guid.Empty) throw new JsonException("Empty command");
        }
        catch (JsonException ex)
        {
            logger.LogError(ex, "Poison message (invalid JSON); dead-lettering");
            await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: false, ct);
            return;
        }

        try
        {
            var items = string.Join(", ", command.Lines.Select(l => $"{l.Quantity}× {l.Name}"));
            logger.LogInformation("Preparing order {OrderId} for {CustomerName}: {Items}",
                command.OrderId, command.CustomerName, items);

            await api.SetStatusAsync(command.OrderId, "Preparing", correlationId, ct);
            await Task.Delay(kitchenOptions.Value.PrepareTime, ct);
            await api.SetStatusAsync(command.OrderId, "Ready", correlationId, ct);

            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutting down: put the message back so another worker (or we, after restart) can take it.
            await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true, CancellationToken.None);
        }
        catch (Exception ex)
        {
            var transient = ex is not PermanentFailureException;
            if (RetryPolicy.ShouldRetry(retryCount, transient))
            {
                var delay = RetryPolicy.Backoff(retryCount);
                logger.LogWarning(ex, "Order {OrderId} failed (retry {Retry}/{Max}); retrying in {Delay}",
                    command.OrderId, retryCount + 1, RetryPolicy.MaxRetries, delay);

                await Task.Delay(delay, ct);
                await publisher.PublishAsync(MessagingNames.KitchenExchange, MessagingNames.KitchenRoutingKey,
                    delivery.BasicProperties.MessageId ?? Guid.NewGuid().ToString(), nameof(PrepareOrderCommand),
                    body, correlationId, retryCount + 1, ct);
                await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, ct);
            }
            else
            {
                logger.LogError(ex, "Order {OrderId} failed permanently after {Retries} retries; dead-lettering",
                    command.OrderId, retryCount);
                await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: false, ct);
            }
        }
    }

    private async Task<IChannel> ConnectWithRetryAsync(CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var conn = await connection.GetAsync(ct);
                return await conn.CreateChannelAsync(cancellationToken: ct);
            }
            catch (Exception ex) when (attempt < 20 && ex is not OperationCanceledException)
            {
                logger.LogWarning("RabbitMQ not ready (attempt {Attempt}): {Message}", attempt, ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(3), ct);
            }
        }
    }
}
