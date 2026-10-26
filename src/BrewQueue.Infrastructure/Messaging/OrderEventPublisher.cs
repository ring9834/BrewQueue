using System.Text;
using BrewQueue.Contracts;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Proto = BrewQueue.Contracts.Events.V1;

namespace BrewQueue.Infrastructure.Messaging;

public interface IOrderEventPublisher
{
    /// <summary>
    /// Serializes the event (Protobuf, framed with its Schema Registry id) and hands it to the
    /// Kafka producer. Returns once the message is <b>queued</b>; the returned task completes when
    /// the brokers have acknowledged it (or fails).
    /// <para>Messages are queued in call order, and the idempotent producer keeps that order per
    /// partition, so a caller can queue several messages and then await them all.</para>
    /// </summary>
    Task<Task> EnqueueAsync(OrderEvent evt, string? correlationId, CancellationToken ct);

    /// <summary>False after a fatal producer error. The process must be restarted.</summary>
    bool IsHealthy { get; }
}

public sealed class KafkaOrderEventPublisher : IOrderEventPublisher, IDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly IAsyncSerializer<Proto.OrderEvent> _serializer;
    private readonly ILogger<KafkaOrderEventPublisher> _logger;
    private volatile bool _fatal;

    public KafkaOrderEventPublisher(
        IOptions<KafkaOptions> options,
        IAsyncSerializer<Proto.OrderEvent> serializer,
        ILogger<KafkaOrderEventPublisher> logger)
    {
        _serializer = serializer;
        _logger = logger;

        // Values are serialized *before* calling the producer (see EnqueueAsync), so the producer
        // itself only deals with bytes. With synchronous serializers, ProduceAsync queues the
        // message before it returns, which is what guarantees call order == send order.
        _producer = new ProducerBuilder<string, byte[]>(KafkaClientConfig.DurableProducer(options.Value))
            .SetErrorHandler((_, e) =>
            {
                if (e.IsFatal)
                {
                    _fatal = true;
                    _logger.LogCritical("Fatal Kafka producer error {Code}: {Reason}. Restart required.", e.Code, e.Reason);
                }
                else
                {
                    _logger.LogWarning("Kafka producer error {Code}: {Reason}", e.Code, e.Reason);
                }
            })
            .Build();
    }

    public bool IsHealthy => !_fatal;

    public async Task<Task> EnqueueAsync(OrderEvent evt, string? correlationId, CancellationToken ct)
    {
        var headers = new Headers { { MessagingNames.MessageTypeHeader, Encoding.UTF8.GetBytes(evt.EventType) } };
        if (correlationId is not null)
            headers.Add(MessagingNames.CorrelationIdHeader, Encoding.UTF8.GetBytes(correlationId));

        var proto = OrderEventMapper.ToProto(evt);
        var value = await _serializer.SerializeAsync(proto,
            new SerializationContext(MessageComponentType.Value, MessagingNames.OrderEventsTopic, headers));

        // Key = order id: every event of one order goes to the same partition, in order.
        // puts the message into librdkafka’s in-memory send queue and returns immediately. 
        // librdkafka is the native client library that does the real network work. 
        // The Task it returns completes when the brokers acknowledge the message (acks=all, so all in-sync replicas have it), or fails with a ProduceException.
        return _producer.ProduceAsync(MessagingNames.OrderEventsTopic, new Message<string, byte[]>
        {
            Key = evt.OrderId.ToString(),
            Value = value,
            Headers = headers
        }, ct);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
