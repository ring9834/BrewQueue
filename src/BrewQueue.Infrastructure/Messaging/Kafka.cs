using System.Text;
using BrewQueue.Contracts;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BrewQueue.Infrastructure.Messaging;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string ConsumerGroupId { get; set; } = "brewqueue-stats";
    public int OrderEventsPartitions { get; set; } = 3;
}

public interface IKafkaProducer
{
    Task ProduceAsync(string topic, string key, string type, string jsonValue, string? correlationId, CancellationToken ct);
}

public sealed class KafkaProducer : IKafkaProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;

    public KafkaProducer(IOptions<KafkaOptions> options, ILogger<KafkaProducer> logger)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            // Kafka's own idempotent producer: retries can't create duplicates or reorder messages within a partition. (Implies Acks = All.)
            EnableIdempotence = true, // Setting EnableIdempotence = true automatically forces acks=all (and retries > 0), which is why the explicit Acks = Acks.All is somewhat redundant but harmless/documentary.
            Acks = Acks.All, // Wait for all replicas to acknowledge before considering a message sent.
            LingerMs = 5, // Batch messages for a few milliseconds to improve throughput.
            ClientId = AppDomain.CurrentDomain.FriendlyName // For logging and metrics.
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => logger.LogWarning("Kafka producer error: {Reason}", e.Reason))
            .Build();
    }

    public async Task ProduceAsync(string topic, string key, string type, string jsonValue, string? correlationId, CancellationToken ct)
    {
        // type is used by consumers to deserialize the message.
        // Why use Encoding.UTF8.GetBytes? Because Kafka headers are byte arrays, and we want to store the type as a UTF-8 encoded string in the header. 
        // { MessagingNames.MessageTypeHeader, Encoding.UTF8.GetBytes(type) } is creating a header with the name "x-message-type" and the value being the UTF-8 encoded bytes of the type string. This allows consumers to read the message type from the header and deserialize the message accordingly.
        var headers = new Headers { { MessagingNames.MessageTypeHeader, Encoding.UTF8.GetBytes(type) } }; 
        if (correlationId is not null)
            headers.Add(MessagingNames.CorrelationIdHeader, Encoding.UTF8.GetBytes(correlationId));

        // Same key (order id) => same partition => per-order ordering is preserved.
        await _producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = key,
            Value = jsonValue,
            Headers = headers
        }, ct);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5)); // Wait for any in-flight messages to be sent before disposing.
        _producer.Dispose();
    }
}

public static class KafkaTopics
{
    /// <summary>
    /// Creates the topic with an explicit partition count (instead of relying on auto-create).
    /// </summary>
    public static async Task EnsureCreatedAsync(KafkaOptions options, ILogger logger, CancellationToken ct)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = options.BootstrapServers })
            .Build();

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await admin.CreateTopicsAsync(
                [
                    new TopicSpecification
                    {
                        Name = MessagingNames.OrderEventsTopic,
                        NumPartitions = options.OrderEventsPartitions,
                        ReplicationFactor = 1
                    }
                ]);
                logger.LogInformation("Created Kafka topic {Topic}", MessagingNames.OrderEventsTopic);
                return;
            }
            catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
            {
                return;
            }
            catch (KafkaException ex) when (attempt < 10)
            {
                logger.LogWarning(ex, "Kafka not ready (attempt {Attempt}); retrying", attempt);
                await Task.Delay(TimeSpan.FromSeconds(3), ct);
            }
        }
    }

    public static string? ReadHeader(Headers? headers, string name) =>
        headers is not null && headers.TryGetLastBytes(name, out var bytes) ? Encoding.UTF8.GetString(bytes) : null;
}
