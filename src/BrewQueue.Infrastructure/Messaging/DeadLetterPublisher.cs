using System.Globalization;
using System.Text;
using BrewQueue.Contracts;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BrewQueue.Infrastructure.Messaging;

public static class DeadLetterReasons
{
    /// <summary>Couldn't be decoded or failed validation. Retrying can never help.</summary>
    public const string InvalidMessage = "invalid-message";

    /// <summary>Decoded fine, but applying it kept failing with a non-transient error.</summary>
    public const string ProcessingFailed = "processing-failed";
}

public interface IDeadLetterPublisher
{
    /// <summary>
    /// Copies the message to the dead-letter topic: same key, same value bytes, original headers,
    /// plus dlt-* headers describing where it came from and why it failed. Completes only after
    /// the brokers have acknowledged it, so the caller can safely move past the original.
    /// </summary>
    Task PublishAsync(ConsumeResult<string, byte[]> original, string reason, Exception error, int attempts, CancellationToken ct);
}

public sealed class KafkaDeadLetterPublisher : IDeadLetterPublisher, IDisposable
{
    private const int MaxErrorHeaderLength = 1000;

    private readonly IProducer<string, byte[]> _producer;
    private readonly TimeProvider _clock;

    public KafkaDeadLetterPublisher(IOptions<KafkaOptions> options, TimeProvider clock, ILogger<KafkaDeadLetterPublisher> logger)
    {
        _clock = clock;
        _producer = new ProducerBuilder<string, byte[]>(KafkaClientConfig.DurableProducer(options.Value))
            .SetErrorHandler((_, e) => logger.LogWarning("Dead-letter producer error {Code}: {Reason}", e.Code, e.Reason))
            .Build();
    }

    public async Task PublishAsync(ConsumeResult<string, byte[]> original, string reason, Exception error, int attempts, CancellationToken ct)
    {
        var headers = new Headers();
        if (original.Message.Headers is not null)
        {
            foreach (var header in original.Message.Headers)
                headers.Add(header.Key, header.GetValueBytes());
        }

        var message = $"{error.GetType().Name}: {error.Message}";
        if (message.Length > MaxErrorHeaderLength) message = message[..MaxErrorHeaderLength];

        Add(headers, MessagingNames.DltOriginalTopicHeader, original.Topic);
        Add(headers, MessagingNames.DltOriginalPartitionHeader, original.Partition.Value.ToString(CultureInfo.InvariantCulture));
        Add(headers, MessagingNames.DltOriginalOffsetHeader, original.Offset.Value.ToString(CultureInfo.InvariantCulture));
        Add(headers, MessagingNames.DltReasonHeader, reason);
        Add(headers, MessagingNames.DltErrorHeader, message);
        Add(headers, MessagingNames.DltAttemptsHeader, attempts.ToString(CultureInfo.InvariantCulture));
        Add(headers, MessagingNames.DltFailedAtHeader, _clock.GetUtcNow().ToString("O"));

        await _producer.ProduceAsync(MessagingNames.OrderEventsDeadLetterTopic, new Message<string, byte[]>
        {
            Key = original.Message.Key,
            Value = original.Message.Value,
            Headers = headers
        }, ct);
    }

    private static void Add(Headers headers, string key, string value) => headers.Add(key, Encoding.UTF8.GetBytes(value));

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
