using System.Text;
using BrewQueue.Contracts;
using BrewQueue.Infrastructure.Messaging;
using BrewQueue.Infrastructure.Stats;
using BrewQueue.IntegrationTests.Infrastructure;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Proto = BrewQueue.Contracts.Events.V1;

namespace BrewQueue.IntegrationTests;

/// <summary>
/// A poison message on order-events goes through the real processor and the real dead-letter
/// publisher, and ends up on order-events.dlt with the original bytes and dlt-* headers.
/// </summary>
[Collection(ApiCollection.Name)]
public class DeadLetterTests(ApiFactory factory)
{
    [Fact]
    public async Task Undecodable_message_is_copied_to_the_dead_letter_topic_with_diagnostics()
    {
        var kafkaOptions = factory.Services.GetRequiredService<IOptions<KafkaOptions>>();
        var key = Guid.NewGuid().ToString();
        var garbage = Encoding.UTF8.GetBytes("definitely not protobuf");

        // 1. Put a poison message on order-events (as a misbehaving producer would).
        using (var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = factory.Kafka.GetBootstrapAddress() }).Build())
            await producer.ProduceAsync(MessagingNames.OrderEventsTopic, new Message<string, byte[]> { Key = key, Value = garbage });

        var poison = ConsumeFirst(MessagingNames.OrderEventsTopic, m => m.Message.Key == key);

        // 2. Run it through the processor with the real dead-letter publisher.
        using var deadLetters = new KafkaDeadLetterPublisher(kafkaOptions, TimeProvider.System, NullLogger<KafkaDeadLetterPublisher>.Instance);
        var applier = new ThrowingApplier();
        var processor = new OrderEventProcessor(
            new ProtobufDeserializer<Proto.OrderEvent>(null, new ProtobufDeserializerConfig { SubjectNameStrategy = SubjectNameStrategy.Topic }),
            applier,
            deadLetters,
            Options.Create(new OrderEventProcessorOptions()),
            NullLogger<OrderEventProcessor>.Instance);

        var outcome = await processor.ProcessAsync(poison, CancellationToken.None);

        Assert.Equal(ProcessingOutcome.Completed, outcome); // the consumer may move on
        Assert.False(applier.Called);                       // never projected

        // 3. It's on the DLT: same key and bytes, plus where it came from and why.
        var dead = ConsumeFirst(MessagingNames.OrderEventsDeadLetterTopic, m => m.Message.Key == key);
        Assert.Equal(garbage, dead.Message.Value);
        Assert.Equal(MessagingNames.OrderEventsTopic, Header(dead, MessagingNames.DltOriginalTopicHeader));
        Assert.Equal(poison.Offset.Value.ToString(), Header(dead, MessagingNames.DltOriginalOffsetHeader));
        Assert.Equal(DeadLetterReasons.InvalidMessage, Header(dead, MessagingNames.DltReasonHeader));
        Assert.False(string.IsNullOrEmpty(Header(dead, MessagingNames.DltErrorHeader)));
    }

    private ConsumeResult<string, byte[]> ConsumeFirst(string topic, Func<ConsumeResult<string, byte[]>, bool> match)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = factory.Kafka.GetBootstrapAddress(),
            GroupId = $"test-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();
        consumer.Subscribe(topic);

        var deadline = DateTime.UtcNow.AddSeconds(30);
        try
        {
            while (DateTime.UtcNow < deadline)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result?.Message is not null && match(result)) return result;
            }
        }
        finally
        {
            consumer.Close();
        }
        throw new TimeoutException($"No matching message on {topic}");
    }

    private static string? Header(ConsumeResult<string, byte[]> result, string name) =>
        KafkaTopics.ReadHeader(result.Message.Headers, name);

    private sealed class ThrowingApplier : IOrderEventApplier
    {
        public bool Called { get; private set; }

        public Task<bool> ApplyAsync(OrderEvent evt, CancellationToken ct)
        {
            Called = true;
            throw new InvalidOperationException("should not be called for an invalid message");
        }
    }
}
