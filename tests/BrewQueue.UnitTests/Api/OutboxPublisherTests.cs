using System.Text.Json;
using BrewQueue.Api.Services;
using BrewQueue.Contracts;
using Confluent.Kafka;

namespace BrewQueue.UnitTests.Api;

/// <summary>Which outbox failures may eventually abandon a message, and which are retried forever.</summary>
public class OutboxPublisherTests
{
    public static TheoryData<Exception, bool> Cases => new()
    {
        // Retrying can't fix these.
        { new JsonException("bad payload"), true },
        { new ArgumentException("unknown status"), true },
        { new InvalidEventException("bad event"), true },
        { Produce(ErrorCode.MsgSizeTooLarge), true },
        // These are outages: never abandon, just keep retrying with backoff.
        { Produce(ErrorCode.Local_MsgTimedOut), false },
        { Produce(ErrorCode.NotEnoughReplicas), false },
        { new KafkaException(ErrorCode.Local_Transport), false },
        { new HttpRequestException("schema registry down"), false },
        { new TimeoutException(), false }
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public void Classifies_failures(Exception ex, bool permanent) => Assert.Equal(permanent, OutboxPublisher.IsPermanent(ex));

    private static ProduceException<string, byte[]> Produce(ErrorCode code) =>
        new(new Error(code), new DeliveryResult<string, byte[]>());
}
