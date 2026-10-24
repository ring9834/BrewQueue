using System.Text;
using BrewQueue.Contracts;
using BrewQueue.Infrastructure.Messaging;
using BrewQueue.Infrastructure.Stats;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Google.Protobuf;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Proto = BrewQueue.Contracts.Events.V1;

namespace BrewQueue.UnitTests.Infrastructure;

/// <summary>Every branch of the consumer's retry / dead-letter rules, without Kafka or a database.</summary>
public class OrderEventProcessorTests
{
    private readonly FakeApplier _applier = new();
    private readonly FakeDeadLetters _deadLetters = new();

    private OrderEventProcessor CreateProcessor(int maxAttempts = 3) => new(
        new ProtobufDeserializer<Proto.OrderEvent>(null, new ProtobufDeserializerConfig { SubjectNameStrategy = SubjectNameStrategy.Topic }),
        _applier,
        _deadLetters,
        Options.Create(new OrderEventProcessorOptions { MaxAttempts = maxAttempts, InitialRetryDelay = TimeSpan.Zero }),
        NullLogger<OrderEventProcessor>.Instance);

    [Fact]
    public async Task Valid_message_is_applied()
    {
        var outcome = await CreateProcessor().ProcessAsync(Result(ValidPayload()), CancellationToken.None);

        Assert.Equal(ProcessingOutcome.Completed, outcome);
        Assert.Equal(1, _applier.Calls);
        Assert.Empty(_deadLetters.Published);
    }

    [Fact]
    public async Task Garbage_bytes_are_dead_lettered_without_being_applied()
    {
        var outcome = await CreateProcessor().ProcessAsync(Result(Encoding.UTF8.GetBytes("{\"json\":true}")), CancellationToken.None);

        Assert.Equal(ProcessingOutcome.Completed, outcome);
        Assert.Equal(0, _applier.Calls);
        Assert.Equal(DeadLetterReasons.InvalidMessage, Assert.Single(_deadLetters.Published).Reason);
    }

    [Fact]
    public async Task Tombstone_is_dead_lettered()
    {
        var outcome = await CreateProcessor().ProcessAsync(Result(null), CancellationToken.None);

        Assert.Equal(ProcessingOutcome.Completed, outcome);
        Assert.Equal(DeadLetterReasons.InvalidMessage, Assert.Single(_deadLetters.Published).Reason);
    }

    [Fact]
    public async Task Well_formed_protobuf_that_breaks_the_contract_is_dead_lettered()
    {
        var message = OrderEventMapper.ToProto(SampleEvent());
        message.EventId = "nope";

        await CreateProcessor().ProcessAsync(Result(Frame(message)), CancellationToken.None);

        Assert.Equal(0, _applier.Calls);
        Assert.Equal(DeadLetterReasons.InvalidMessage, Assert.Single(_deadLetters.Published).Reason);
    }

    [Fact]
    public async Task Transient_failure_that_clears_is_retried_in_place()
    {
        _applier.FailuresBeforeSuccess = 2;
        _applier.Failure = () => new TimeoutException();

        var outcome = await CreateProcessor(maxAttempts: 3).ProcessAsync(Result(ValidPayload()), CancellationToken.None);

        Assert.Equal(ProcessingOutcome.Completed, outcome);
        Assert.Equal(3, _applier.Calls);
        Assert.Empty(_deadLetters.Published);
    }

    [Fact]
    public async Task Persistent_transient_failure_asks_to_retry_later_and_never_dead_letters()
    {
        // A database outage must not send every event to the DLT.
        _applier.FailuresBeforeSuccess = int.MaxValue;
        _applier.Failure = () => new NpgsqlException("connection refused", new System.Net.Sockets.SocketException());

        var outcome = await CreateProcessor(maxAttempts: 3).ProcessAsync(Result(ValidPayload()), CancellationToken.None);

        Assert.Equal(ProcessingOutcome.RetryLater, outcome);
        Assert.Equal(3, _applier.Calls);
        Assert.Empty(_deadLetters.Published);
    }

    [Fact]
    public async Task Permanent_failure_is_dead_lettered_after_max_attempts()
    {
        _applier.FailuresBeforeSuccess = int.MaxValue;
        _applier.Failure = () => new InvalidOperationException("bug in the projector");

        var outcome = await CreateProcessor(maxAttempts: 3).ProcessAsync(Result(ValidPayload()), CancellationToken.None);

        Assert.Equal(ProcessingOutcome.Completed, outcome);
        Assert.Equal(3, _applier.Calls);
        var dead = Assert.Single(_deadLetters.Published);
        Assert.Equal(DeadLetterReasons.ProcessingFailed, dead.Reason);
        Assert.Equal(3, dead.Attempts);
    }

    [Fact]
    public async Task If_the_dead_letter_topic_is_unavailable_the_message_is_not_skipped()
    {
        _deadLetters.Fail = true;

        var outcome = await CreateProcessor().ProcessAsync(Result(Encoding.UTF8.GetBytes("garbage")), CancellationToken.None);

        Assert.Equal(ProcessingOutcome.RetryLater, outcome);
    }

    // ------------------------------------------------------------------ helpers

    private static OrderEvent SampleEvent() => new(
        Guid.NewGuid(), OrderEventTypes.OrderPlaced, Guid.NewGuid(), "Received", null, 5.50m,
        [new OrderEventLine(1, "Flat White", 1, 5.50m)], DateTimeOffset.UtcNow);

    private static byte[] ValidPayload() => Frame(OrderEventMapper.ToProto(SampleEvent()));

    /// <summary>
    /// Confluent wire format, as the producer's serializer writes it: magic byte 0, 4-byte
    /// big-endian schema id, message-index array (a single 0 = "first message in the file"), payload.
    /// </summary>
    private static byte[] Frame(Proto.OrderEvent message) =>
        [0, 0, 0, 0, 1, 0, .. message.ToByteArray()];

    private static ConsumeResult<string, byte[]> Result(byte[]? value) => new()
    {
        Topic = MessagingNames.OrderEventsTopic,
        Partition = new Partition(0),
        Offset = new Offset(42),
        Message = new Message<string, byte[]> { Key = Guid.NewGuid().ToString(), Value = value!, Headers = new Headers() }
    };

    private sealed class FakeApplier : IOrderEventApplier
    {
        public int Calls { get; private set; }
        public int FailuresBeforeSuccess { get; set; }
        public Func<Exception> Failure { get; set; } = () => new InvalidOperationException();

        public Task<bool> ApplyAsync(OrderEvent evt, CancellationToken ct)
        {
            Calls++;
            if (Calls <= FailuresBeforeSuccess) throw Failure();
            return Task.FromResult(true);
        }
    }

    private sealed class FakeDeadLetters : IDeadLetterPublisher
    {
        public List<(string Reason, int Attempts)> Published { get; } = [];
        public bool Fail { get; set; }

        public Task PublishAsync(ConsumeResult<string, byte[]> original, string reason, Exception error, int attempts, CancellationToken ct)
        {
            if (Fail) throw new KafkaException(ErrorCode.Local_Transport);
            Published.Add((reason, attempts));
            return Task.CompletedTask;
        }
    }
}

public class FailureClassifierTests
{
    public static TheoryData<Exception, bool> Cases => new()
    {
        { new TimeoutException(), true },
        { new DbUpdateConcurrencyException("row changed"), true },
        { new DbUpdateException("dup", new PostgresException("duplicate key", "ERROR", "ERROR", PostgresErrorCodes.UniqueViolation)), true },
        { new NpgsqlException("socket", new System.Net.Sockets.SocketException()), true },
        { new InvalidOperationException("wrapped", new TimeoutException()), true },
        { new DbUpdateException("too long", new PostgresException("value too long", "ERROR", "ERROR", PostgresErrorCodes.StringDataRightTruncation)), false },
        { new InvalidOperationException("bug"), false },
        { new ArgumentException("bad"), false }
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public void Classifies(Exception ex, bool transient) => Assert.Equal(transient, FailureClassifier.IsTransient(ex));
}
