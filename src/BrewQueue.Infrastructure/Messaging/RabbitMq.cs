using System.Text;
using BrewQueue.Contracts;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace BrewQueue.Infrastructure.Messaging;

public sealed class RabbitMqOptions
{
    public const string SectionName = "RabbitMq";
    public string HostName { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string UserName { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public ushort PrefetchCount { get; set; } = 10;
    public ushort ConsumerDispatchConcurrency { get; set; } = 4;
}

/// <summary>
/// One long-lived connection per process (connections are expensive; channels are cheap).
/// RabbitMQ.Client 7.x is fully async.
/// </summary>
public sealed class RabbitMqConnection(IOptions<RabbitMqOptions> options, ILogger<RabbitMqConnection> logger)
    : IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private IConnection? _connection;

    public bool IsOpen => _connection?.IsOpen == true;

    public async Task<IConnection> GetAsync(CancellationToken ct)
    {
        if (_connection is { IsOpen: true }) return _connection;

        await _lock.WaitAsync(ct);
        try
        {
            if (_connection is { IsOpen: true }) return _connection;

            var o = options.Value;
            var factory = new ConnectionFactory
            {
                HostName = o.HostName,
                Port = o.Port,
                UserName = o.UserName,
                Password = o.Password,
                ClientProvidedName = AppDomain.CurrentDomain.FriendlyName,
                AutomaticRecoveryEnabled = true,
                // Lets an async consumer handle several deliveries in parallel (default is 1).
                ConsumerDispatchConcurrency = o.ConsumerDispatchConcurrency
            };

            logger.LogInformation("Connecting to RabbitMQ at {Host}:{Port}", o.HostName, o.Port);
            _connection = await factory.CreateConnectionAsync(ct);
            return _connection;
        }
        finally
        {
            _lock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.CloseAsync();
            _connection.Dispose();
        }
        _lock.Dispose();
    }
}

public static class RabbitMqTopology
{
    /// <summary>
    /// Declares the kitchen exchange + work queue, and a dead-letter exchange + queue.
    /// Messages that are rejected (nack, requeue: false) are routed to the DLQ automatically.
    /// Declarations are idempotent, so every process can safely call this at startup.
    /// </summary>
    public static async Task DeclareAsync(IChannel channel, CancellationToken ct)
    {
        await channel.ExchangeDeclareAsync(MessagingNames.KitchenDeadLetterExchange, ExchangeType.Fanout,
            durable: true, autoDelete: false, cancellationToken: ct);
        await channel.QueueDeclareAsync(MessagingNames.KitchenDeadLetterQueue,
            durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);
        await channel.QueueBindAsync(MessagingNames.KitchenDeadLetterQueue, MessagingNames.KitchenDeadLetterExchange,
            routingKey: "", cancellationToken: ct);

        await channel.ExchangeDeclareAsync(MessagingNames.KitchenExchange, ExchangeType.Direct,
            durable: true, autoDelete: false, cancellationToken: ct);
        await channel.QueueDeclareAsync(MessagingNames.KitchenQueue,
            durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = MessagingNames.KitchenDeadLetterExchange,
                ["x-queue-type"] = "quorum" // replicated, durable queue type recommended by RabbitMQ
            },
            cancellationToken: ct);
        await channel.QueueBindAsync(MessagingNames.KitchenQueue, MessagingNames.KitchenExchange,
            MessagingNames.KitchenRoutingKey, cancellationToken: ct);
    }
}

public interface IRabbitMqPublisher
{
    Task PublishAsync(string exchange, string routingKey, string messageId, string type, string jsonBody,
        string? correlationId, int retryCount = 0, CancellationToken ct = default);
}

/// <summary>
/// Publishes with publisher confirms, so a completed call means the broker has the message.
/// </summary>
public sealed class RabbitMqPublisher(RabbitMqConnection connection) : IRabbitMqPublisher, IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1); // channels are not thread-safe for publishing
    private IChannel? _channel;

    public async Task PublishAsync(string exchange, string routingKey, string messageId, string type,
        string jsonBody, string? correlationId, int retryCount = 0, CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            var channel = await GetChannelAsync(ct);
            var headers = new Dictionary<string, object?>
            {
                [MessagingNames.MessageTypeHeader] = type,
                [MessagingNames.RetryCountHeader] = retryCount
            };
            if (correlationId is not null) headers[MessagingNames.CorrelationIdHeader] = correlationId;

            var props = new BasicProperties
            {
                MessageId = messageId,
                ContentType = "application/json",
                Persistent = true,
                Type = type,
                CorrelationId = correlationId,
                Headers = headers
            };

            // With publisher confirmation tracking enabled, this awaits the broker's ack
            // and throws if the broker nacks the message.
            await channel.BasicPublishAsync(exchange, routingKey, mandatory: false,
                basicProperties: props, body: Encoding.UTF8.GetBytes(jsonBody), cancellationToken: ct);
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task<IChannel> GetChannelAsync(CancellationToken ct)
    {
        if (_channel is { IsOpen: true }) return _channel;

        var conn = await connection.GetAsync(ct);
        _channel = await conn.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            ct);
        await RabbitMqTopology.DeclareAsync(_channel, ct);
        return _channel;
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.CloseAsync();
            _channel.Dispose();
        }
        _lock.Dispose();
    }
}

/// <summary>
/// Pure retry decision for message consumers. Transient failures are retried with backoff
/// up to <see cref="MaxRetries"/>; permanent failures go straight to the dead-letter queue.
/// </summary>
public static class RetryPolicy
{
    public const int MaxRetries = 3;

    public static bool ShouldRetry(int retryCount, bool isTransient) => isTransient && retryCount < MaxRetries;

    /// <summary>Exponential backoff: 1s, 2s, 4s…</summary>
    public static TimeSpan Backoff(int retryCount) => TimeSpan.FromSeconds(Math.Pow(2, retryCount));

    /// <summary>
    /// Reads an integer header. AMQP may deliver numbers as int/long, and strings as byte[].
    /// </summary>
    public static int ReadIntHeader(IDictionary<string, object?>? headers, string name)
    {
        if (headers is null || !headers.TryGetValue(name, out var value) || value is null) return 0;
        return value switch
        {
            int i => i,
            long l => (int)l,
            byte[] bytes when int.TryParse(Encoding.UTF8.GetString(bytes), out var parsed) => parsed,
            _ => 0
        };
    }

    public static string? ReadStringHeader(IDictionary<string, object?>? headers, string name)
    {
        if (headers is null || !headers.TryGetValue(name, out var value) || value is null) return null;
        return value switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            string s => s,
            _ => value.ToString()
        };
    }
}
