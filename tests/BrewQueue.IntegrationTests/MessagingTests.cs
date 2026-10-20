using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using BrewQueue.Api.Contracts;
using BrewQueue.Contracts;
using BrewQueue.Infrastructure.Messaging;
using BrewQueue.IntegrationTests.Infrastructure;
using Confluent.Kafka;
using RabbitMQ.Client;
using static BrewQueue.IntegrationTests.Infrastructure.TestHelpers;

namespace BrewQueue.IntegrationTests;

/// <summary>
/// Verifies the outbox → broker path end to end, against real RabbitMQ and Kafka containers.
/// </summary>
[Collection(ApiCollection.Name)]
public class MessagingTests(ApiFactory factory) : IAsyncLifetime
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly HttpClient _client = factory.CreateClient();
    private IConnection _rabbit = null!;
    private IChannel _channel = null!;

    public async Task InitializeAsync()
    {
        var connectionFactory = new ConnectionFactory
        {
            HostName = factory.RabbitUri.Host,
            Port = factory.RabbitUri.Port,
            UserName = factory.RabbitUser,
            Password = factory.RabbitPassword
        };
        _rabbit = await connectionFactory.CreateConnectionAsync();
        _channel = await _rabbit.CreateChannelAsync();
        await RabbitMqTopology.DeclareAsync(_channel, CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        await _channel.CloseAsync();
        await _rabbit.CloseAsync();
        _channel.Dispose();
        _rabbit.Dispose();
    }

    [Fact]
    public async Task Placing_an_order_puts_a_PrepareOrder_command_on_the_kitchen_queue()
    {
        var order = await PlaceAsync();

        var command = await EventuallyAsync(async () =>
        {
            // Drain the queue until we find our order (other tests also publish to it).
            while (await _channel.BasicGetAsync(MessagingNames.KitchenQueue, autoAck: true) is { } msg)
            {
                var cmd = JsonSerializer.Deserialize<PrepareOrderCommand>(msg.Body.Span, Json);
                if (cmd?.OrderId == order.Id) return cmd;
            }
            return null;
        }, Timeout, "PrepareOrderCommand on RabbitMQ");

        Assert.Equal("Test Customer", command.CustomerName);
        Assert.Equal("Flat White", Assert.Single(command.Lines).Name);
    }

    [Fact]
    public async Task Rejected_messages_are_dead_lettered()
    {
        var messageId = Guid.NewGuid().ToString();
        await _channel.BasicPublishAsync(MessagingNames.KitchenExchange, MessagingNames.KitchenRoutingKey,
            mandatory: false, basicProperties: new BasicProperties { MessageId = messageId },
            body: Encoding.UTF8.GetBytes("this is not json"));

        // Simulate the kitchen rejecting the poison message (nack without requeue).
        await EventuallyAsync(async () =>
        {
            while (await _channel.BasicGetAsync(MessagingNames.KitchenQueue, autoAck: false) is { } msg)
            {
                if (msg.BasicProperties.MessageId == messageId)
                {
                    await _channel.BasicNackAsync(msg.DeliveryTag, multiple: false, requeue: false);
                    return msg;
                }
                await _channel.BasicAckAsync(msg.DeliveryTag, multiple: false);
            }
            return null;
        }, Timeout, "poison message on the kitchen queue");

        var dead = await EventuallyAsync(
            async () => await _channel.BasicGetAsync(MessagingNames.KitchenDeadLetterQueue, autoAck: true),
            Timeout, "message in the dead-letter queue");

        Assert.Equal(messageId, dead.BasicProperties.MessageId);
    }

    [Fact]
    public async Task Status_changes_are_published_to_Kafka_keyed_by_order_id()
    {
        var order = await PlaceAsync();
        var patch = await _client.PatchAsJsonAsync($"/api/orders/{order.Id}/status",
            new { status = "Preparing" }, ApiFactory.Json);
        patch.EnsureSuccessStatusCode();

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = factory.Kafka.GetBootstrapAddress(),
            GroupId = $"test-{Guid.NewGuid()}", // fresh group => reads the topic from the start
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();
        consumer.Subscribe(MessagingNames.OrderEventsTopic);

        var seen = new List<(string Key, OrderEvent Event)>();
        await EventuallyAsync(() =>
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(1));
            if (result?.Message is not null)
                seen.Add((result.Message.Key, JsonSerializer.Deserialize<OrderEvent>(result.Message.Value, Json)!));

            var done = seen.Any(s => s.Event.OrderId == order.Id && s.Event.EventType == OrderEventTypes.OrderStatusChanged);
            return Task.FromResult(done ? seen : null);
        }, Timeout, "OrderStatusChanged on Kafka");

        consumer.Close();

        var ours = seen.Where(s => s.Event.OrderId == order.Id).ToList();
        Assert.All(ours, s => Assert.Equal(order.Id.ToString(), s.Key));
        Assert.Equal(new[] { OrderEventTypes.OrderPlaced, OrderEventTypes.OrderStatusChanged },
            ours.Select(s => s.Event.EventType));

        var changed = ours.Last().Event;
        Assert.Equal("Received", changed.PreviousStatus);
        Assert.Equal("Preparing", changed.Status);
    }

    private async Task<OrderDto> PlaceAsync()
    {
        var response = await _client.SendAsync(PlaceOrderRequest(OrderBody(), Guid.NewGuid()));
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<OrderDto>(ApiFactory.Json))!;
    }
}
