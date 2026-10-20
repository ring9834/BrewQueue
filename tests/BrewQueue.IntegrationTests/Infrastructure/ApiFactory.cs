using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;
using Testcontainers.Redis;

namespace BrewQueue.IntegrationTests.Infrastructure;

/// <summary>
/// Boots the real API (WebApplicationFactory) against real infrastructure running in throwaway
/// Docker containers. Containers start once per test collection and are removed afterwards.
/// Requires Docker to be running.
/// </summary>
public sealed class ApiFactory : WebApplicationFactory<Program>, IAsyncLifetime
{
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    public PostgreSqlContainer Postgres { get; } = new PostgreSqlBuilder()
        .WithImage("postgres:17-alpine")
        .Build();

    public RedisContainer Redis { get; } = new RedisBuilder()
        .WithImage("redis:7-alpine")
        .Build();

    public RabbitMqContainer RabbitMq { get; } = new RabbitMqBuilder()
        .WithImage("rabbitmq:4-management-alpine")
        .Build();

    public KafkaContainer Kafka { get; } = new KafkaBuilder().Build();

    /// <summary>Parsed from the container's amqp:// connection string.</summary>
    public Uri RabbitUri => new(RabbitMq.GetConnectionString());

    public string RabbitUser => Uri.UnescapeDataString(RabbitUri.UserInfo.Split(':')[0]);
    public string RabbitPassword => Uri.UnescapeDataString(RabbitUri.UserInfo.Split(':')[1]);

    public async Task InitializeAsync()
    {
        // Start all four in parallel: much faster than one after another.
        await Task.WhenAll(Postgres.StartAsync(), Redis.StartAsync(), RabbitMq.StartAsync(), Kafka.StartAsync());
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");

        // UseSetting is applied early enough for Program.cs to read these while registering services.
        builder.UseSetting("ConnectionStrings:Postgres", Postgres.GetConnectionString());
        builder.UseSetting("ConnectionStrings:Redis", Redis.GetConnectionString());
        builder.UseSetting("RabbitMq:HostName", RabbitUri.Host);
        builder.UseSetting("RabbitMq:Port", RabbitUri.Port.ToString());
        builder.UseSetting("RabbitMq:UserName", RabbitUser);
        builder.UseSetting("RabbitMq:Password", RabbitPassword);
        builder.UseSetting("Kafka:BootstrapServers", Kafka.GetBootstrapAddress());
        builder.UseSetting("Kafka:OrderEventsPartitions", "1");
    }

    async Task IAsyncLifetime.DisposeAsync()
    {
        await base.DisposeAsync();
        await Task.WhenAll(
            Postgres.DisposeAsync().AsTask(),
            Redis.DisposeAsync().AsTask(),
            RabbitMq.DisposeAsync().AsTask(),
            Kafka.DisposeAsync().AsTask());
    }
}

[CollectionDefinition(Name)]
public sealed class ApiCollection : ICollectionFixture<ApiFactory>
{
    public const string Name = "api";
}

public static class TestHelpers
{
    public static HttpRequestMessage PlaceOrderRequest(object body, Guid? idempotencyKey = null)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/api/orders")
        {
            Content = JsonContent.Create(body, options: ApiFactory.Json)
        };
        if (idempotencyKey is not null)
            request.Headers.Add("Idempotency-Key", idempotencyKey.ToString());
        return request;
    }

    public static object OrderBody(string customer = "Test Customer", int menuItemId = 1, int quantity = 1) => new
    {
        customerName = customer,
        lines = new[] { new { menuItemId, quantity } }
    };

    /// <summary>Polls until <paramref name="probe"/> returns a value, or fails after the timeout.</summary>
    public static async Task<T> EventuallyAsync<T>(Func<Task<T?>> probe, TimeSpan timeout, string because)
        where T : class
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var result = await probe();
            if (result is not null) return result;
            await Task.Delay(250);
        }
        throw new TimeoutException($"Timed out after {timeout.TotalSeconds}s waiting for: {because}");
    }
}
