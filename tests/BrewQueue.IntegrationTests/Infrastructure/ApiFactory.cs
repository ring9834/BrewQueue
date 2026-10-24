using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;
using Testcontainers.Redis;

namespace BrewQueue.IntegrationTests.Infrastructure;

/// <summary>
/// Boots the real API (WebApplicationFactory) against real infrastructure running in throwaway
/// Docker containers: PostgreSQL, Redis, RabbitMQ, Kafka and Schema Registry. Containers start
/// once per test collection and are removed afterwards. Requires Docker to be running.
///
/// Kafka runs as a single plaintext broker here. TLS, SASL and ACLs are configuration of the
/// same client code (see KafkaClientConfig) and are exercised by the docker-compose cluster.
/// </summary>
public sealed class ApiFactory : WebApplicationFactory<Program>, IAsyncLifetime
{
    public const string OrderEventsSubject = "order-events-value";

    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly INetwork _network = new NetworkBuilder().Build();

    public ApiFactory()
    {
        // Extra listener "kafka:19092" so Schema Registry can reach Kafka over the Docker network.
        Kafka = new KafkaBuilder("apache/kafka:4.0.0")
            .WithNetwork(_network)
            .WithListener("kafka:19092")
            .Build();

        SchemaRegistry = new ContainerBuilder("confluentinc/cp-schema-registry:7.9.0")
            .WithNetwork(_network)
            .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .WithEnvironment("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
            .WithEnvironment("SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL", "backward_transitive")
            .WithPortBinding(8081, true)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(r => r.ForPort(8081).ForPath("/subjects")))
            .Build();
    }

    public PostgreSqlContainer Postgres { get; } = new PostgreSqlBuilder("postgres:17-alpine").Build();
    public RedisContainer Redis { get; } = new RedisBuilder("redis:7-alpine").Build();
    public RabbitMqContainer RabbitMq { get; } = new RabbitMqBuilder("rabbitmq:4-management-alpine").Build();
    public KafkaContainer Kafka { get; }
    public IContainer SchemaRegistry { get; }

    public string SchemaRegistryUrl => $"http://{SchemaRegistry.Hostname}:{SchemaRegistry.GetMappedPublicPort(8081)}";

    /// <summary>Parsed from the container's amqp:// connection string.</summary>
    public Uri RabbitUri => new(RabbitMq.GetConnectionString());

    public string RabbitUser => Uri.UnescapeDataString(RabbitUri.UserInfo.Split(':')[0]);
    public string RabbitPassword => Uri.UnescapeDataString(RabbitUri.UserInfo.Split(':')[1]);

    public async Task InitializeAsync()
    {
        await _network.CreateAsync();
        // Start in parallel: much faster than one after another. Schema Registry needs Kafka first.
        await Task.WhenAll(Postgres.StartAsync(), Redis.StartAsync(), RabbitMq.StartAsync(), Kafka.StartAsync());
        await SchemaRegistry.StartAsync();
        await RegisterOrderEventSchemaAsync();
    }

    /// <summary>
    /// Does what infra/schema-registry/register_schemas.py does in docker-compose: the API never
    /// registers schemas itself (auto.register.schemas=false), it uses the latest registered one.
    /// </summary>
    private async Task RegisterOrderEventSchemaAsync()
    {
        using var http = CreateSchemaRegistryClient();
        var config = await http.PutAsJsonAsync($"/config/{OrderEventsSubject}", new { compatibility = "BACKWARD_TRANSITIVE" });
        config.EnsureSuccessStatusCode();

        var register = await http.PostAsJsonAsync($"/subjects/{OrderEventsSubject}/versions",
            new { schemaType = "PROTOBUF", schema = ReadOrderEventProto() });
        register.EnsureSuccessStatusCode();
    }

    public HttpClient CreateSchemaRegistryClient() => new() { BaseAddress = new Uri(SchemaRegistryUrl) };

    /// <summary>The same .proto the app is compiled from (linked into the test output by the csproj).</summary>
    public static string ReadOrderEventProto() =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Schemas", "order_event.proto"));

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
        builder.UseSetting("Kafka:SecurityProtocol", "Plaintext");
        builder.UseSetting("Kafka:ManageTopics", "true");
        builder.UseSetting("Kafka:OrderEventsPartitions", "1");
        builder.UseSetting("Kafka:ReplicationFactor", "1");
        builder.UseSetting("Kafka:MinInSyncReplicas", "1");

        builder.UseSetting("SchemaRegistry:Url", SchemaRegistryUrl);
        builder.UseSetting("SchemaRegistry:Username", "");   // the test registry has no auth
        builder.UseSetting("SchemaRegistry:SslCaLocation", "");
    }

    async Task IAsyncLifetime.DisposeAsync()
    {
        await base.DisposeAsync();
        await SchemaRegistry.DisposeAsync();
        await Task.WhenAll(
            Postgres.DisposeAsync().AsTask(),
            Redis.DisposeAsync().AsTask(),
            RabbitMq.DisposeAsync().AsTask(),
            Kafka.DisposeAsync().AsTask());
        await _network.DisposeAsync();
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
