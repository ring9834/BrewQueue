using BrewQueue.Infrastructure.Idempotency;
using BrewQueue.Infrastructure.Messaging;
using BrewQueue.Infrastructure.Persistence;
using BrewQueue.Infrastructure.Stats;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Proto = BrewQueue.Contracts.Events.V1;

namespace BrewQueue.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddPersistence(this IServiceCollection services, IConfiguration config)
    {
        var connectionString = config.GetConnectionString("Postgres")
            ?? throw new InvalidOperationException("Connection string 'Postgres' is missing.");

        services.AddDbContext<BrewQueueDbContext>(o => o.UseNpgsql(connectionString));
        services.AddScoped<OrderStatsProjector>();
        return services;
    }

    public static IServiceCollection AddRedisIdempotency(this IServiceCollection services, IConfiguration config)
    {
        var redis = config.GetConnectionString("Redis")
            ?? throw new InvalidOperationException("Connection string 'Redis' is missing.");

        services.Configure<IdempotencyOptions>(config.GetSection(IdempotencyOptions.SectionName));
        // AbortOnConnectFail=false: keep retrying in the background instead of crashing at startup.
        services.AddSingleton<IConnectionMultiplexer>(_ =>
            ConnectionMultiplexer.Connect($"{redis},abortConnect=false"));
        services.AddSingleton<IIdempotencyStore, RedisIdempotencyStore>();
        return services;
    }

    public static IServiceCollection AddRabbitMq(this IServiceCollection services, IConfiguration config)
    {
        services.Configure<RabbitMqOptions>(config.GetSection(RabbitMqOptions.SectionName));
        services.AddSingleton<RabbitMqConnection>();
        services.AddSingleton<IRabbitMqPublisher, RabbitMqPublisher>();
        return services;
    }

    /// <summary>Kafka producer side (the API): Schema Registry client, Protobuf serializer, publisher.</summary>
    public static IServiceCollection AddKafkaProducer(this IServiceCollection services, IConfiguration config)
    {
        services.Configure<KafkaOptions>(config.GetSection(KafkaOptions.SectionName));
        services.Configure<SchemaRegistryOptions>(config.GetSection(SchemaRegistryOptions.SectionName));

        services.AddSingleton<ISchemaRegistryClient>(sp =>
            new CachedSchemaRegistryClient(sp.GetRequiredService<IOptions<SchemaRegistryOptions>>().Value.ToConfig()));

        services.AddSingleton<IAsyncSerializer<Proto.OrderEvent>>(sp =>
            new ProtobufSerializer<Proto.OrderEvent>(sp.GetRequiredService<ISchemaRegistryClient>(), new ProtobufSerializerConfig
            {
                // Subject "order-events-value". Set explicitly: newer client versions default to a
                // strategy that can pick a different subject name.
                SubjectNameStrategy = SubjectNameStrategy.Topic,
                // Never register from the app. Schemas are registered (after a compatibility check)
                // by infra/schema-registry/register_schemas.py; the app uses the latest version.
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            }));

        services.AddSingleton<KafkaOrderEventPublisher>();
        services.AddSingleton<IOrderEventPublisher>(sp => sp.GetRequiredService<KafkaOrderEventPublisher>());
        return services;
    }

    /// <summary>Kafka consumer side (the stats worker): deserializer, dead-letter publisher, processor.</summary>
    public static IServiceCollection AddKafkaConsumer(this IServiceCollection services, IConfiguration config)
    {
        services.Configure<KafkaOptions>(config.GetSection(KafkaOptions.SectionName));
        services.Configure<OrderEventProcessorOptions>(config.GetSection(OrderEventProcessorOptions.SectionName));

        // Decoding Protobuf doesn't need the registry: the bytes carry the schema id and the message
        // type index, and the generated class knows how to parse them. Leaving the registry out
        // means a registry outage can't stop consumption.
        services.AddSingleton<IAsyncDeserializer<Proto.OrderEvent>>(_ =>
            new ProtobufDeserializer<Proto.OrderEvent>(null, new ProtobufDeserializerConfig
            {
                SubjectNameStrategy = SubjectNameStrategy.Topic
            }));

        services.AddSingleton<IDeadLetterPublisher, KafkaDeadLetterPublisher>();
        services.AddSingleton<IOrderEventApplier, ScopedProjectorApplier>();
        services.AddSingleton<OrderEventProcessor>();
        return services;
    }
}
