using BrewQueue.Infrastructure.Idempotency;
using BrewQueue.Infrastructure.Messaging;
using BrewQueue.Infrastructure.Persistence;
using BrewQueue.Infrastructure.Stats;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

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

    public static IServiceCollection AddKafka(this IServiceCollection services, IConfiguration config)
    {
        services.Configure<KafkaOptions>(config.GetSection(KafkaOptions.SectionName));
        services.AddSingleton<IKafkaProducer, KafkaProducer>();
        return services;
    }
}
