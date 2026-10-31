using System.Text;
using BrewQueue.Contracts;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BrewQueue.Infrastructure.Messaging;

/// <summary>Connection and topic settings shared by every Kafka client in the solution.</summary>
public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:19092,localhost:19093,localhost:19094";

    // ---- security (defaults match the secured docker-compose cluster)
    public SecurityProtocol SecurityProtocol { get; set; } = SecurityProtocol.SaslSsl;
    public SaslMechanism SaslMechanism { get; set; } = SaslMechanism.ScramSha512;
    public string? SaslUsername { get; set; }

    /// <summary>Never put this in appsettings.json: use environment variables, user-secrets or a vault.</summary>
    public string? SaslPassword { get; set; }

    /// <summary>PEM file of the CA that signed the brokers' certificates.</summary>
    public string? SslCaLocation { get; set; }

    // ---- consumer
    public string ConsumerGroupId { get; set; } = MessagingNames.StatsConsumerGroup;

    /// <summary>
    /// Static group membership. Set a stable, unique id per instance (for example the Kubernetes
    /// StatefulSet pod name) so a restart within the session timeout doesn't trigger a rebalance.
    /// Leave empty for dynamic membership.
    /// </summary>
    public string? GroupInstanceId { get; set; }

    // ---- topics
    /// <summary>
    /// When true, the apps create their topics at startup (tests and single-node dev only).
    /// In the secured cluster topics are owned by infra/kafka/setup-kafka.sh and the apps' users
    /// aren't allowed to create topics.
    /// </summary>
    public bool ManageTopics { get; set; }

    public int OrderEventsPartitions { get; set; } = 6; // one per partition of the order-events topic, so each StatsWorker instance can handle one partition
    public short ReplicationFactor { get; set; } = 3; // the secured cluster has 3 brokers, so 3 replicas is the max and gives full redundancy
    public int MinInSyncReplicas { get; set; } = 2; // a write is confirmed only once at least 2 brokers have it, so we can lose one broker without losing data
}

public sealed class SchemaRegistryOptions
{
    public const string SectionName = "SchemaRegistry";

    public string Url { get; set; } = "https://localhost:8085";
    public string? Username { get; set; }
    public string? Password { get; set; }
    public string? SslCaLocation { get; set; }

    public SchemaRegistryConfig ToConfig()
    {
        var config = new SchemaRegistryConfig { Url = Url };
        if (!string.IsNullOrEmpty(Username))
        {
            if (string.IsNullOrEmpty(Password))
                throw new InvalidOperationException("SchemaRegistry:Password is required when SchemaRegistry:Username is set.");

            config.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
            config.BasicAuthUserInfo = $"{Username}:{Password}";
        }
        if (!string.IsNullOrEmpty(SslCaLocation))
            config.SslCaLocation = FilePaths.Resolve(SslCaLocation);
        return config;
    }
}

/// <summary>
/// Applies connection and security settings to any Kafka client config, and provides topic management.
/// <para>All clients in the solution must use the same settings, so they can talk to the same brokers and schema registry.</para>
/// <para>All producers must use the same <c>min.insync.replicas</c> and <c>acks=all</c> so a write is confirmed only once at least two brokers have it, and the idempotent producer so broker-side retries can't duplicate or reorder messages within a partition.</para>
/// </summary>
public static class KafkaClientConfig
{
    /// <summary>Applies connection and security settings to any producer/consumer/admin config.</summary>
    public static T Apply<T>(T config, KafkaOptions options) where T : ClientConfig
    {
        config.BootstrapServers = options.BootstrapServers;
        config.SecurityProtocol = options.SecurityProtocol;
        config.ClientId = AppDomain.CurrentDomain.FriendlyName;

        if (options.SecurityProtocol is SecurityProtocol.SaslSsl or SecurityProtocol.SaslPlaintext)
        {
            if (string.IsNullOrEmpty(options.SaslUsername) || string.IsNullOrEmpty(options.SaslPassword))
                throw new InvalidOperationException(
                    "Kafka:SaslUsername and Kafka:SaslPassword are required when Kafka:SecurityProtocol uses SASL.");

            config.SaslMechanism = options.SaslMechanism;
            config.SaslUsername = options.SaslUsername;
            config.SaslPassword = options.SaslPassword;
        }

        if (options.SecurityProtocol is SecurityProtocol.SaslSsl or SecurityProtocol.Ssl)
        {
            config.SslCaLocation = FilePaths.Resolve(options.SslCaLocation);
            // Verify the broker's hostname against its certificate (librdkafka's default, made explicit).
            config.SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https;
        }

        return config;
    }

    /// <summary>
    /// Settings for producers whose messages must not be lost or reordered.
    /// <list type="bullet">
    /// <item><c>Acks.All</c> + the topics' <c>min.insync.replicas=2</c>: a write is confirmed only
    /// once at least two brokers have it.</item>
    /// <item>Idempotence: broker-side retries can't duplicate or reorder messages within a partition
    /// (it allows up to 5 in-flight requests per connection while keeping order).</item>
    /// </list>
    /// </summary>
    public static ProducerConfig DurableProducer(KafkaOptions options) => Apply(new ProducerConfig
    {
        Acks = Acks.All,
        EnableIdempotence = true,
        MaxInFlight = 5,
        CompressionType = CompressionType.Zstd,
        LingerMs = 10,
        // Give up on a message after 2 minutes; the outbox keeps it and tries again later.
        MessageTimeoutMs = 120_000
    }, options);
}

public static class FilePaths
{
    /// <summary>
    /// Resolves a relative path (such as "infra/kafka/secrets/public/ca.crt" in appsettings) by
    /// looking in the current directory and its parents, so it works whether you start the app
    /// from the repository root, the project folder or the IDE.
    /// </summary>
    public static string? Resolve(string? path)
    {
        if (string.IsNullOrWhiteSpace(path) || Path.IsPathRooted(path)) return path;

        for (var dir = new DirectoryInfo(Directory.GetCurrentDirectory()); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, path);
            if (File.Exists(candidate)) return candidate;
        }
        return path; // let the client report a clear "file not found"
    }
}

public static class KafkaTopics
{
    /// <summary>
    /// Creates the order-events topic and its dead-letter topic, for tests and single-node dev
    /// (<see cref="KafkaOptions.ManageTopics"/>). Retries while the broker is starting.
    /// </summary>
    public static async Task EnsureCreatedAsync(KafkaOptions options, ILogger logger, CancellationToken ct)
    {
        using var admin = new AdminClientBuilder(KafkaClientConfig.Apply(new AdminClientConfig(), options)).Build();

        var topicConfig = new Dictionary<string, string>
        {
            ["min.insync.replicas"] = options.MinInSyncReplicas.ToString()
        };
        TopicSpecification[] topics =
        [
            new() { Name = MessagingNames.OrderEventsTopic, NumPartitions = options.OrderEventsPartitions, ReplicationFactor = options.ReplicationFactor, Configs = topicConfig },
            new() { Name = MessagingNames.OrderEventsDeadLetterTopic, NumPartitions = options.OrderEventsPartitions, ReplicationFactor = options.ReplicationFactor, Configs = topicConfig }
        ];

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await admin.CreateTopicsAsync(topics);
                logger.LogInformation("Created Kafka topics {Topics}", string.Join(", ", topics.Select(t => t.Name)));
                return;
            }
            catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code is ErrorCode.NoError or ErrorCode.TopicAlreadyExists))
            {
                return;
            }
            catch (KafkaException ex) when (attempt < 10)
            {
                logger.LogWarning(ex, "Kafka not ready (attempt {Attempt}); retrying", attempt);
                await Task.Delay(TimeSpan.FromSeconds(3), ct);
            }
        }
    }

    public static string? ReadHeader(Headers? headers, string name) =>
        headers is not null && headers.TryGetLastBytes(name, out var bytes) ? Encoding.UTF8.GetString(bytes) : null;
}
