using System.Text.Json;
using StackExchange.Redis;

namespace BrewQueue.Infrastructure.Idempotency;

public enum IdempotencyState
{
    Processing,
    Completed
}

/// <summary>What we store in Redis per idempotency key.</summary>
public sealed record IdempotencyRecord(
    IdempotencyState State,
    string Fingerprint,
    int? StatusCode = null,
    string? ResponseBody = null,
    string? Location = null);

public enum IdempotencyDecisionKind
{
    /// <summary>First time we see this key: run the request.</summary>
    Proceed,

    /// <summary>Same key + same payload already succeeded: return the stored response.</summary>
    Replay,

    /// <summary>Same key is still being processed by another request: 409.</summary>
    InProgress,

    /// <summary>Same key was used with a different payload: 422.</summary>
    PayloadMismatch
}

public sealed record IdempotencyDecision(IdempotencyDecisionKind Kind, IdempotencyRecord? Existing = null);

/// <summary>
/// Pure decision logic, kept separate from Redis so it can be unit tested without infrastructure.
/// Semantics follow the IETF "Idempotency-Key HTTP Header Field" draft.
/// </summary>
public static class IdempotencyEvaluator
{
    public static IdempotencyDecision Evaluate(IdempotencyRecord? existing, string fingerprint)
    {
        if (existing is null)
            return new(IdempotencyDecisionKind.Proceed);

        if (!string.Equals(existing.Fingerprint, fingerprint, StringComparison.Ordinal))
            return new(IdempotencyDecisionKind.PayloadMismatch, existing);

        return existing.State == IdempotencyState.Completed
            ? new(IdempotencyDecisionKind.Replay, existing)
            : new(IdempotencyDecisionKind.InProgress, existing);
    }
}

public interface IIdempotencyStore
{
    /// <summary>
    /// Atomically claims the key (Redis SET NX). Returns null if we claimed it,
    /// otherwise the record that already exists.
    /// </summary>
    Task<IdempotencyRecord?> TryClaimAsync(string key, string fingerprint, CancellationToken ct);

    Task CompleteAsync(string key, IdempotencyRecord record, CancellationToken ct);

    /// <summary>Releases a claim after a failed request so the client can retry with the same key.</summary>
    Task ReleaseAsync(string key, CancellationToken ct);
}

public sealed class IdempotencyOptions
{
    public const string SectionName = "Idempotency";

    /// <summary>How long a "processing" lock lives if the server crashes mid-request.</summary>
    public TimeSpan LockTtl { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long completed responses are kept for replay.</summary>
    public TimeSpan ResponseTtl { get; set; } = TimeSpan.FromHours(24);
}

public sealed class RedisIdempotencyStore(IConnectionMultiplexer redis, Microsoft.Extensions.Options.IOptions<IdempotencyOptions> options)
    : IIdempotencyStore
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly IdempotencyOptions _options = options.Value;

    private static RedisKey Key(string key) => $"idempotency:{key}";

    public async Task<IdempotencyRecord?> TryClaimAsync(string key, string fingerprint, CancellationToken ct)
    {
        var db = redis.GetDatabase();
        var processing = new IdempotencyRecord(IdempotencyState.Processing, fingerprint);

        // SET key value NX EX ttl — atomic "claim if absent".
        var claimed = await db.StringSetAsync(
            Key(key), JsonSerializer.Serialize(processing, Json), _options.LockTtl, When.NotExists);

        if (claimed) return null;

        var existing = await db.StringGetAsync(Key(key));
        // The key may have expired between the two calls; treat it as in progress to be safe.
        return existing.HasValue
            ? JsonSerializer.Deserialize<IdempotencyRecord>(existing.ToString(), Json)
            : processing;
    }

    public Task CompleteAsync(string key, IdempotencyRecord record, CancellationToken ct) =>
        redis.GetDatabase().StringSetAsync(Key(key), JsonSerializer.Serialize(record, Json), _options.ResponseTtl);

    public Task ReleaseAsync(string key, CancellationToken ct) =>
        redis.GetDatabase().KeyDeleteAsync(Key(key));
}
