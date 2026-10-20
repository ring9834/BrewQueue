using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using BrewQueue.Infrastructure.Idempotency;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Mvc.Infrastructure;
using Microsoft.Extensions.Options;

namespace BrewQueue.Api.CrossCutting;

/// <summary>
/// Marks a controller action as idempotent: the client must send an <c>Idempotency-Key</c>
/// header, and repeating the request with the same key returns the original response
/// instead of executing the action again.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class IdempotentAttribute : Attribute, IFilterFactory
{
    public bool IsReusable => false;

    public IFilterMetadata CreateInstance(IServiceProvider services) =>
        ActivatorUtilities.CreateInstance<IdempotencyFilter>(services);
}

public sealed class IdempotencyFilter(
    IIdempotencyStore store,
    ProblemDetailsFactory problemFactory,
    IOptions<JsonOptions> jsonOptions,
    ILogger<IdempotencyFilter> logger) : IAsyncActionFilter
{
    public const string HeaderName = "Idempotency-Key";
    public const string ReplayedHeader = "Idempotent-Replayed";

    /// <summary>Name of the action parameter bound from the header; excluded from the fingerprint.</summary>
    public const string KeyParameterName = "idempotencyKey";

    public async Task OnActionExecutionAsync(ActionExecutingContext context, ActionExecutionDelegate next)
    {
        var http = context.HttpContext;
        var ct = http.RequestAborted;

        if (!Guid.TryParse(http.Request.Headers[HeaderName].ToString(), out var key))
        {
            context.Result = Problem(http, StatusCodes.Status400BadRequest, "Missing or invalid Idempotency-Key",
                $"Send a UUID in the {HeaderName} header.", "idempotency_key_invalid");
            return;
        }

        // Scope keys per endpoint so the same UUID used on two different endpoints can't collide.
        var storeKey = $"{http.Request.Method}:{http.Request.Path}:{key}";
        var fingerprint = Fingerprint(context);

        var existing = await store.TryClaimAsync(storeKey, fingerprint, ct);
        var decision = IdempotencyEvaluator.Evaluate(existing, fingerprint);

        switch (decision.Kind)
        {
            case IdempotencyDecisionKind.Replay: // The key was already used with the same request body, so we can replay the stored response.
                logger.LogInformation("Replaying stored response for idempotency key {IdempotencyKey}", key);
                var record = decision.Existing!;
                http.Response.Headers[ReplayedHeader] = "true";
                if (record.Location is not null) http.Response.Headers.Location = record.Location;
                context.Result = new ContentResult // Return the stored response body and status code.
                {
                    StatusCode = record.StatusCode,
                    Content = record.ResponseBody,
                    ContentType = "application/json; charset=utf-8"
                };
                return; // The action is not executed; the stored response is returned instead.

            case IdempotencyDecisionKind.InProgress:
                http.Response.Headers.RetryAfter = "1"; // Suggest the client retry after 1 second.
                context.Result = Problem(http, StatusCodes.Status409Conflict, "Request already in progress",
                    "A request with this Idempotency-Key is still being processed. Retry shortly.",
                    "idempotency_in_progress");
                return; // The action is not executed; the client should retry later.

            case IdempotencyDecisionKind.PayloadMismatch: // The key was already used with a different request body, so we can't proceed.
                context.Result = Problem(http, StatusCodes.Status422UnprocessableEntity, "Idempotency-Key reused",
                    "This Idempotency-Key was already used with a different request body.",
                    "idempotency_key_reused");
                return; // The action is not executed; the client should use a different key.
        }

        // Proceed: we own the key. Run the action, then store or release.
        var executed = await next(); // next() executes the action and returns an ActionExecutedContext

        var succeeded = executed.Exception is null || executed.ExceptionHandled;
        if (succeeded && executed.Result is ObjectResult { Value: not null } result
            && (result.StatusCode ?? StatusCodes.Status200OK) is >= 200 and < 300)
        {
            var body = JsonSerializer.Serialize(result.Value, result.Value.GetType(),
                jsonOptions.Value.JsonSerializerOptions);
            var location = (result as CreatedResult)?.Location;

            // Here the key already holds {Processing, fingerprint}, and the call replaces it with {Completed, fingerprint, 201, body, location}. Because an expiry is passed, it also swaps the 30-second lock TTL for the 24-hour response TTL. Had the expiry been omitted, the Completed record would never expire and Redis would slowly fill up.
            await store.CompleteAsync(storeKey,
                new IdempotencyRecord(IdempotencyState.Completed, fingerprint,
                    result.StatusCode ?? StatusCodes.Status200OK, body, location),
                CancellationToken.None);
        }
        else
        {
            // Failed requests are not cached: release the key so the client can retry.
            await store.ReleaseAsync(storeKey, CancellationToken.None);
        }
    }

    /// <summary>SHA-256 over the bound action arguments (i.e. the request payload).</summary>
    private string Fingerprint(ActionExecutingContext context)
    {
        var args = context.ActionArguments
            .Where(a => a.Key != KeyParameterName && a.Value is not CancellationToken)
            .OrderBy(a => a.Key, StringComparer.Ordinal)
            .ToDictionary(a => a.Key, a => a.Value);

        var json = JsonSerializer.Serialize(args, jsonOptions.Value.JsonSerializerOptions);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(json)));
    }

    private ObjectResult Problem(HttpContext http, int status, string title, string detail, string errorCode)
    {
        var problem = problemFactory.CreateProblemDetails(http, status, title, detail: detail);
        problem.Extensions["errorCode"] = errorCode;
        return new ObjectResult(problem) { StatusCode = status, ContentTypes = { "application/problem+json" } };
    }
}
