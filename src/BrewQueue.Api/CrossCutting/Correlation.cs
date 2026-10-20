using Serilog.Context;

namespace BrewQueue.Api.CrossCutting;

public interface ICorrelationIdAccessor
{
    string? CorrelationId { get; }
}

public sealed class CorrelationIdAccessor(IHttpContextAccessor http) : ICorrelationIdAccessor
{
    public string? CorrelationId =>
        http.HttpContext?.Items[CorrelationIdMiddleware.ItemKey] as string;
}

/// <summary>
/// Reads X-Correlation-Id from the request (or creates one), echoes it in the response,
/// and pushes it into the Serilog LogContext so every log line for this request carries it.
/// The id is also stored on outbox messages, sent as a broker header, and logged by the
/// workers — so you can follow one order across all services in Seq.
/// </summary>
public sealed class CorrelationIdMiddleware(RequestDelegate next)
{
    public const string HeaderName = "X-Correlation-Id";
    public const string ItemKey = "CorrelationId";

    public async Task InvokeAsync(HttpContext context)
    {
        var correlationId = context.Request.Headers[HeaderName].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(correlationId) || correlationId.Length > 100)
            correlationId = Guid.NewGuid().ToString("N");

        context.Items[ItemKey] = correlationId;
        context.Response.OnStarting(() =>
        {
            context.Response.Headers[HeaderName] = correlationId;
            return Task.CompletedTask;
        });

        using (LogContext.PushProperty("CorrelationId", correlationId))
        {
            await next(context);
        }
    }
}
