using System.Diagnostics;
using BrewQueue.Domain;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace BrewQueue.Api.CrossCutting;

/// <summary>
/// Turns any unhandled exception into an RFC 9457 ProblemDetails response.
/// Expected errors (not found, rule violations, conflicts) are logged as warnings without a
/// stack trace; anything unexpected is logged as an error and its details are hidden from
/// clients outside Development.
/// </summary>
public sealed class GlobalExceptionHandler(
    IProblemDetailsService problemDetails,
    IHostEnvironment env,
    ILogger<GlobalExceptionHandler> logger) : IExceptionHandler
{
    public async ValueTask<bool> TryHandleAsync(HttpContext context, Exception exception, CancellationToken ct)
    {
        var (status, title, errorCode) = exception switch
        {
            NotFoundException => (StatusCodes.Status404NotFound, "Resource not found", "not_found"),
            DomainException d => (StatusCodes.Status422UnprocessableEntity, "Business rule violated", d.ErrorCode),
            ConflictException => (StatusCodes.Status409Conflict, "Conflict", "conflict"),
            DbUpdateConcurrencyException => (StatusCodes.Status409Conflict, "Conflict",
                "concurrency_conflict"),
            BadHttpRequestException bad => (bad.StatusCode, "Bad request", "bad_request"),
            OperationCanceledException when context.RequestAborted.IsCancellationRequested =>
                (499, "Client closed request", "request_cancelled"),
            _ => (StatusCodes.Status500InternalServerError, "An unexpected error occurred", "internal_error")
        };

        if (status >= 500)
            logger.LogError(exception, "Unhandled exception for {Method} {Path}", context.Request.Method, context.Request.Path);
        else
            logger.LogWarning("Request failed with {StatusCode} ({ErrorCode}): {Message}", status, errorCode, exception.Message);

        var detail = status >= 500 && !env.IsDevelopment()
            ? "Something went wrong on our side. Quote the traceId when reporting this."
            : exception is DbUpdateConcurrencyException
                ? "The resource was modified by someone else. Reload and try again."
                : exception.Message;

        var problem = new ProblemDetails { Status = status, Title = title, Detail = detail };
        problem.Extensions["errorCode"] = errorCode;
        if (env.IsDevelopment() && status >= 500)
            problem.Extensions["exception"] = exception.GetType().FullName;

        context.Response.StatusCode = status;
        return await problemDetails.TryWriteAsync(new ProblemDetailsContext
        {
            HttpContext = context,
            Exception = exception,
            ProblemDetails = problem
        });
    }
}

public static class ProblemDetailsSetup
{
    /// <summary>
    /// Adds fields to EVERY ProblemDetails the app produces — from the exception handler,
    /// [ApiController] validation errors, status-code pages and our own filters.
    /// </summary>
    public static IServiceCollection AddBrewQueueProblemDetails(this IServiceCollection services) =>
        services.AddProblemDetails(options =>
        {
            options.CustomizeProblemDetails = ctx =>
            {
                var http = ctx.HttpContext;
                ctx.ProblemDetails.Instance ??= $"{http.Request.Method} {http.Request.Path}";
                ctx.ProblemDetails.Extensions.TryAdd("traceId", Activity.Current?.Id ?? http.TraceIdentifier);
                if (http.Items[CorrelationIdMiddleware.ItemKey] is string correlationId)
                    ctx.ProblemDetails.Extensions.TryAdd("correlationId", correlationId);
            };
        });
}
