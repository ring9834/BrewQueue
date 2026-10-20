using BrewQueue.Api.CrossCutting;
using BrewQueue.Domain;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace BrewQueue.UnitTests.Api;

public class GlobalExceptionHandlerTests
{
    private readonly IProblemDetailsService _problemDetails = Substitute.For<IProblemDetailsService>();
    private ProblemDetailsContext? _written;

    public GlobalExceptionHandlerTests()
    {
        _problemDetails.TryWriteAsync(Arg.Do<ProblemDetailsContext>(c => _written = c)).Returns(true);
    }

    private GlobalExceptionHandler CreateHandler(string environment)
    {
        var env = Substitute.For<IHostEnvironment>();
        env.EnvironmentName.Returns(environment);
        return new GlobalExceptionHandler(_problemDetails, env, NullLogger<GlobalExceptionHandler>.Instance);
    }

    public static TheoryData<Exception, int, string> Mappings => new()
    {
        { new NotFoundException("Order", Guid.Empty), 404, "not_found" },
        { new DomainException("order_empty", "Empty"), 422, "order_empty" },
        { new InvalidStatusTransitionException(OrderStatus.Ready, OrderStatus.Received), 422, "invalid_status_transition" },
        { new ConflictException("Changed"), 409, "conflict" },
        { new InvalidOperationException("Boom"), 500, "internal_error" }
    };

    [Theory]
    [MemberData(nameof(Mappings))]
    public async Task Maps_exceptions_to_status_and_error_code(Exception exception, int expectedStatus, string expectedCode)
    {
        var context = new DefaultHttpContext();

        var handled = await CreateHandler(Environments.Development).TryHandleAsync(context, exception, CancellationToken.None);

        Assert.True(handled);
        Assert.Equal(expectedStatus, context.Response.StatusCode);
        Assert.NotNull(_written);
        Assert.Equal(expectedStatus, _written!.ProblemDetails.Status);
        Assert.Equal(expectedCode, _written.ProblemDetails.Extensions["errorCode"]);
    }

    [Fact]
    public async Task Hides_internal_error_details_outside_development()
    {
        var context = new DefaultHttpContext();

        await CreateHandler(Environments.Production)
            .TryHandleAsync(context, new InvalidOperationException("connection string leaked!"), CancellationToken.None);

        Assert.DoesNotContain("leaked", _written!.ProblemDetails.Detail);
        Assert.False(_written.ProblemDetails.Extensions.ContainsKey("exception"));
    }

    [Fact]
    public async Task Shows_business_error_messages_in_production()
    {
        var context = new DefaultHttpContext();

        await CreateHandler(Environments.Production)
            .TryHandleAsync(context, new DomainException("order_empty", "An order must contain at least one item."), CancellationToken.None);

        Assert.Equal("An order must contain at least one item.", _written!.ProblemDetails.Detail);
    }
}
