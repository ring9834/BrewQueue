using System.Net;
using System.Net.Http.Json;
using BrewQueue.Api.Contracts;
using BrewQueue.Domain;
using BrewQueue.Infrastructure.Persistence;
using BrewQueue.IntegrationTests.Infrastructure;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using static BrewQueue.IntegrationTests.Infrastructure.TestHelpers;

namespace BrewQueue.IntegrationTests;

[Collection(ApiCollection.Name)]
public class OrdersApiTests(ApiFactory factory)
{
    private readonly HttpClient _client = factory.CreateClient();

    [Fact]
    public async Task Menu_returns_seeded_items()
    {
        var menu = await _client.GetFromJsonAsync<List<MenuItemDto>>("/api/menu", ApiFactory.Json);

        Assert.NotNull(menu);
        Assert.Contains(menu, m => m.Name == "Flat White" && m.IsAvailable);
        Assert.Contains(menu, m => !m.IsAvailable);
    }

    [Fact]
    public async Task Placing_an_order_returns_201_with_location()
    {
        var response = await _client.SendAsync(PlaceOrderRequest(OrderBody(quantity: 2), Guid.NewGuid()));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var order = await response.Content.ReadFromJsonAsync<OrderDto>(ApiFactory.Json);
        Assert.NotNull(order);
        Assert.Equal(OrderStatus.Received, order.Status);
        Assert.Equal(11.00m, order.Total); // 2 × Flat White @ 5.50
        Assert.Equal($"/api/orders/{order.Id}", response.Headers.Location?.OriginalString);

        var fetched = await _client.GetFromJsonAsync<OrderDto>($"/api/orders/{order.Id}", ApiFactory.Json);
        Assert.Equal(order.Id, fetched!.Id);
    }

    [Fact]
    public async Task Same_idempotency_key_returns_the_original_response_and_creates_one_order()
    {
        var key = Guid.NewGuid();
        var customer = $"Idem {key:N}";

        var first = await _client.SendAsync(PlaceOrderRequest(OrderBody(customer), key));
        var second = await _client.SendAsync(PlaceOrderRequest(OrderBody(customer), key));

        Assert.Equal(HttpStatusCode.Created, first.StatusCode);
        Assert.Equal(HttpStatusCode.Created, second.StatusCode);
        Assert.False(first.Headers.Contains("Idempotent-Replayed"));
        Assert.Equal("true", second.Headers.GetValues("Idempotent-Replayed").Single());

        var a = await first.Content.ReadFromJsonAsync<OrderDto>(ApiFactory.Json);
        var b = await second.Content.ReadFromJsonAsync<OrderDto>(ApiFactory.Json);
        Assert.Equal(a!.Id, b!.Id);

        Assert.Equal(1, await CountOrdersAsync(customer));
    }

    [Fact]
    public async Task Concurrent_requests_with_the_same_key_create_exactly_one_order()
    {
        var key = Guid.NewGuid();
        var customer = $"Race {key:N}";

        var responses = await Task.WhenAll(Enumerable.Range(0, 5)
            .Select(_ => _client.SendAsync(PlaceOrderRequest(OrderBody(customer), key))));

        // Each response is either the original 201, a replay (201), or "still in progress" (409).
        Assert.All(responses, r => Assert.Contains(r.StatusCode, new[] { HttpStatusCode.Created, HttpStatusCode.Conflict }));
        Assert.Equal(1, await CountOrdersAsync(customer));
    }

    [Fact]
    public async Task Reusing_a_key_with_a_different_body_returns_422()
    {
        var key = Guid.NewGuid();
        await _client.SendAsync(PlaceOrderRequest(OrderBody("First"), key));

        var response = await _client.SendAsync(PlaceOrderRequest(OrderBody("Second"), key));

        var problem = await AssertProblemAsync(response, HttpStatusCode.UnprocessableEntity);
        Assert.Equal("idempotency_key_reused", problem.Extensions["errorCode"]?.ToString());
    }

    [Fact]
    public async Task Missing_idempotency_key_returns_validation_problem()
    {
        var response = await _client.SendAsync(PlaceOrderRequest(OrderBody()));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
        var problem = await response.Content.ReadFromJsonAsync<ValidationProblemDetails>(ApiFactory.Json);
        Assert.NotEmpty(problem!.Errors);
    }

    [Fact]
    public async Task Invalid_body_returns_validation_problem_with_field_errors()
    {
        var body = new { customerName = "", lines = Array.Empty<object>() };

        var response = await _client.SendAsync(PlaceOrderRequest(body, Guid.NewGuid()));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var problem = await response.Content.ReadFromJsonAsync<ValidationProblemDetails>(ApiFactory.Json);
        Assert.Contains(problem!.Errors.Keys, k => k.Contains("CustomerName", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Ordering_an_unavailable_item_returns_422_with_error_code()
    {
        var unavailableId = SeedData.MenuItems.First(m => !m.IsAvailable).Id;

        var response = await _client.SendAsync(PlaceOrderRequest(OrderBody(menuItemId: unavailableId), Guid.NewGuid()));

        var problem = await AssertProblemAsync(response, HttpStatusCode.UnprocessableEntity);
        Assert.Equal("menu_item_unavailable", problem.Extensions["errorCode"]?.ToString());
    }

    [Fact]
    public async Task Unknown_order_returns_404_problem_with_trace_and_correlation_ids()
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, $"/api/orders/{Guid.NewGuid()}");
        request.Headers.Add("X-Correlation-Id", "test-correlation-123");

        var response = await _client.SendAsync(request);

        var problem = await AssertProblemAsync(response, HttpStatusCode.NotFound);
        Assert.True(problem.Extensions.ContainsKey("traceId"));
        Assert.Equal("test-correlation-123", problem.Extensions["correlationId"]?.ToString());
        Assert.Equal("test-correlation-123", response.Headers.GetValues("X-Correlation-Id").Single());
    }

    [Fact]
    public async Task Invalid_status_transition_returns_422()
    {
        var order = await PlaceAsync();

        var response = await _client.PatchAsJsonAsync($"/api/orders/{order.Id}/status",
            new { status = "Collected" }, ApiFactory.Json);

        var problem = await AssertProblemAsync(response, HttpStatusCode.UnprocessableEntity);
        Assert.Equal("invalid_status_transition", problem.Extensions["errorCode"]?.ToString());
    }

    [Fact]
    public async Task Status_flow_and_board_filtering()
    {
        var order = await PlaceAsync();

        foreach (var status in new[] { "Preparing", "Ready" })
        {
            var r = await _client.PatchAsJsonAsync($"/api/orders/{order.Id}/status", new { status }, ApiFactory.Json);
            r.EnsureSuccessStatusCode();
        }

        var ready = await _client.GetFromJsonAsync<PagedResult<OrderDto>>(
            "/api/orders?status=Ready&pageSize=100", ApiFactory.Json);
        Assert.Contains(ready!.Items, o => o.Id == order.Id);
        Assert.All(ready.Items, o => Assert.Equal(OrderStatus.Ready, o.Status));
    }

    private async Task<OrderDto> PlaceAsync()
    {
        var response = await _client.SendAsync(PlaceOrderRequest(OrderBody(), Guid.NewGuid()));
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<OrderDto>(ApiFactory.Json))!;
    }

    private async Task<int> CountOrdersAsync(string customer)
    {
        using var scope = factory.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<BrewQueueDbContext>();
        return await db.Orders.CountAsync(o => o.CustomerName == customer);
    }

    private static async Task<ProblemDetails> AssertProblemAsync(HttpResponseMessage response, HttpStatusCode expected)
    {
        Assert.Equal(expected, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
        var problem = await response.Content.ReadFromJsonAsync<ProblemDetails>(ApiFactory.Json);
        Assert.NotNull(problem);
        Assert.Equal((int)expected, problem.Status);
        return problem;
    }
}
