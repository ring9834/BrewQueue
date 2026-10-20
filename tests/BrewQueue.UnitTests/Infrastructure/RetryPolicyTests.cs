using System.Text;
using BrewQueue.Infrastructure.Messaging;
using BrewQueue.Infrastructure.Stats;

namespace BrewQueue.UnitTests.Infrastructure;

public class RetryPolicyTests
{
    [Theory]
    [InlineData(0, true, true)]
    [InlineData(2, true, true)]
    [InlineData(3, true, false)]  // max retries reached
    [InlineData(0, false, false)] // permanent failures never retry
    public void ShouldRetry(int retryCount, bool transient, bool expected) =>
        Assert.Equal(expected, RetryPolicy.ShouldRetry(retryCount, transient));

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 2)]
    [InlineData(2, 4)]
    public void Backoff_is_exponential(int retryCount, int expectedSeconds) =>
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), RetryPolicy.Backoff(retryCount));

    [Fact]
    public void ReadIntHeader_handles_amqp_value_types()
    {
        var headers = new Dictionary<string, object?>
        {
            ["int"] = 2,
            ["long"] = 3L,
            ["bytes"] = Encoding.UTF8.GetBytes("4"),
            ["junk"] = Encoding.UTF8.GetBytes("nope")
        };

        Assert.Equal(2, RetryPolicy.ReadIntHeader(headers, "int"));
        Assert.Equal(3, RetryPolicy.ReadIntHeader(headers, "long"));
        Assert.Equal(4, RetryPolicy.ReadIntHeader(headers, "bytes"));
        Assert.Equal(0, RetryPolicy.ReadIntHeader(headers, "junk"));
        Assert.Equal(0, RetryPolicy.ReadIntHeader(headers, "missing"));
        Assert.Equal(0, RetryPolicy.ReadIntHeader(null, "int"));
    }

    [Fact]
    public void ReadStringHeader_decodes_bytes()
    {
        var headers = new Dictionary<string, object?> { ["cid"] = Encoding.UTF8.GetBytes("abc") };
        Assert.Equal("abc", RetryPolicy.ReadStringHeader(headers, "cid"));
        Assert.Null(RetryPolicy.ReadStringHeader(headers, "missing"));
    }

    [Fact]
    public void Stats_hours_are_truncated_in_utc()
    {
        var local = new DateTimeOffset(2026, 10, 17, 14, 45, 12, TimeSpan.FromHours(13));
        Assert.Equal(new DateTimeOffset(2026, 10, 17, 1, 0, 0, TimeSpan.Zero), OrderStatsProjector.TruncateToHour(local));
    }
}
