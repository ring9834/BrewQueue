using BrewQueue.Infrastructure.Idempotency;

namespace BrewQueue.UnitTests.Infrastructure;

public class IdempotencyEvaluatorTests
{
    private const string Fingerprint = "ABC123";

    [Fact]
    public void New_key_proceeds()
    {
        var decision = IdempotencyEvaluator.Evaluate(null, Fingerprint);
        Assert.Equal(IdempotencyDecisionKind.Proceed, decision.Kind);
    }

    [Fact]
    public void Completed_key_with_same_payload_is_replayed()
    {
        var existing = new IdempotencyRecord(IdempotencyState.Completed, Fingerprint, 201, "{\"id\":1}", "/api/orders/1");

        var decision = IdempotencyEvaluator.Evaluate(existing, Fingerprint);

        Assert.Equal(IdempotencyDecisionKind.Replay, decision.Kind);
        Assert.Same(existing, decision.Existing);
    }

    [Fact]
    public void Key_still_processing_is_in_progress()
    {
        var existing = new IdempotencyRecord(IdempotencyState.Processing, Fingerprint);
        Assert.Equal(IdempotencyDecisionKind.InProgress, IdempotencyEvaluator.Evaluate(existing, Fingerprint).Kind);
    }

    [Theory]
    [InlineData(IdempotencyState.Processing)]
    [InlineData(IdempotencyState.Completed)]
    public void Different_payload_is_a_mismatch_regardless_of_state(IdempotencyState state)
    {
        var existing = new IdempotencyRecord(state, "SOMETHING-ELSE");
        Assert.Equal(IdempotencyDecisionKind.PayloadMismatch, IdempotencyEvaluator.Evaluate(existing, Fingerprint).Kind);
    }
}
