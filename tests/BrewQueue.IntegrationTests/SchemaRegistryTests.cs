using System.Net.Http.Json;
using System.Text.Json;
using BrewQueue.IntegrationTests.Infrastructure;

namespace BrewQueue.IntegrationTests;

/// <summary>
/// The guard rail behind "one careless change can't break every consumer": Schema Registry, set
/// to BACKWARD_TRANSITIVE, rejects incompatible versions of order_event.proto. This is the same
/// check register_schemas.py --check-only runs in CI. Nothing is registered by these tests.
/// </summary>
[Collection(ApiCollection.Name)]
public class SchemaRegistryTests(ApiFactory factory)
{
    [Fact]
    public async Task Changing_a_field_type_is_rejected()
    {
        // total_cents: int64 -> string. Old data can no longer be read with the new schema.
        var changed = ApiFactory.ReadOrderEventProto().Replace("int64 total_cents = 6;", "string total_cents = 6;");

        Assert.False(await IsCompatibleAsync(changed));
    }

    [Fact]
    public async Task Reusing_a_field_number_for_a_different_type_is_rejected()
    {
        // Someone deletes total_cents and reuses its number for a new string field.
        var changed = ApiFactory.ReadOrderEventProto().Replace("int64 total_cents = 6;", "string currency = 6;");

        Assert.False(await IsCompatibleAsync(changed));
    }

    [Fact]
    public async Task Adding_a_new_field_is_accepted()
    {
        var changed = ApiFactory.ReadOrderEventProto().Replace(
            "int64 occurred_at_unix_ms = 8;",
            "int64 occurred_at_unix_ms = 8;\n  string store_id = 9;");

        Assert.True(await IsCompatibleAsync(changed));
    }

    [Fact]
    public async Task Renaming_a_field_is_accepted_because_the_wire_format_is_unchanged()
    {
        // Protobuf puts field numbers on the wire, not names, so existing consumers keep working.
        var changed = ApiFactory.ReadOrderEventProto().Replace("int64 total_cents = 6;", "int64 amount_cents = 6;");

        Assert.True(await IsCompatibleAsync(changed));
    }

    private async Task<bool> IsCompatibleAsync(string schema)
    {
        Assert.NotEqual(ApiFactory.ReadOrderEventProto(), schema); // the Replace must have matched

        using var http = factory.CreateSchemaRegistryClient();
        var response = await http.PostAsJsonAsync(
            $"/compatibility/subjects/{ApiFactory.OrderEventsSubject}/versions/latest?verbose=true",
            new { schemaType = "PROTOBUF", schema });
        response.EnsureSuccessStatusCode();

        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        return body.GetProperty("is_compatible").GetBoolean();
    }
}
