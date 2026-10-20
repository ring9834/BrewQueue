namespace BrewQueue.Domain;

public sealed class MenuItem
{
    public int Id { get; set; }
    public required string Name { get; set; }
    public required string Category { get; set; }
    public string Description { get; set; } = "";
    public decimal Price { get; set; }
    public bool IsAvailable { get; set; } = true;
}
