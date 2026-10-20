using BrewQueue.Domain;

namespace BrewQueue.Infrastructure.Persistence;

public static class SeedData
{
    public static readonly MenuItem[] MenuItems =
    [
        new() { Id = 1, Name = "Flat White", Category = "Coffee", Price = 5.50m, Description = "Double ristretto with velvety microfoam." },
        new() { Id = 2, Name = "Long Black", Category = "Coffee", Price = 4.80m, Description = "Double shot over hot water." },
        new() { Id = 3, Name = "Cappuccino", Category = "Coffee", Price = 5.50m, Description = "Espresso, steamed milk, thick foam, chocolate dusting." },
        new() { Id = 4, Name = "Mocha", Category = "Coffee", Price = 6.20m, Description = "Espresso, chocolate and steamed milk." },
        new() { Id = 5, Name = "Chai Latte", Category = "Tea", Price = 5.80m, Description = "Spiced chai with steamed milk." },
        new() { Id = 6, Name = "Matcha Latte", Category = "Tea", Price = 6.50m, Description = "Ceremonial-grade matcha with oat milk." },
        new() { Id = 7, Name = "Cheese Scone", Category = "Food", Price = 4.50m, Description = "Warm, with butter." },
        new() { Id = 8, Name = "Banana Bread", Category = "Food", Price = 5.00m, Description = "Toasted, with whipped butter." },
        new() { Id = 9, Name = "Seasonal Cold Brew", Category = "Coffee", Price = 6.00m, Description = "Back soon.", IsAvailable = false }
    ];
}
