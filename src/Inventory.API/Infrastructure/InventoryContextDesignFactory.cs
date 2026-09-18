using Microsoft.EntityFrameworkCore.Design;

namespace eShop.Inventory.API.Infrastructure;

public sealed class InventoryContextDesignFactory : IDesignTimeDbContextFactory<InventoryContext>
{
    public InventoryContext CreateDbContext(string[] args)
    {
        var connectionString = Environment.GetEnvironmentVariable("ConnectionStrings__inventorydb")
            ?? "Host=localhost;Database=inventorydb;Username=postgres;Password=postgres";
        var options = new DbContextOptionsBuilder<InventoryContext>()
            .UseNpgsql(connectionString)
            .Options;
        return new InventoryContext(options);
    }
}
