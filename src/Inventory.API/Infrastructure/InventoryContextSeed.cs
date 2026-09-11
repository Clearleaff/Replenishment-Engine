namespace eShop.Inventory.API.Infrastructure;

public sealed class InventoryContextSeed(
    ILogger<InventoryContextSeed> logger)
{
    private static readonly (string Code, string Name)[] Locations =
    [
        ("NCR", "National Capital Region"),
        ("BLR", "Bengaluru"),
        ("BOM", "Mumbai"),
        ("HYD", "Hyderabad")
    ];

    public async Task SeedAsync(InventoryContext context)
    {
        foreach (var (code, name) in Locations)
        {
            if (!await context.Locations.AnyAsync(location => location.Code == code))
            {
                context.Locations.Add(new InventoryLocation(code, name));
            }
        }

        await context.SaveChangesAsync();

        var existingKeys = await context.InventoryBalances
            .Select(balance => new { balance.SkuId, balance.LocationCode })
            .ToListAsync();
        var existing = existingKeys
            .Select(key => (key.SkuId, key.LocationCode))
            .ToHashSet();

        var added = 0;
        foreach (var skuId in Enumerable.Range(1, 101))
        {
            for (var locationIndex = 0; locationIndex < Locations.Length; locationIndex++)
            {
                var locationCode = Locations[locationIndex].Code;
                if (existing.Contains((skuId, locationCode)))
                {
                    continue;
                }

                var safetyStock = 10 + skuId % 5;
                var reorderPoint = safetyStock + 20;
                var onHand = 60 + skuId % 41 + locationIndex * 10;
                var maxStock = Math.Max(onHand + 50, reorderPoint + 50);

                context.InventoryBalances.Add(new InventoryBalance(
                    skuId,
                    locationCode,
                    onHand,
                    reserved: 0,
                    safetyStock,
                    reorderPoint,
                    maxStock));
                added++;
            }
        }

        await context.SaveChangesAsync();
        logger.LogInformation(
            "Inventory seed contains {LocationCount} locations and added {BalanceCount} SKU-location balances",
            Locations.Length,
            added);
    }
}
