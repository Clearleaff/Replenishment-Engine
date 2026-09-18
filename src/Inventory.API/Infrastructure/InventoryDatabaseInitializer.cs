namespace eShop.Inventory.API.Infrastructure;

public sealed class InventoryDatabaseInitializer(
    IServiceProvider services,
    IHostEnvironment environment,
    ILogger<InventoryDatabaseInitializer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (environment.IsEnvironment("Testing"))
        {
            return;
        }

        await using var scope = services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<InventoryContext>();
        var seeder = scope.ServiceProvider.GetRequiredService<InventoryContextSeed>();

        try
        {
            await context.Database.MigrateAsync(stoppingToken);
            await seeder.SeedAsync(context);
        }
        catch (Exception exception)
        {
            logger.LogCritical(exception, "Inventory database migration or seeding failed");
            throw;
        }
    }
}
