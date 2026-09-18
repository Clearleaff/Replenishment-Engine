namespace eShop.Inventory.API.Infrastructure;

public class InventoryContext(DbContextOptions<InventoryContext> options)
    : DbContext(options)
{
    public DbSet<InventoryLocation> Locations => Set<InventoryLocation>();

    public DbSet<InventoryBalance> InventoryBalances => Set<InventoryBalance>();

    public DbSet<InventoryReservation> InventoryReservations => Set<InventoryReservation>();

    public DbSet<InventoryMovement> InventoryMovements => Set<InventoryMovement>();

    public DbSet<IncomingIntegrationEvent> IncomingIntegrationEvents => Set<IncomingIntegrationEvent>();

    public DbSet<InventoryShadowCheck> InventoryShadowChecks => Set<InventoryShadowCheck>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("inventory");

        modelBuilder.ApplyConfiguration(new InventoryLocationEntityTypeConfiguration());
        modelBuilder.ApplyConfiguration(new InventoryBalanceEntityTypeConfiguration());
        modelBuilder.ApplyConfiguration(new InventoryReservationEntityTypeConfiguration());
        modelBuilder.ApplyConfiguration(new InventoryMovementEntityTypeConfiguration());
        modelBuilder.ApplyConfiguration(new IncomingIntegrationEventEntityTypeConfiguration());
        modelBuilder.ApplyConfiguration(new InventoryShadowCheckEntityTypeConfiguration());
        modelBuilder.UseIntegrationEventLogs();
    }
}
