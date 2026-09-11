namespace eShop.Inventory.API.Infrastructure.EntityConfigurations;

public sealed class InventoryShadowCheckEntityTypeConfiguration : IEntityTypeConfiguration<InventoryShadowCheck>
{
    public void Configure(EntityTypeBuilder<InventoryShadowCheck> builder)
    {
        builder.ToTable("inventory_shadow_checks", "inventory");
        builder.HasKey(check => check.SourceEventId);
        builder.Property(check => check.SourceEventId).HasColumnName("source_event_id").ValueGeneratedNever();
        builder.Property(check => check.OrderId).HasColumnName("order_id");
        builder.Property(check => check.LocationCode).HasColumnName("location_code").HasMaxLength(16).IsRequired();
        builder.Property(check => check.InventoryConfirmed).HasColumnName("inventory_confirmed");
        builder.Property(check => check.CatalogConfirmed).HasColumnName("catalog_confirmed");
        builder.Ignore(check => check.Agreement);
        builder.Property(check => check.Details).HasColumnName("details").HasMaxLength(1000).IsRequired();
        builder.Property(check => check.EvaluatedAt).HasColumnName("evaluated_at");
        builder.Property(check => check.CatalogObservedAt).HasColumnName("catalog_observed_at");
        builder.HasIndex(check => check.OrderId);
    }
}
