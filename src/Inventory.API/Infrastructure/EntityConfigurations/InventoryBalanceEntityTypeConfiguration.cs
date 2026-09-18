namespace eShop.Inventory.API.Infrastructure.EntityConfigurations;

internal sealed class InventoryBalanceEntityTypeConfiguration
    : IEntityTypeConfiguration<InventoryBalance>
{
    public void Configure(EntityTypeBuilder<InventoryBalance> builder)
    {
        builder.ToTable("inventory_balances", table =>
        {
            table.HasCheckConstraint("ck_inventory_balances_on_hand", "on_hand >= 0");
            table.HasCheckConstraint("ck_inventory_balances_reserved", "reserved >= 0");
            table.HasCheckConstraint("ck_inventory_balances_reserved_on_hand", "reserved <= on_hand");
            table.HasCheckConstraint("ck_inventory_balances_safety_stock", "safety_stock >= 0");
            table.HasCheckConstraint("ck_inventory_balances_reorder_point", "reorder_point >= safety_stock");
            table.HasCheckConstraint("ck_inventory_balances_max_stock", "max_stock >= reorder_point");
            table.HasCheckConstraint("ck_inventory_balances_capacity", "on_hand <= max_stock");
        });

        builder.HasKey(balance => new { balance.SkuId, balance.LocationCode });

        builder.Property(balance => balance.SkuId)
            .HasColumnName("sku_id")
            .ValueGeneratedNever();

        builder.Property(balance => balance.LocationCode)
            .HasColumnName("location_code")
            .HasMaxLength(16);

        builder.Property(balance => balance.OnHand)
            .HasColumnName("on_hand");

        builder.Property(balance => balance.Reserved)
            .HasColumnName("reserved");

        builder.Ignore(balance => balance.Available);

        builder.Property(balance => balance.SafetyStock)
            .HasColumnName("safety_stock");

        builder.Property(balance => balance.ReorderPoint)
            .HasColumnName("reorder_point");

        builder.Property(balance => balance.MaxStock)
            .HasColumnName("max_stock");

        builder.Property(balance => balance.Version)
            .HasColumnName("version")
            .IsConcurrencyToken();

        builder.Property(balance => balance.UpdatedAt)
            .HasColumnName("updated_at");

        builder.HasOne<InventoryLocation>()
            .WithMany()
            .HasForeignKey(balance => balance.LocationCode)
            .OnDelete(DeleteBehavior.Restrict);

        builder.HasIndex(balance => new { balance.LocationCode, balance.SkuId })
            .HasDatabaseName("ix_inventory_balances_location_sku");
    }
}
