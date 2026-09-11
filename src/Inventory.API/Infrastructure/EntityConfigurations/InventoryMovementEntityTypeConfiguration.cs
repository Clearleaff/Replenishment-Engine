namespace eShop.Inventory.API.Infrastructure.EntityConfigurations;

internal sealed class InventoryMovementEntityTypeConfiguration
    : IEntityTypeConfiguration<InventoryMovement>
{
    public void Configure(EntityTypeBuilder<InventoryMovement> builder)
    {
        builder.ToTable("inventory_movements", table =>
        {
            table.HasCheckConstraint("ck_inventory_movements_balance_version", "balance_version_after > 0");
            table.HasCheckConstraint("ck_inventory_movements_quantity", "quantity <> 0");
        });

        builder.HasKey(movement => movement.MovementId);

        builder.Property(movement => movement.MovementId)
            .HasColumnName("movement_id")
            .ValueGeneratedNever();

        builder.Property(movement => movement.SourceEventId)
            .HasColumnName("source_event_id");

        builder.Property(movement => movement.SkuId)
            .HasColumnName("sku_id")
            .ValueGeneratedNever();

        builder.Property(movement => movement.LocationCode)
            .HasColumnName("location_code")
            .HasMaxLength(16);

        builder.Property(movement => movement.OrderId)
            .HasColumnName("order_id");

        builder.Property(movement => movement.MovementType)
            .HasColumnName("movement_type")
            .HasConversion<string>()
            .HasMaxLength(24);

        builder.Property(movement => movement.Quantity)
            .HasColumnName("quantity");

        builder.Property(movement => movement.OccurredAt)
            .HasColumnName("occurred_at");

        builder.Property(movement => movement.RecordedAt)
            .HasColumnName("recorded_at");

        builder.Property(movement => movement.BalanceVersionAfter)
            .HasColumnName("balance_version_after");

        builder.Property(movement => movement.Reason)
            .HasColumnName("reason")
            .HasMaxLength(200);

        builder.HasOne<InventoryBalance>()
            .WithMany()
            .HasForeignKey(movement => new { movement.SkuId, movement.LocationCode })
            .OnDelete(DeleteBehavior.Restrict);

        builder.HasIndex(movement => new
        {
            movement.SourceEventId,
            movement.SkuId,
            movement.LocationCode,
            movement.MovementType
        })
        .IsUnique()
        .HasDatabaseName("ux_inventory_movements_source_sku_location_type");

        builder.HasIndex(movement => new
        {
            movement.SkuId,
            movement.LocationCode,
            movement.OccurredAt
        })
        .HasDatabaseName("ix_inventory_movements_sku_location_occurred_at");

        builder.HasIndex(movement => movement.OrderId)
            .HasDatabaseName("ix_inventory_movements_order_id");

        builder.HasIndex(movement => movement.RecordedAt)
            .HasDatabaseName("ix_inventory_movements_recorded_at");
    }
}
