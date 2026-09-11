namespace eShop.Inventory.API.Infrastructure.EntityConfigurations;

internal sealed class InventoryReservationEntityTypeConfiguration
    : IEntityTypeConfiguration<InventoryReservation>
{
    public void Configure(EntityTypeBuilder<InventoryReservation> builder)
    {
        builder.ToTable("inventory_reservations", table =>
        {
            table.HasCheckConstraint("ck_inventory_reservations_quantity", "quantity > 0");
        });

        builder.HasKey(reservation => new
        {
            reservation.OrderId,
            reservation.SkuId,
            reservation.LocationCode
        });

        builder.Property(reservation => reservation.OrderId)
            .HasColumnName("order_id")
            .ValueGeneratedNever();

        builder.Property(reservation => reservation.SkuId)
            .HasColumnName("sku_id")
            .ValueGeneratedNever();

        builder.Property(reservation => reservation.LocationCode)
            .HasColumnName("location_code")
            .HasMaxLength(16);

        builder.Property(reservation => reservation.Quantity)
            .HasColumnName("quantity");

        builder.Property(reservation => reservation.Status)
            .HasColumnName("status")
            .HasConversion<string>()
            .HasMaxLength(16);

        builder.Property(reservation => reservation.ReservedAt)
            .HasColumnName("reserved_at");

        builder.Property(reservation => reservation.CompletedAt)
            .HasColumnName("completed_at");

        builder.HasOne<InventoryBalance>()
            .WithMany()
            .HasForeignKey(reservation => new
            {
                reservation.SkuId,
                reservation.LocationCode
            })
            .OnDelete(DeleteBehavior.Restrict);

        builder.HasIndex(reservation => new { reservation.Status, reservation.ReservedAt })
            .HasDatabaseName("ix_inventory_reservations_status_reserved_at");
    }
}
