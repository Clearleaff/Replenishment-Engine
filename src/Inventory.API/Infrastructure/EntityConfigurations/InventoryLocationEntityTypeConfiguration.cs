namespace eShop.Inventory.API.Infrastructure.EntityConfigurations;

internal sealed class InventoryLocationEntityTypeConfiguration
    : IEntityTypeConfiguration<InventoryLocation>
{
    public void Configure(EntityTypeBuilder<InventoryLocation> builder)
    {
        builder.ToTable("locations");
        builder.HasKey(location => location.Code);

        builder.Property(location => location.Code)
            .HasColumnName("code")
            .HasMaxLength(16)
            .ValueGeneratedNever();

        builder.Property(location => location.Name)
            .HasColumnName("name")
            .HasMaxLength(100)
            .IsRequired();

        builder.Property(location => location.IsActive)
            .HasColumnName("is_active")
            .IsRequired();

        builder.Property(location => location.CreatedAt)
            .HasColumnName("created_at")
            .IsRequired();
    }
}
