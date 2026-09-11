namespace eShop.Inventory.API.Infrastructure.EntityConfigurations;

public sealed class IncomingIntegrationEventEntityTypeConfiguration : IEntityTypeConfiguration<IncomingIntegrationEvent>
{
    public void Configure(EntityTypeBuilder<IncomingIntegrationEvent> builder)
    {
        builder.ToTable("incoming_integration_events", "inventory");
        builder.HasKey(message => message.EventId);
        builder.Property(message => message.EventId).HasColumnName("event_id").ValueGeneratedNever();
        builder.Property(message => message.EventType).HasColumnName("event_type").HasMaxLength(200).IsRequired();
        builder.Property(message => message.ProcessedAt).HasColumnName("processed_at").IsRequired();
    }
}
