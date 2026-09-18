namespace eShop.Inventory.API.Model;

public sealed class IncomingIntegrationEvent(Guid eventId, string eventType, DateTime processedAt)
{
    public Guid EventId { get; private set; } = eventId != Guid.Empty
        ? eventId
        : throw new ArgumentException("Event ID is required.", nameof(eventId));

    public string EventType { get; private set; } = !string.IsNullOrWhiteSpace(eventType)
        ? eventType.Trim()
        : throw new ArgumentException("Event type is required.", nameof(eventType));

    public DateTime ProcessedAt { get; private set; } = processedAt.Kind == DateTimeKind.Utc
        ? processedAt
        : throw new ArgumentException("Processed timestamp must use UTC.", nameof(processedAt));
}
