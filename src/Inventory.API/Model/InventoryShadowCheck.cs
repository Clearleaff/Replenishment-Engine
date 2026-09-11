namespace eShop.Inventory.API.Model;

public sealed class InventoryShadowCheck(
    Guid sourceEventId,
    int orderId,
    string locationCode,
    bool inventoryConfirmed,
    string details,
    DateTime evaluatedAt)
{
    public Guid SourceEventId { get; private set; } = sourceEventId;
    public int OrderId { get; private set; } = orderId;
    public string LocationCode { get; private set; } = locationCode.Trim().ToUpperInvariant();
    public bool InventoryConfirmed { get; private set; } = inventoryConfirmed;
    public bool? CatalogConfirmed { get; private set; }
    public bool? Agreement => CatalogConfirmed is null ? null : CatalogConfirmed == InventoryConfirmed;
    public string Details { get; private set; } = details;
    public DateTime EvaluatedAt { get; private set; } = evaluatedAt;
    public DateTime? CatalogObservedAt { get; private set; }

    public void ObserveCatalogResult(bool confirmed, DateTime observedAt)
    {
        CatalogConfirmed = confirmed;
        CatalogObservedAt = observedAt;
    }
}
