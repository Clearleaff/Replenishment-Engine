namespace eShop.Inventory.API.IntegrationEvents.Events;

public sealed record OrderStockItem(int ProductId, int Units);

public sealed record ConfirmedOrderStockItem(int ProductId, bool HasStock);

public sealed record OrderStatusChangedToAwaitingValidationIntegrationEvent(
    int OrderId,
    string LocationCode,
    IEnumerable<OrderStockItem> OrderStockItems) : IntegrationEvent;

public sealed record OrderStatusChangedToPaidIntegrationEvent(int OrderId, string LocationCode) : IntegrationEvent;

public sealed record OrderStatusChangedToCancelledIntegrationEvent(int OrderId, string LocationCode) : IntegrationEvent;

public sealed record OrderStockConfirmedIntegrationEvent(int OrderId) : IntegrationEvent;

public sealed record OrderStockRejectedIntegrationEvent(
    int OrderId,
    List<ConfirmedOrderStockItem> OrderStockItems) : IntegrationEvent;
