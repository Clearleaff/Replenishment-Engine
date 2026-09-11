using eShop.Inventory.API.IntegrationEvents.Events;

namespace eShop.Inventory.API.IntegrationEvents.EventHandling;

public sealed class OrderStatusChangedToAwaitingValidationIntegrationEventHandler(
    InventoryIntegrationEventProcessor processor) : IIntegrationEventHandler<OrderStatusChangedToAwaitingValidationIntegrationEvent>
{
    public Task Handle(OrderStatusChangedToAwaitingValidationIntegrationEvent integrationEvent) => processor.ProcessAsync(integrationEvent);
}

public sealed class OrderStatusChangedToPaidIntegrationEventHandler(
    InventoryIntegrationEventProcessor processor) : IIntegrationEventHandler<OrderStatusChangedToPaidIntegrationEvent>
{
    public Task Handle(OrderStatusChangedToPaidIntegrationEvent integrationEvent) => processor.ProcessAsync(integrationEvent);
}

public sealed class OrderStatusChangedToCancelledIntegrationEventHandler(
    InventoryIntegrationEventProcessor processor) : IIntegrationEventHandler<OrderStatusChangedToCancelledIntegrationEvent>
{
    public Task Handle(OrderStatusChangedToCancelledIntegrationEvent integrationEvent) => processor.ProcessAsync(integrationEvent);
}

public sealed class OrderStockConfirmedIntegrationEventHandler(
    InventoryIntegrationEventProcessor processor) : IIntegrationEventHandler<OrderStockConfirmedIntegrationEvent>
{
    public Task Handle(OrderStockConfirmedIntegrationEvent integrationEvent) =>
        processor.ObserveCatalogAsync(integrationEvent, integrationEvent.OrderId, confirmed: true);
}

public sealed class OrderStockRejectedIntegrationEventHandler(
    InventoryIntegrationEventProcessor processor) : IIntegrationEventHandler<OrderStockRejectedIntegrationEvent>
{
    public Task Handle(OrderStockRejectedIntegrationEvent integrationEvent) =>
        processor.ObserveCatalogAsync(integrationEvent, integrationEvent.OrderId, confirmed: false);
}
